package health

import (
	"math"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
)

var qualityMilestones = [...]uint64{1 << 20, 4 << 20, 16 << 20, 64 << 20}

type qualitySocketReader func(syscall.RawConn) tcpQualitySnapshot

// Each point is immutable once published. Reads do not take an observer lock
// between the first reply and the four bulk milestones.
type qualityFirstPoint struct {
	at, bytes int64
	ttfb      time.Duration
	tcp       tcpQualitySnapshot
}

type qualityBulkPoint struct {
	at, bytes int64
}

type qualityTCPPoint struct {
	bytes int64
	tcp   tcpQualitySnapshot
}

// qualityKernelPoint is a kernel byte counter at a milestone: received bytes
// for a download, acknowledged bytes for an upload. Its difference measures
// how fast the network delivered, whatever the pace at which the user drained
// or filled the socket buffer.
type qualityKernelPoint struct {
	at    int64
	bytes uint64
}

// qualityPeak keeps the fastest delivery of one direction between
// consecutive milestones. Out-of-order marks keep the counter monotonic; an
// interval is measured only between published points.
type qualityPeak struct {
	last atomic.Pointer[qualityKernelPoint]
	kbps atomic.Int64
}

// note publishes a counter. start, when there is no earlier milestone, gives
// the point the first interval starts at.
func (p *qualityPeak) note(now int64, bytes uint64, start func() *qualityKernelPoint) {
	point := &qualityKernelPoint{at: now, bytes: bytes}
	for {
		previous := p.last.Load()
		if previous != nil && previous.bytes >= bytes {
			return
		}
		if !p.last.CompareAndSwap(previous, point) {
			continue
		}
		from := previous
		if from == nil && start != nil {
			from = start()
		}
		if from == nil || from.bytes >= bytes {
			return
		}
		if elapsed := now - from.at; elapsed >= qualityPeakMinInterval {
			kbps := float64(bytes-from.bytes) * 8e6 / float64(elapsed)
			if kbps > 0 && kbps < math.MaxInt64 {
				for {
					peak := p.kbps.Load()
					if int64(kbps) <= peak || p.kbps.CompareAndSwap(peak, int64(kbps)) {
						break
					}
				}
			}
		}
		return
	}
}

// Shorter intervals between two counter reads say more about the clock and
// the syscall than about the path.
const qualityPeakMinInterval = int64(20 * time.Millisecond)

type qualityInitialSample struct {
	ttfb, dial       time.Duration
	hasTTFB, hasDial bool
	tcp              tcpQualitySnapshot
}

type qualityEndSample struct {
	bytesDown int64
	hasBytes  bool
	bulkKbps  int64
	hasBulk   bool
	peakKbps  int64
	hasPeak   bool
	limit     qualitySpeedLimit
	spliced   bool
	s1, s2    tcpQualitySnapshot
	lastTCP   tcpQualitySnapshot
	// The upload: its peak, and snapshots at its first and latest milestone.
	upPeakKbps int64
	hasUpPeak  bool
	u1, u2     tcpQualitySnapshot
}

// qualitySpeedLimit is the speedLimit of the outbound, in decimal Kbit/s like
// the measured speeds. Zero is no limit.
type qualitySpeedLimit struct {
	upKbps, downKbps int64
}

func qualitySpeedLimitOf(h outbound.Handler) qualitySpeedLimit {
	limited, ok := h.(outbound.SpeedLimited)
	if !ok {
		return qualitySpeedLimit{}
	}
	up, down := limited.SpeedLimit()
	kbps := func(bytesPerSecond uint64) int64 { return int64(min(bytesPerSecond, math.MaxInt64/8) * 8 / 1000) }
	return qualitySpeedLimit{upKbps: kbps(up), downKbps: kbps(down)}
}

// connectionQuality holds measurements only. No path decision reads it.
type connectionQuality struct {
	life       qualityLifecycle
	network    uint64
	started    int64
	limit      qualitySpeedLimit // set before the tracker is published
	dial       atomic.Int64
	hasDial    atomic.Bool
	socket     atomic.Pointer[syscall.RawConn]
	first      atomic.Pointer[qualityFirstPoint]
	firstClaim atomic.Bool
	bulk       atomic.Pointer[qualityBulkPoint]
	s2         atomic.Pointer[qualityTCPPoint]
	down, up   qualityPeak
	u1, u2     atomic.Pointer[qualityTCPPoint]
	next       atomic.Uint64
	nextUp     atomic.Uint64
	spliced    atomic.Bool
	initial    atomic.Bool
}

func (o *Observer) newConnectionQuality(udp bool, started int64) *connectionQuality {
	if udp || o.s.passive.skipQuality {
		return nil
	}
	q := &connectionQuality{network: o.qualityEpoch.Load(), started: started}
	q.next.Store(qualityMilestones[0])
	q.nextUp.Store(qualityMilestones[0])
	return q
}

func (o *Observer) qualitySocket(rc syscall.RawConn) tcpQualitySnapshot {
	if rc == nil {
		return tcpQualitySnapshot{}
	}
	if o.qualityInfo != nil {
		return o.qualityInfo(rc)
	}
	return kernelTCPQuality(rc)
}

func (q *connectionQuality) dialed(conn net.Conn, now int64) {
	if q == nil {
		return
	}
	if rc := rawConnOf(conn); rc != nil {
		q.dial.Store(max(now-q.started, 0))
		q.hasDial.Store(true)
		q.socket.Store(&rc)
	}
}

func (q *connectionQuality) snapshot(o *Observer) tcpQualitySnapshot {
	if rc := q.socket.Load(); rc != nil {
		return o.qualitySocket(*rc)
	}
	return tcpQualitySnapshot{}
}

func (q *connectionQuality) firstRead(o *Observer, now, bytes, ttfb int64) bool {
	return q.firstReply(o, now, bytes, ttfb, false)
}

// firstKernelRead is used only when the existing splice sweep observes actual
// receive progress on an open socket. It does not use the final byte counter
// as a first-reply timestamp or introduce another socket poll.
func (q *connectionQuality) firstKernelRead(o *Observer, now, bytes, ttfb int64) bool {
	return q.firstReply(o, now, bytes, ttfb, true)
}

func (q *connectionQuality) firstReply(o *Observer, now, bytes, ttfb int64, kernelProgress bool) bool {
	if q == nil || !q.beginFirstSample(kernelProgress) {
		return false
	}
	defer q.publishedSample()
	// This syscall runs before answered takes the observer lock.
	p := &qualityFirstPoint{at: now, bytes: bytes, ttfb: time.Duration(max(ttfb, 0)), tcp: q.snapshot(o)}
	// Success is returned only after the immutable point is visible. The
	// witness winner may now consume the initial sample under the lock.
	return q.first.CompareAndSwap(nil, p)
}

func (q *connectionQuality) needsMark(total int64) bool {
	return q != nil && uint64(total) >= q.next.Load()
}

func (q *connectionQuality) mark(o *Observer, now, total int64) {
	if !q.needsMark(total) {
		return
	}
	// Admission and the milestone claim are one private critical section.
	// Finish cannot miss a claimed sampler before it registers in flight.
	if !q.beginMarkSample(total) {
		return
	}
	defer q.publishedSample()
	// Successful snapshots have their own ordering. A newer byte/time mark
	// may have failed to read TCP_INFO while an earlier successful one was
	// still in flight. Neither may discard the other's measurements.
	if info := q.snapshot(o); info.present != 0 {
		if info.present&qualityHasRXBytes != 0 {
			q.down.note(now, info.rxBytes, q.firstKernelPoint)
		}
		latestQualityPoint(&q.s2, &qualityTCPPoint{bytes: total, tcp: info})
	}
	point := &qualityBulkPoint{at: now, bytes: total}
	for {
		previous := q.bulk.Load()
		if previous != nil && previous.bytes >= total {
			return
		}
		if q.bulk.CompareAndSwap(previous, point) {
			return
		}
	}
}

// The first download interval starts at the first reply.
func (q *connectionQuality) firstKernelPoint() *qualityKernelPoint {
	first := q.first.Load()
	if first == nil || first.tcp.present&qualityHasRXBytes == 0 {
		return nil
	}
	return &qualityKernelPoint{at: first.at, bytes: first.tcp.rxBytes}
}

// latestQualityPoint keeps the snapshot of the most bytes.
func latestQualityPoint(latest *atomic.Pointer[qualityTCPPoint], point *qualityTCPPoint) {
	for {
		previous := latest.Load()
		if previous != nil && previous.bytes >= point.bytes || latest.CompareAndSwap(previous, point) {
			return
		}
	}
}

func (q *connectionQuality) needsUpMark(total int64) bool {
	return q != nil && uint64(total) >= q.nextUp.Load()
}

// markUp samples the socket when the user's writes cross a milestone. The
// writes only fill the socket buffer; the acknowledged bytes tell the speed.
// Upload intervals start at the first upload milestone.
func (q *connectionQuality) markUp(o *Observer, now, total int64) {
	if !q.needsUpMark(total) || !q.beginUpMarkSample(total) {
		return
	}
	defer q.publishedSample()
	info := q.snapshot(o)
	if info.present == 0 {
		return
	}
	point := &qualityTCPPoint{bytes: total, tcp: info}
	q.u1.CompareAndSwap(nil, point)
	latestQualityPoint(&q.u2, point)
	if info.present&qualityHasTXAcked != 0 {
		q.up.note(now, info.txAcked, nil)
	}
}

func (q *connectionQuality) onSpliced() {
	if q != nil {
		q.life.mu.Lock()
		defer q.life.mu.Unlock()
		if q.life.closed {
			return
		}
		q.spliced.Store(true)
		q.next.Store(math.MaxUint64)
		// Its counter may next report all spliced bytes after the socket
		// closed. That is no timestamp of the first answer.
		if q.first.Load() == nil && q.firstClaim.CompareAndSwap(false, true) {
			q.life.splicedFirstSuppressed = true
		}
	}
}

// takeInitial is called under the observer lock. An unanswered connection
// contributes its successful socket dial at the end, without a fake TTFB.
func (q *connectionQuality) takeInitial() (qualityInitialSample, bool) {
	if q == nil || q.initial.Swap(true) {
		return qualityInitialSample{}, false
	}
	s := qualityInitialSample{dial: time.Duration(q.dial.Load()), hasDial: q.hasDial.Load()}
	if first := q.first.Load(); first != nil {
		s.ttfb, s.hasTTFB, s.tcp = first.ttfb, true, first.tcp
	}
	return s, s.hasTTFB || s.hasDial || s.tcp.present != 0
}

func (q *connectionQuality) end(total int64) qualityEndSample {
	s := qualityEndSample{bytesDown: total}
	if q == nil {
		return s
	}
	s.limit = q.limit
	s.hasBytes = q.hasDial.Load() || q.first.Load() != nil || total > 0
	if first := q.first.Load(); first != nil {
		s.s1, s.lastTCP = first.tcp, first.tcp
	}
	if bulk := q.bulk.Load(); bulk != nil {
		if first := q.first.Load(); first != nil && bulk.at > first.at && bulk.bytes > first.bytes && !q.spliced.Load() {
			kbps := float64(bulk.bytes-first.bytes) * 8e6 / float64(bulk.at-first.at)
			if kbps > 0 && kbps < math.MaxInt64 {
				s.bulkKbps, s.hasBulk = int64(kbps), true
			}
		}
	}
	if peak := q.down.kbps.Load(); peak > 0 {
		s.peakKbps, s.hasPeak = peak, true
	}
	if peak := q.up.kbps.Load(); peak > 0 {
		s.upPeakKbps, s.hasUpPeak = peak, true
	}
	if u1 := q.u1.Load(); u1 != nil {
		s.u1, s.u2 = u1.tcp, u1.tcp
		if u2 := q.u2.Load(); u2 != nil {
			s.u2 = u2.tcp
		}
	}
	if s2 := q.s2.Load(); s2 != nil {
		s.s2, s.lastTCP = s2.tcp, s2.tcp
	}
	s.spliced = q.spliced.Load() && total >= int64(qualityMilestones[0])
	return s
}
