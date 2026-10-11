package health

import (
	"math"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xtls/xray-core/common/net"
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
	spliced   bool
	s1, s2    tcpQualitySnapshot
	lastTCP   tcpQualitySnapshot
}

// connectionQuality holds measurements only. No path decision reads it.
type connectionQuality struct {
	network    uint64
	started    int64
	dial       atomic.Int64
	hasDial    atomic.Bool
	socket     atomic.Pointer[syscall.RawConn]
	first      atomic.Pointer[qualityFirstPoint]
	firstClaim atomic.Bool
	bulk       atomic.Pointer[qualityBulkPoint]
	s2         atomic.Pointer[qualityTCPPoint]
	next       atomic.Uint64
	spliced    atomic.Bool
	initial    atomic.Bool
}

func (o *Observer) newConnectionQuality(udp bool, started int64) *connectionQuality {
	if udp || o.s.passive.skipQuality {
		return nil
	}
	q := &connectionQuality{network: o.qualityEpoch.Load(), started: started}
	q.next.Store(qualityMilestones[0])
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

func (q *connectionQuality) firstRead(o *Observer, now, bytes, ttfb int64) {
	if q == nil || q.firstClaim.Swap(true) {
		return
	}
	// This syscall runs before answered takes the observer lock.
	p := &qualityFirstPoint{at: now, bytes: bytes, ttfb: time.Duration(max(ttfb, 0)), tcp: q.snapshot(o)}
	q.first.CompareAndSwap(nil, p)
}

func (q *connectionQuality) needsMark(total int64) bool {
	return q != nil && uint64(total) >= q.next.Load()
}

func (q *connectionQuality) mark(o *Observer, now, total int64) {
	if q == nil {
		return
	}
	boundary := q.next.Load()
	if uint64(total) < boundary {
		return
	}
	// A single counted read can cross several milestones. They are the same
	// observation, so use one clock reading and one socket snapshot for it.
	// The next mark is the first milestone beyond the observed byte total.
	next := uint64(math.MaxUint64)
	for _, boundary := range qualityMilestones {
		if boundary > uint64(total) {
			next = boundary
			break
		}
	}
	for !q.next.CompareAndSwap(boundary, next) {
		boundary = q.next.Load()
		if uint64(total) < boundary {
			return
		}
	}
	// Successful snapshots have their own ordering. A newer byte/time mark
	// may have failed to read TCP_INFO while an earlier successful one was
	// still in flight. Neither may discard the other's measurements.
	if info := q.snapshot(o); info.present != 0 {
		point := &qualityTCPPoint{bytes: total, tcp: info}
		for {
			previous := q.s2.Load()
			if previous != nil && previous.bytes >= total {
				break
			}
			if q.s2.CompareAndSwap(previous, point) {
				break
			}
		}
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

func (q *connectionQuality) onSpliced() {
	if q != nil {
		q.spliced.Store(true)
		q.next.Store(math.MaxUint64)
		// Its counter may next report all spliced bytes after the socket
		// closed. That is no timestamp of the first answer.
		if q.first.Load() == nil {
			q.firstClaim.Store(true)
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
	if s2 := q.s2.Load(); s2 != nil {
		s.s2, s.lastTCP = s2.tcp, s2.tcp
	}
	s.spliced = q.spliced.Load() && total >= int64(qualityMilestones[0])
	return s
}
