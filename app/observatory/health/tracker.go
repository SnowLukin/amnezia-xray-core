package health

import (
	"context"
	goerrors "errors"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	xctx "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/stats"
)

const (
	trackerDialing int32 = iota
	trackerOpen
	trackerFinished
)

const (
	reportedDialTimeout uint32 = 1 << iota
	reportedNoResponse
	reportedStall
	reportedPartialStall
	reportedSuccess
	// The observer ended the connection itself; how it ends tells nothing.
	reportedAborted
)

// Trackers without any traffic for this long are dropped. This covers
// connections whose end the handler never reports, such as mux workers.
const trackerAbandoned = 10 * time.Minute

// connTracker follows one user connection. The counters it returns from
// Dialed keep per-read state in atomics; the observer is
// involved on the first reply, success and deadlines, not per packet.
type connTracker struct {
	o      *Observer
	tag    string
	target string
	server string
	conn   uint32
	// Network generation the connection started in.
	network uint64
	// UDP flows have no request and answer to wait for; only their dial is judged.
	udp bool
	// Ends the user connection; nil when none waits for this dial.
	abort func()

	started int64
	dialed  atomic.Int64
	state   atomic.Int32

	bytesRead    atomic.Int64
	bytesWritten atomic.Int64
	firstRead    atomic.Int64
	firstWrite   atomic.Int64
	lastRead     atomic.Int64
	lastWrite    atomic.Int64
	// Written since the last read: how much of a request waits for an answer.
	pendingWrite atomic.Int64
	ttfb         atomic.Int64
	reported     atomic.Uint32
	// Bytes now move in bulk, without per-read counting. Progress then comes
	// from the kernel socket where the platform reports it.
	spliced atomic.Bool
	socket  atomic.Pointer[syscall.RawConn]
	// The kernel counts everything since connect, the outer handshake too, so
	// progress is what it received after the splice started.
	kernelBase atomic.Uint64
	kernelRx   atomic.Uint64
	// The tracker has a deadline the sweep must check.
	watched atomic.Bool
	// What it had received when its outbound was found down at
	// silentMark, see silence.go; zero silentMark when not noted. Guarded
	// by the observer's trackMu.
	silentMark int64
	silentRead uint64
	// The same for the check of an outbound no balancer chooses for idle
	// connections at idleMark, see idle.go. Guarded by trackMu as well.
	idleMark int64
	idleRead uint64

	dialedConn net.Conn
	quality    *connectionQuality
	read       stats.Counter
	write      stats.Counter
	// The counters handed to the connection live here to save allocations.
	readCounter  trackerReadCounter
	writeCounter trackerWriteCounter
}

func (t *connTracker) mark(flag uint32) bool {
	for {
		old := t.reported.Load()
		if old&flag != 0 {
			return false
		}
		if t.reported.CompareAndSwap(old, old|flag) {
			return true
		}
	}
}

func (t *connTracker) has(flag uint32) bool {
	return t.reported.Load()&flag != 0
}

func (t *connTracker) watch() {
	if !t.watched.Load() && t.watched.CompareAndSwap(false, true) {
		t.o.watch(t)
	}
}

// Dialed implements outbound.DialTracker.
func (t *connTracker) Dialed(conn net.Conn, err error, read, write stats.Counter) (stats.Counter, stats.Counter) {
	now := t.o.nowNano()
	t.dialed.Store(now)
	if err != nil {
		t.state.Store(trackerFinished)
		t.o.untrack(t)
		if goerrors.Is(err, context.Canceled) || t.has(reportedAborted) {
			return nil, nil
		}
		if !t.has(reportedDialTimeout) {
			t.o.failure(t, classifyDialError(err))
		}
		return nil, nil
	}
	t.dialedConn = conn
	t.quality.dialed(conn, now)
	t.read = read
	t.write = write
	t.state.Store(trackerOpen)
	t.readCounter.t, t.writeCounter.t = t, t
	return &t.readCounter, &t.writeCounter
}

func (t *connTracker) onSpliced() {
	if t.spliced.Swap(true) {
		return
	}
	rc := rawConnOf(t.dialedConn)
	if rc == nil {
		return
	}
	// Zero means the kernel does not count: it has received the handshake.
	rx, ok := kernelReceived(rc)
	if !ok || rx == 0 {
		return
	}
	t.kernelBase.Store(rx)
	t.kernelRx.Store(rx)
	t.socket.Store(&rc)
	t.watch()
}

// sweepSpliced judges a spliced connection by what its socket received: the
// same success and young-and-silent checks as for counted connections.
func (t *connTracker) sweepSpliced(now int64) bool {
	p := &t.o.s.passive
	rc := t.socket.Load()
	if rc == nil || t.has(reportedSuccess) || now-t.dialed.Load() > int64(p.partialStallAge+p.stallTimeout) {
		return false
	}
	rx, ok := kernelReceived(*rc)
	if !ok {
		return false
	}
	if rx > t.kernelRx.Load() {
		t.kernelRx.Store(rx)
		t.lastRead.Store(now)
		if t.firstRead.CompareAndSwap(0, now) {
			from := t.firstWrite.Load()
			if from == 0 {
				from = t.dialed.Load()
			}
			t.quality.firstRead(t.o, now, t.bytesRead.Load(), now-from)
			t.o.answered(t)
		}
	}
	got := t.bytesRead.Load() + int64(rx-t.kernelBase.Load())
	if got >= p.successBytes {
		if t.mark(reportedSuccess) {
			t.o.success(t)
		}
		return false
	}
	if p.partialStallAge == 0 || t.has(reportedPartialStall) || t.lastRead.Load()-t.dialed.Load() >= int64(p.partialStallAge) {
		return false
	}
	if got >= p.partialMinBytes && now-t.lastRead.Load() > int64(p.stallTimeout) && t.mark(reportedPartialStall) {
		t.o.failure(t, failPartialStall)
		return false
	}
	return true
}

func classifyDialError(err error) failureKind {
	if strings.Contains(err.Error(), "REALITY: processed invalid connection") {
		return failUnverified
	}
	return failDial
}

func (t *connTracker) onRead(n int64) {
	if n <= 0 {
		return
	}
	total := t.bytesRead.Add(n)
	if t.pendingWrite.Load() != 0 {
		t.pendingWrite.Store(0)
	}
	p := &t.o.s.passive
	timing := total-n < p.successBytes
	if !timing && !t.quality.needsMark(total) {
		// The connection already proved the outbound works and nothing about
		// its reads is timed any more, so bulk downloads skip the clock.
		return
	}
	now := t.o.nowNano()
	if !timing {
		t.quality.mark(t.o, now, total)
		return
	}
	t.lastRead.Store(now)
	if t.firstRead.Load() == 0 && t.firstRead.CompareAndSwap(0, now) {
		if t.quality != nil {
			qualityFrom := t.firstWrite.Load()
			if qualityFrom == 0 {
				qualityFrom = t.dialed.Load()
			}
			t.quality.firstRead(t.o, now, total, now-qualityFrom)
		}
		t.o.answered(t)
		// Preserve the legacy origin load after answered, including concurrent
		// writes while that callback waits for the observer mutex.
		from := t.firstWrite.Load()
		if from == 0 {
			from = t.dialed.Load()
		}
		t.ttfb.Store(now - from)
	}
	t.quality.mark(t.o, now, total)
	if total >= p.successBytes {
		if total-n < p.successBytes && t.mark(reportedSuccess) {
			t.o.success(t)
		}
		return
	}
	if !t.udp && p.partialStallAge > 0 && total >= p.partialMinBytes {
		t.watch()
	}
}

func (t *connTracker) onWrite(n int64) {
	if n <= 0 {
		return
	}
	now := t.o.nowNano()
	t.bytesWritten.Add(n)
	t.lastWrite.Store(now)
	if t.firstWrite.Load() == 0 {
		t.firstWrite.CompareAndSwap(0, now)
	}
	if t.udp {
		return
	}
	if t.firstRead.Load() == 0 {
		t.watch()
		return
	}
	if t.pendingWrite.Add(n) >= t.o.s.passive.stallMinWrite {
		t.watch()
	}
}

// Finished implements outbound.DialTracker.
func (t *connTracker) Finished(err error) {
	if t.state.Swap(trackerFinished) == trackerFinished {
		return
	}
	// Ended by a failure before any answer: a transport that connects
	// lazily, such as XHTTP, shows a dead server this way. A connection the
	// user closed ends canceled and tells nothing.
	if err != nil && !goerrors.Is(err, context.Canceled) && !t.udp && !t.spliced.Load() && !t.has(reportedAborted) &&
		t.firstRead.Load() == 0 && t.mark(reportedNoResponse) {
		t.o.failure(t, failClosed)
	}
	t.o.untrack(t)
	// The proxy is done with the connection; a reference to the tracker left
	// somewhere must not keep it and its buffers alive.
	t.dialedConn = nil
	t.socket.Store(nil)
	t.o.finished(t)
	if t.quality != nil {
		t.quality.socket.Store(nil)
	}
}

// sweep checks the deadlines of one connection. It returns whether the
// connection still has a deadline to watch, and whether to drop it.
func (t *connTracker) sweep(now int64) (watching, drop bool) {
	p := &t.o.s.passive
	if t.has(reportedAborted) {
		return false, false
	}
	switch t.state.Load() {
	case trackerDialing:
		if now-t.started > int64(p.dialTimeout) && t.mark(reportedDialTimeout) {
			t.o.failure(t, failDialTimeout)
		}
		// A dial that never reports back would otherwise be watched forever.
		// It stays tracked until it ends: a connection waiting inside a
		// tunnel, whose dial ends only when the server answers, may wait
		// longer, and closePending must still end it when its outbound is
		// found down. An abandoned one goes with the others, see track.
		return now-t.started <= 4*int64(p.dialTimeout), false
	case trackerOpen:
	default:
		return false, true
	}
	if t.udp {
		return false, false
	}
	if t.spliced.Load() {
		return t.sweepSpliced(now), false
	}
	firstWrite, firstRead := t.firstWrite.Load(), t.firstRead.Load()
	lastWrite, lastRead := t.lastWrite.Load(), t.lastRead.Load()
	if firstRead == 0 {
		if t.has(reportedNoResponse) {
			return false, false
		}
		from := firstWrite
		if from == 0 {
			// Nothing went out yet. On a working path one side speaks within
			// seconds of the dial; a transport that connects lazily can hold
			// the request inside when its server is gone.
			from = t.dialed.Load()
		}
		if now-from > int64(p.firstByteTimeout) && t.mark(reportedNoResponse) {
			t.o.failure(t, failNoResponse)
			return false, false
		}
		return true, false
	}
	pending := t.pendingWrite.Load()
	if pending >= p.stallMinWrite && !t.has(reportedStall) {
		if now-lastWrite > int64(p.stallTimeout) && t.mark(reportedStall) {
			t.o.failure(t, failStall)
			return false, false
		}
		return true, false
	}
	read := t.bytesRead.Load()
	if p.partialStallAge > 0 && read >= p.partialMinBytes && read < p.successBytes &&
		lastRead-t.dialed.Load() < int64(p.partialStallAge) && !t.has(reportedPartialStall) {
		if now-lastRead > int64(p.stallTimeout) && t.mark(reportedPartialStall) {
			t.o.failure(t, failPartialStall)
			return false, false
		}
		return true, false
	}
	return false, false
}

func (t *connTracker) abandoned(now int64) bool {
	last := maxInt64(t.lastWrite.Load(), t.lastRead.Load(), t.dialed.Load(), t.started)
	return now-last > int64(trackerAbandoned)
}

func maxInt64(v ...int64) int64 {
	m := v[0]
	for _, x := range v[1:] {
		if x > m {
			m = x
		}
	}
	return m
}

type trackerReadCounter struct{ t *connTracker }

func (c *trackerReadCounter) Value() int64 {
	if c.t.read != nil {
		return c.t.read.Value()
	}
	return c.t.bytesRead.Load()
}

func (c *trackerReadCounter) Set(v int64) int64 {
	if c.t.read != nil {
		return c.t.read.Set(v)
	}
	return 0
}

func (c *trackerReadCounter) Add(n int64) int64 {
	c.t.onRead(n)
	if c.t.read != nil {
		return c.t.read.Add(n)
	}
	return 0
}

// Spliced is called before bytes start to move without per-read counting.
func (c *trackerReadCounter) Spliced() {
	c.t.quality.onSpliced()
	c.t.onSpliced()
}

type trackerWriteCounter struct{ t *connTracker }

func (c *trackerWriteCounter) Value() int64 {
	if c.t.write != nil {
		return c.t.write.Value()
	}
	return c.t.bytesWritten.Load()
}

func (c *trackerWriteCounter) Set(v int64) int64 {
	if c.t.write != nil {
		return c.t.write.Set(v)
	}
	return 0
}

func (c *trackerWriteCounter) Add(n int64) int64 {
	c.t.onWrite(n)
	if c.t.write != nil {
		return c.t.write.Add(n)
	}
	return 0
}

// Spliced is called before bytes start to move without per-write counting.
func (c *trackerWriteCounter) Spliced() { c.t.onSpliced() }

// probeTracker counts the traffic of the observatory's own probes. It never
// produces evidence: probe results are judged by the probe itself.
type probeTracker struct {
	state *probeState
}

func (t *probeTracker) Dialed(_ net.Conn, err error, read, write stats.Counter) (stats.Counter, stats.Counter) {
	t.state.recordDial(err)
	if err != nil {
		return nil, nil
	}
	return &probeCounter{t.state.bytes, read}, &probeCounter{t.state.bytes, write}
}

func (t *probeTracker) Finished(error) {}

type probeCounter struct {
	bytes *atomic.Int64
	inner stats.Counter
}

func (c *probeCounter) Value() int64 {
	if c.inner != nil {
		return c.inner.Value()
	}
	return c.bytes.Load()
}

func (c *probeCounter) Set(v int64) int64 {
	if c.inner != nil {
		return c.inner.Set(v)
	}
	return 0
}

func (c *probeCounter) Add(n int64) int64 {
	c.bytes.Add(n)
	if c.inner != nil {
		return c.inner.Add(n)
	}
	return 0
}

// witnessTracker follows a connection of an outbound that only tells
// whether the network works. It reports once, when the connection has
// received enough to count as working.
type witnessTracker struct {
	o           *Observer
	bytes       atomic.Int64
	done        atomic.Bool
	quality     *connectionQuality
	qualityRead atomic.Int64
	dialedAt    atomic.Int64
	firstWrite  atomic.Int64
	finished    atomic.Bool
}

func (t *witnessTracker) Dialed(conn net.Conn, err error, read, write stats.Counter) (stats.Counter, stats.Counter) {
	if err != nil {
		return nil, nil
	}
	if t.quality != nil {
		now := t.o.nowNano()
		t.dialedAt.Store(now)
		t.quality.dialed(conn, now)
		write = &witnessWriteCounter{t: t, inner: write}
	}
	return &witnessCounter{t, read}, write
}

func (t *witnessTracker) Finished(error) {
	if t.quality != nil && !t.finished.Swap(true) {
		t.o.qualityWitnessFinished(t)
		t.quality.socket.Store(nil)
	}
}

type witnessCounter struct {
	t     *witnessTracker
	inner stats.Counter
}

func (c *witnessCounter) Value() int64 {
	if c.inner != nil {
		return c.inner.Value()
	}
	return c.t.bytes.Load()
}

func (c *witnessCounter) Set(v int64) int64 {
	if c.inner != nil {
		return c.inner.Set(v)
	}
	return 0
}

func (c *witnessCounter) Add(n int64) int64 {
	if q := c.t.quality; q != nil && n > 0 {
		total := c.t.qualityRead.Add(n)
		now := int64(0)
		if !q.firstClaim.Load() {
			now = c.t.o.nowNano()
			from := c.t.firstWrite.Load()
			if from == 0 {
				from = c.t.dialedAt.Load()
			}
			if q.firstRead(c.t.o, now, total, now-from) {
				c.t.o.qualityWitnessAnswered(c.t)
			}
		}
		if q.needsMark(total) {
			if now == 0 {
				now = c.t.o.nowNano()
			}
			q.mark(c.t.o, now, total)
		}
	}
	if !c.t.done.Load() && c.t.bytes.Add(n) >= c.t.o.s.passive.successBytes && c.t.done.CompareAndSwap(false, true) {
		c.t.o.witnessed()
	}
	if c.inner != nil {
		return c.inner.Add(n)
	}
	return 0
}

func (c *witnessCounter) Spliced() { c.t.quality.onSpliced() }

type witnessWriteCounter struct {
	t     *witnessTracker
	inner stats.Counter
}

func (c *witnessWriteCounter) Value() int64 {
	if c.inner != nil {
		return c.inner.Value()
	}
	return 0
}

func (c *witnessWriteCounter) Set(v int64) int64 {
	if c.inner != nil {
		return c.inner.Set(v)
	}
	return 0
}

func (c *witnessWriteCounter) Add(n int64) int64 {
	if n > 0 && c.t.firstWrite.Load() == 0 {
		c.t.firstWrite.CompareAndSwap(0, c.t.o.nowNano())
	}
	if c.inner != nil {
		return c.inner.Add(n)
	}
	return 0
}

// Upload splice does not remove the timing of counted download reads.
func (c *witnessWriteCounter) Spliced() {}

type probeContextKey struct{}

func withProbe(ctx context.Context, bytes *atomic.Int64) context.Context {
	return context.WithValue(ctx, probeContextKey{}, &probeState{bytes: bytes})
}

func probeStateFrom(ctx context.Context) *probeState {
	v, _ := ctx.Value(probeContextKey{}).(*probeState)
	return v
}

// targetFrom returns the destination of the user connection and whether it is UDP.
func targetFrom(ctx context.Context) (string, bool) {
	obs := session.OutboundsFromContext(ctx)
	if len(obs) == 0 {
		return "", false
	}
	ob := obs[len(obs)-1]
	if ob.Target.Address == nil {
		return "", ob.Target.Network == net.Network_UDP
	}
	return ob.Target.Address.String(), ob.Target.Network == net.Network_UDP
}

func connFrom(ctx context.Context) uint32 {
	return uint32(xctx.IDFromContext(ctx))
}

var (
	_ outbound.DialTracker = (*connTracker)(nil)
	_ outbound.DialTracker = (*probeTracker)(nil)
	_ outbound.DialTracker = (*witnessTracker)(nil)
)

func destString(d net.Destination) string {
	if d.Address == nil {
		return ""
	}
	return d.Address.String()
}
