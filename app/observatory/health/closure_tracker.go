package health

import (
	"sync/atomic"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/stats"
)

// closureTracker keeps only abort and progress. It has no passive deadline,
// quality sample, target identity, TTFB or TCP_INFO reader. An enabled UDP
// detector may also use its byte counter as the required TCP proof of life.
type closureTracker struct {
	o                *Observer
	tag              string
	started          int64
	network          uint64
	labelEpoch       uint64
	labels           bool
	activity         bool
	proof            bool
	progress         bool
	tcpProof         bool
	proofPath        *path
	proofEpoch       uint64
	proofIncarnation uint64
	state            atomic.Int32
	aborted          atomic.Bool
	proved           atomic.Bool
	answered         atomic.Bool
	bytesRead        atomic.Int64
	lastRead         atomic.Int64
	lastWrite        atomic.Int64
	spliced          atomic.Bool
	// Guarded by trackMu, together with the registry and abort closure.
	abort      func()
	silentMark int64
	silentRead int64
	read       closureReadCounter
	write      closureWriteCounter
}

func (o *Observer) connectionRegistry(tag string) bool {
	if len(o.providers) > 0 {
		return true
	}
	if o.s.features.CloseUnchosenIdle && o.ohm != nil && sleeperOf(o.ohm.GetHandler(tag)) != nil {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, s := range o.e.selectors {
		if s.hasCandidate(tag) && (s.cfg.closePending || s.cfg.closeSilent) {
			return true
		}
	}
	return false
}

func (o *Observer) newClosureTracker(tag string, abort func(), tcpProof, register, activity, proof bool, snapshot *path, epoch, udpEpoch, incarnation, labelEpoch uint64, labels bool) *closureTracker {
	t := &closureTracker{o: o, tag: tag, abort: abort, started: o.nowNano(), network: epoch, labelEpoch: labelEpoch, labels: labels, tcpProof: tcpProof, activity: activity, proof: proof, progress: register}
	t.read.t = t
	t.proofPath = snapshot
	t.proofEpoch = udpEpoch
	t.proofIncarnation = incarnation
	if !register {
		t.abort = nil
	}
	if register {

		func() {
			o.trackMu.Lock()
			defer o.trackMu.Unlock()

			if o.closures == nil {
				o.closures = make(map[*closureTracker]struct{})
			}
			o.closures[t] = struct{}{}
			o.countTrackedLocked()

		}()
	}
	return t
}

func (t *closureTracker) Dialed(_ net.Conn, err error, read, write stats.Counter) (stats.Counter, stats.Counter) {
	if err != nil {
		t.Finished(err)
		return nil, nil
	}
	t.read.inner = read
	t.state.Store(trackerOpen)
	if t.activity || t.progress {
		t.write.t = t
		t.write.inner = write
		write = &t.write
	}
	return &t.read, write
}

func (t *closureTracker) Finished(error) {
	if t.state.Swap(trackerFinished) == trackerFinished {
		return
	}
	t.o.trackMu.Lock()
	delete(t.o.closures, t)
	t.abort = nil
	t.o.trackMu.Unlock()
	if t.o.failed.Load() {
		return
	}
	var woke bool
	t.o.locked("app/observatory/health/closure_tracker.go:closureTracker.Finished", func() { woke = t.o.e.used(t.tag, t.o.now()) })
	if woke || t.aborted.Load() {
		t.o.wake()
	}
}

func (t *closureTracker) closeFor(d downMark, before int64) func() {
	if t.tag != d.tag || t.abort == nil || t.started > before || t.state.Load() == trackerFinished || t.spliced.Load() {
		return nil
	}
	n := t.bytesRead.Load()
	shouldClose := d.pending && n == 0
	if d.silent {
		shouldClose = t.silentMark == before && n == t.silentRead && t.lastRead.Load() <= before
	}
	if !shouldClose {
		if d.watch {
			t.silentMark, t.silentRead = before, n
		}
		return nil
	}
	if t.aborted.CompareAndSwap(false, true) {
		return t.abort
	}
	return nil
}

func (t *closureTracker) idleSince(now int64) (int64, bool) {
	if t.spliced.Load() {
		return 0, false
	}
	// Registry trackers timestamp every read. Unlike the full tracker after
	// its success threshold, they need no counter sample to establish silence.
	// Using the check time here would restart the wait after a late first check.
	return max(t.lastRead.Load(), t.lastWrite.Load(), t.started), true
}

func (t *closureTracker) abandoned(now int64) bool {
	return now-max(t.lastRead.Load(), t.lastWrite.Load(), t.started) > int64(trackerAbandoned)
}

type closureReadCounter struct {
	t     *closureTracker
	inner stats.Counter
}

func (c *closureReadCounter) Value() int64 {
	if c.inner != nil {
		return c.inner.Value()
	}
	return c.t.bytesRead.Load()
}
func (c *closureReadCounter) Set(n int64) int64 {
	if c.inner != nil {
		return c.inner.Set(n)
	}
	return 0
}
func (c *closureReadCounter) Add(n int64) int64 {
	if n > 0 {
		total := int64(0)
		if c.t.progress || c.t.proof || c.t.tcpProof || c.t.labels {
			total = c.t.bytesRead.Add(n)
		}
		if c.t.progress {
			c.t.lastRead.Store(c.t.o.nowNano())
		}
		if c.t.activity {
			c.t.o.e.lastDemand.Store(c.t.o.now().UnixNano())
		}
		if c.t.proof && c.t.answered.CompareAndSwap(false, true) && !c.t.o.failed.Load() {
			c.t.o.locked("app/observatory/health/closure_tracker.go:closureReadCounter.Add", func() {
				c.t.o.noteControllerReplyLocked(c.t.tag, c.t.started, c.t.network, c.t.proofPath)
			})
		}
		if (c.t.tcpProof || c.t.proof || c.t.labels) && total >= c.t.o.s.passive.successBytes && c.t.proved.CompareAndSwap(false, true) {
			if c.t.labels {
				c.t.o.labelSucceeded(c.t.labelEpoch)
			}
			c.t.o.connectionSuccess(c.t.tag, c.t.started, c.t.network, c.t.proofPath, c.t.proofEpoch, c.t.proofIncarnation, c.t.tcpProof, c.t.proof)
		}
	}
	if c.inner != nil {
		return c.inner.Add(n)
	}
	return 0
}

// Without kernel progress a splice cannot be closed based on silence.
func (c *closureReadCounter) Spliced() { c.t.spliced.Store(true) }

var _ outbound.DialTracker = (*closureTracker)(nil)

// connectionSignals identifies enabled consumers of activity/proof without
// enabling passive detectors or telemetry collection.
func (o *Observer) connectionSignals(tag string) (activity, proof bool) {
	f := o.s.features
	proof = f.passiveTCP() || f.NetworkMemory || f.UserPresent || f.Connectivity
	activity = len(o.providers) > 0
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, s := range o.e.selectors {
		if !s.hasCandidate(tag) {
			continue
		}
		if s.cfg.returnToCheaper.interval > 0 || s.cfg.onNetworkChange != NetworkChangeMode_IGNORE || (f.Candidates && len(s.inventory) > len(s.candidates)) {
			activity = true
		}
	}
	return
}

type closureWriteCounter struct {
	t     *closureTracker
	inner stats.Counter
}

func (c *closureWriteCounter) Value() int64 {
	if c.inner != nil {
		return c.inner.Value()
	}
	return 0
}
func (c *closureWriteCounter) Set(n int64) int64 {
	if c.inner != nil {
		return c.inner.Set(n)
	}
	return 0
}
func (c *closureWriteCounter) Add(n int64) int64 {
	if n > 0 {
		if c.t.activity {
			c.t.o.e.lastDemand.Store(c.t.o.now().UnixNano())
		}
		if c.t.progress {
			c.t.lastWrite.Store(c.t.o.nowNano())
		}
	}
	if c.inner != nil {
		return c.inner.Add(n)
	}
	return 0
}
