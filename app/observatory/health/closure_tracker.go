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
	bytesRead        atomic.Int64
	lastRead         atomic.Int64
	spliced          atomic.Bool
	// Guarded by trackMu, together with the registry and abort closure.
	abort                func()
	silentMark, idleMark int64
	silentRead, idleRead int64
	read                 closureReadCounter
	write                closureWriteCounter
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

func (o *Observer) newClosureTracker(tag string, abort func(), tcpProof, register, activity, proof bool) *closureTracker {
	t := &closureTracker{o: o, tag: tag, abort: abort, started: o.nowNano(), network: o.generation.Load(), tcpProof: tcpProof, activity: activity, proof: proof, progress: register}
	t.read.t = t
	if tcpProof || proof {
		o.mu.Lock()
		t.proofPath = o.e.pathAt(tag, o.now())
		t.proofEpoch = o.e.udpEpoch
		t.proofIncarnation = t.proofPath.udpIncarnation
		o.mu.Unlock()
	}
	if !register {
		t.abort = nil
	}
	if register {
		o.trackMu.Lock()
		if o.closures == nil {
			o.closures = make(map[*closureTracker]struct{})
		}
		o.closures[t] = struct{}{}
		o.trackMu.Unlock()
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
	if t.activity {
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
	t.o.mu.Lock()
	woke := t.o.e.used(t.tag, t.o.now())
	t.o.mu.Unlock()
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
	n := t.bytesRead.Load()
	if t.idleMark != 0 && n == t.idleRead && t.lastRead.Load() <= t.idleMark {
		return t.idleMark, true
	}
	t.idleMark, t.idleRead = now, n
	return max(t.lastRead.Load(), t.started), true
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
		if c.t.progress || c.t.proof || c.t.tcpProof {
			total = c.t.bytesRead.Add(n)
		}
		if c.t.progress {
			c.t.lastRead.Store(c.t.o.nowNano())
		}
		if c.t.activity {
			c.t.o.e.lastDemand.Store(c.t.o.now().UnixNano())
		}
		if (c.t.tcpProof || c.t.proof) && total >= c.t.o.s.passive.successBytes && c.t.proved.CompareAndSwap(false, true) {
			c.t.o.mu.Lock()
			if c.t.network == c.t.o.e.generation {
				p := c.t.o.e.paths[c.t.tag]
				// A newer network/replacement may not inherit an older connection's proof.
				if p == c.t.proofPath && c.t.proofEpoch == c.t.o.e.udpEpoch && p.udpIncarnation == c.t.proofIncarnation {
					if c.t.tcpProof {
						p.lastTCPSuccess = c.t.o.now()
						c.t.o.sweepUDPEvidenceLocked(c.t.o.now())
					}
					if c.t.proof {
						c.t.o.e.onSuccess(c.t.tag, c.t.o.now())
					}
				}
			}
			c.t.o.mu.Unlock()
			c.t.o.wake()
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
	proof = f.NetworkMemory || f.LabelRequest || f.UserPresent || f.Connectivity
	activity = len(o.providers) > 0
	o.mu.Lock()
	for _, s := range o.e.selectors {
		if !s.hasCandidate(tag) {
			continue
		}
		if s.cfg.returnToCheaper.interval > 0 || s.cfg.onNetworkChange != NetworkChangeMode_IGNORE || (f.Candidates && len(s.inventory) > len(s.candidates)) {
			activity = true
		}
	}
	o.mu.Unlock()
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
		c.t.o.e.lastDemand.Store(c.t.o.now().UnixNano())
	}
	if c.inner != nil {
		return c.inner.Add(n)
	}
	return 0
}
