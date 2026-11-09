package health

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
)

const (
	udpPortDNS = iota
	udpPortQUIC
	udpPortSTUN
	udpPortRTC
	udpPortOther
	udpPortCount
)

var udpPortNames = [udpPortCount]string{"dns", "quic", "stun", "rtc", "other"}

type udpPortTotals struct {
	flows      int64
	unanswered int64
}

type udpPathTotals struct {
	flows      int64
	unanswered int64
	bytesUp    int64
	bytesDown  int64
	writes     int64
	reads      int64
	ports      [udpPortCount]udpPortTotals
}

func (t udpPathTotals) delta(base udpPathTotals) udpPathTotals {
	d := udpPathTotals{
		flows:      t.flows - base.flows,
		unanswered: t.unanswered - base.unanswered,
		bytesUp:    t.bytesUp - base.bytesUp,
		bytesDown:  t.bytesDown - base.bytesDown,
		writes:     t.writes - base.writes,
		reads:      t.reads - base.reads,
	}
	for i := range d.ports {
		d.ports[i] = udpPortTotals{
			flows:      t.ports[i].flows - base.ports[i].flows,
			unanswered: t.ports[i].unanswered - base.ports[i].unanswered,
		}
	}
	return d
}

func (t udpPathTotals) reportPorts() map[string]reportUDPPort {
	var ports map[string]reportUDPPort
	for i, count := range t.ports {
		if count.flows == 0 && count.unanswered == 0 {
			continue
		}
		if ports == nil {
			ports = make(map[string]reportUDPPort)
		}
		ports[udpPortNames[i]] = reportUDPPort{Flows: count.flows, Unanswered: count.unanswered}
	}
	return ports
}

func udpPortBucket(port net.Port) int {
	switch p := port.Value(); {
	case p == 53:
		return udpPortDNS
	case p == 443:
		return udpPortQUIC
	case p == 3478 || p == 3479 || p == 19302:
		return udpPortSTUN
	case p >= 50000:
		return udpPortRTC
	default:
		return udpPortOther
	}
}

type udpFlow struct {
	o           *Observer
	tag         string
	path        *path
	bucket      int
	target      string
	epoch       uint64
	incarnation uint64

	mu                    sync.Mutex
	closed                bool
	evidenceChecked       bool
	evidenceAnswered      bool
	lastEvidenceBytesDown int64
	bytesUp               int64
	bytesDown             int64
	reads                 int64
	writes                atomic.Int64
	firstWrite            int64
	firstRead             int64
}

var _ outbound.UDPFlowObserver = (*Observer)(nil)
var _ outbound.UDPFlowTracker = (*udpFlow)(nil)

func (o *Observer) StartUDPFlow(ctx context.Context, tag string, firstPort net.Port) outbound.UDPFlowTracker {
	if (!o.s.features.Report && !o.s.features.UdpReplies) || probeStateFrom(ctx) != nil || !o.s.observes(tag) {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.e.report.final {
		return nil
	}
	select {
	case <-o.done:
		return nil
	default:
	}
	p := o.e.pathAt(tag, o.now())
	p.udpFlowsActive++
	f := &udpFlow{o: o, tag: tag, path: p, bucket: udpPortBucket(firstPort), epoch: o.e.udpEpoch, incarnation: p.udpIncarnation}
	if obs := session.OutboundsFromContext(ctx); len(obs) > 0 {
		target := obs[len(obs)-1].Target
		if target.Address != nil {
			f.target = target.String()
		}
	}
	if o.udpFlows == nil {
		o.udpFlows = make(map[*udpFlow]struct{})
	}
	o.udpFlows[f] = struct{}{}
	return f
}

func (f *udpFlow) Uplink(bytes, packets int64) {
	if packets <= 0 {
		return
	}
	first := false
	func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if !f.closed {
			f.bytesUp += bytes
			f.writes.Add(packets)
			if f.firstWrite == 0 {
				f.firstWrite = f.o.nowNano()
				first = true
			}
		}
	}()
	if first {
		f.o.udpFirstWrite(f, f.o.now())
	}
}

func (f *udpFlow) Downlink(bytes, packets int64) {
	if packets <= 0 {
		return
	}
	answered := false
	func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if !f.closed {
			answered = bytes > 0 && f.bytesDown == 0
			f.bytesDown += bytes
			f.reads += packets
			if f.firstRead == 0 {
				f.firstRead = f.o.nowNano()
			}
		}
	}()
	if answered {
		f.o.udpAnswered(f, f.o.now())
	}
}

func (f *udpFlow) Finished() {
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	f.finishLocked()
}

// finishLocked runs with Observer.mu held, including when a final report
// closes all associations while their I/O goroutines are still stopping.
func (f *udpFlow) finishLocked() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	f.o.retainUDPEvidenceLocked(f, f.o.now())
	delete(f.o.udpFlows, f)
	f.path.udpFlowsActive--
	if f.o.e.paths[f.tag] != f.path {
		return
	}
	st := &f.path.stats
	writes := f.writes.Load()
	unanswered := writes > 0 && f.bytesDown == 0
	st.udp.flows++
	st.udp.bytesUp += f.bytesUp
	st.udp.bytesDown += f.bytesDown
	st.udp.writes += writes
	st.udp.reads += f.reads
	st.udp.ports[f.bucket].flows++
	if unanswered {
		st.udp.unanswered++
		st.udp.ports[f.bucket].unanswered++
	}
	if f.firstWrite > 0 && f.firstRead >= f.firstWrite {
		st.udpFirstReply.add(time.Duration(f.firstRead - f.firstWrite))
	}
}

func (o *Observer) finishUDPFlowsLocked() {
	for f := range o.udpFlows {
		f.finishLocked()
	}
	clear(o.udpEvidencePending)
}
