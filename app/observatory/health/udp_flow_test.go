package health

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

func newUDPObserver(t testing.TB) *Observer {
	t.Helper()
	o := newObserver(context.Background(), &Config{Features: &FeatureConfig{Report: v2Bool(true), UdpReplies: v2Bool(true)}, Paths: observed("a1", "b1"), Report: &ReportConfig{Interval: int64(time.Second)}})
	return o
}

func udpReport(t *testing.T, raw []byte) reportView {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("expected a report")
	}
	var v reportView
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestUDPFlowPortBucketsAndObservationScope(t *testing.T) {
	for _, tc := range []struct {
		port net.Port
		want int
	}{
		{53, udpPortDNS}, {443, udpPortQUIC}, {3478, udpPortSTUN},
		{3479, udpPortSTUN}, {19302, udpPortSTUN}, {50000, udpPortRTC},
		{65535, udpPortRTC}, {1024, udpPortOther},
	} {
		if got := udpPortBucket(tc.port); got != tc.want {
			t.Errorf("port %d bucket %d, want %d", tc.port, got, tc.want)
		}
	}
	o := newUDPObserver(t)
	defer o.Close()
	if got := o.StartUDPFlow(context.Background(), "unobserved", 53); got != nil {
		t.Fatal("unobserved tag created a flow")
	}
	var probeBytes atomic.Int64
	if got := o.StartUDPFlow(withProbe(context.Background(), &probeBytes), "a1", 53); got != nil {
		t.Fatal("probe created a user UDP flow")
	}
}

func TestUDPFlowClosesInNextWindowWithFirstReply(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	now := t0
	o.clock = func() time.Time { return now }
	o.base = t0.Add(-time.Second)
	o.e.report.begin(t0)
	f := o.StartUDPFlow(context.Background(), "a1", 53).(*udpFlow)
	now = t0.Add(100 * time.Millisecond)
	f.Uplink(12, 2)
	o.mu.Lock()
	o.e.report.decide()
	o.mu.Unlock()
	now = t0.Add(2 * time.Second)
	first := udpReport(t, o.TakeReport(false))
	if got := first.Paths["a1"].UDPFlows; got != 0 {
		t.Fatalf("open flow counted in first window: %d", got)
	}
	now = t0.Add(2300 * time.Millisecond)
	f.Downlink(7, 1)
	f.Finished()
	f.Finished()
	o.mu.Lock()
	o.e.report.decide()
	o.mu.Unlock()
	now = t0.Add(4 * time.Second)
	second := udpReport(t, o.TakeReport(false))
	p := second.Paths["a1"]
	if p.UDPFlows != 1 || p.UDPFlowsUnanswered != 0 || p.UDPBytesUp != 12 || p.UDPBytesDown != 7 ||
		p.UDPWrites != 2 || p.UDPReads != 1 || p.UDPPorts["dns"] != (reportUDPPort{Flows: 1}) || len(p.UDPPorts) != 1 ||
		p.UDPFirstReplyP50Ms != 3000 {
		t.Fatalf("second window UDP counts: %+v", p)
	}
}

func TestUDPFlowFirstReplyRequiresEarlierUplink(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	now := t0
	o.clock = func() time.Time { return now }
	o.base = t0.Add(-time.Second)
	first := o.StartUDPFlow(context.Background(), "a1", 53).(*udpFlow)
	now = t0.Add(time.Second)
	first.Downlink(3, 1)
	now = t0.Add(2 * time.Second)
	first.Uplink(4, 1)
	first.Finished()
	if got := first.path.stats.udpFirstReply.total; got != 0 {
		t.Fatalf("reply before uplink produced %d latency samples", got)
	}
	second := o.StartUDPFlow(context.Background(), "a1", 19302).(*udpFlow)
	now = t0.Add(3 * time.Second)
	second.Uplink(4, 1)
	now = t0.Add(3050 * time.Millisecond)
	second.Downlink(3, 1)
	second.Finished()
	if got := second.path.stats.udpFirstReply.quantile(0.5); got != 50*time.Millisecond {
		t.Fatalf("first reply bucket %v, want 50ms", got)
	}
}

func TestUDPFlowTwoLogicalLinksAndFinalOnce(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	o.mu.Lock()
	o.e.report.decide()
	o.mu.Unlock()
	first := o.StartUDPFlow(context.Background(), "a1", 53).(*udpFlow)
	second := o.StartUDPFlow(context.Background(), "a1", 19302).(*udpFlow)
	first.Uplink(4, 1)
	second.Uplink(8, 2)
	first.Finished()
	snapshot := udpReport(t, o.PeekReport())
	if snapshot.Paths["a1"].UDPFlows != 1 || snapshot.Paths["a1"].UDPPorts["stun"].Flows != 0 {
		t.Fatalf("snapshot finalized an open logical link: %+v", snapshot.Paths["a1"])
	}
	final := udpReport(t, o.TakeReport(true))
	p := final.Paths["a1"]
	if p.UDPFlows != 2 || p.UDPFlowsUnanswered != 2 || p.UDPWrites != 3 || p.UDPBytesUp != 12 ||
		p.UDPPorts["dns"] != (reportUDPPort{Flows: 1, Unanswered: 1}) ||
		p.UDPPorts["stun"] != (reportUDPPort{Flows: 1, Unanswered: 1}) {
		t.Fatalf("two logical links on one path: %+v", p)
	}
	second.Downlink(3, 1)
	second.Finished()
	o.Close()
	if o.TakeReport(true) != nil || o.e.path("a1").stats.udp.flows != 2 {
		t.Fatal("final report or late finish counted a flow twice")
	}
}

func TestUDPFlowConcurrentFinishDoesNotRaceCounters(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	f := o.StartUDPFlow(context.Background(), "a1", 443).(*udpFlow)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			f.Uplink(100, 1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			f.Downlink(100, 1)
		}
	}()
	f.Finished()
	wg.Wait()
	f.Finished()
	if got := o.e.path("a1").stats.udp.flows; got != 1 {
		t.Fatalf("finished %d flows", got)
	}
}

func TestUDPFlowFinishAfterForgetDoesNotRestorePath(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	old := o.StartUDPFlow(context.Background(), "a1", 53).(*udpFlow)
	oldPath := old.path
	old.Uplink(8, 1)
	if oldPath.udpFlowsActive != 1 {
		t.Fatalf("old path has %d active flows", oldPath.udpFlowsActive)
	}
	o.mu.Lock()
	o.e.forget("a1", t0)
	o.mu.Unlock()
	old.Finished()
	if oldPath.udpFlowsActive != 0 || o.e.paths["a1"] != nil {
		t.Fatalf("late finish restored forgotten path or retained %d flows", oldPath.udpFlowsActive)
	}
	retiring := o.StartUDPFlow(context.Background(), "a1", 443).(*udpFlow)
	retiringPath := retiring.path
	retiring.Uplink(5, 1)
	o.mu.Lock()
	o.e.forget("a1", t0)
	o.mu.Unlock()
	newFlow := o.StartUDPFlow(context.Background(), "a1", 19302).(*udpFlow)
	newPath := newFlow.path
	if newPath == retiringPath || newPath.udpFlowsActive != 1 {
		t.Fatal("readded tag did not get a fresh path")
	}
	newFlow.Uplink(12, 1)
	retiring.Finished()
	if retiringPath.udpFlowsActive != 0 || newPath.stats.udp.flows != 0 || newPath.udpFlowsActive != 1 {
		t.Fatal("old flow contaminated readded path")
	}
	newFlow.Finished()
	if newPath.stats.udp.flows != 1 || newPath.stats.udp.bytesUp != 12 || newPath.udpFlowsActive != 0 {
		t.Fatalf("new path counts %+v, active %d", newPath.stats.udp, newPath.udpFlowsActive)
	}
}

func TestUDPFlowKeepsStalePathUntilClose(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	f := o.StartUDPFlow(context.Background(), "a1", 53).(*udpFlow)
	p := f.path
	o.mu.Lock()
	o.e.gc(t0.Add(staleAfter), staleAfter)
	kept := o.e.paths["a1"] == p
	o.mu.Unlock()
	if !kept || p.udpFlowsActive != 1 {
		t.Fatal("GC forgot a path with an open UDP flow")
	}
	f.Finished()
	o.mu.Lock()
	o.e.gc(t0.Add(2*staleAfter), staleAfter)
	forgotten := o.e.paths["a1"] == nil
	o.mu.Unlock()
	if !forgotten || p.udpFlowsActive != 0 {
		t.Fatal("closed UDP flow kept a stale path")
	}
}

func TestUDPFlowFinishAfterProviderDropDoesNotRestorePath(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	f := o.StartUDPFlow(context.Background(), "a1", 53).(*udpFlow)
	p := f.path
	f.Uplink(9, 1)
	o.mu.Lock()
	o.e.dropProviderOutbound("a1", o.now())
	o.mu.Unlock()
	f.Finished()
	if o.e.paths["a1"] != nil || p.udpFlowsActive != 0 || o.StartUDPFlow(context.Background(), "a1", 53) != nil {
		t.Fatal("late provider flow restored its retired path")
	}
}

func TestQuietSampleOnlyMakesFinalReportDue(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	now := t0.Add(2 * time.Second)
	o.clock = func() time.Time { return now }
	o.e.report.begin(t0)
	o.e.report.quietSample = true
	if raw := o.TakeReport(false); raw != nil {
		t.Fatalf("quiet interval report: %s", raw)
	}
	snapshot := udpReport(t, o.PeekReport())
	if !snapshot.IsQuietSample || snapshot.Reason != reportSnapshot {
		t.Fatalf("quiet snapshot: %+v", snapshot)
	}
	final := udpReport(t, o.TakeReport(true))
	if !final.IsQuietSample || final.Reason != reportStop || o.TakeReport(true) != nil {
		t.Fatalf("quiet final: %+v", final)
	}
}

func TestReportDownStrengthFields(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	o.mu.Lock()
	st := &o.e.pathAt("a1", o.now()).stats
	st.downsStrong = 2
	st.downsWeak = 1
	o.e.report.decide()
	o.mu.Unlock()
	first := udpReport(t, o.TakeReport(true))
	if first.Paths["a1"].DownsStrong != 2 || first.Paths["a1"].DownsWeak != 1 {
		t.Fatalf("down strengths: %+v", first.Paths["a1"])
	}
}

func TestUDPReportSizeAt64Paths(t *testing.T) {
	o := newUDPObserver(t)
	defer o.Close()
	o.mu.Lock()
	o.e.report.decide()
	for i := 0; i < reportPaths; i++ {
		tag := fmt.Sprintf("path-%02d", i)
		st := &o.e.pathAt(tag, o.now()).stats
		st.udp = udpPathTotals{flows: 100, unanswered: 5, bytesUp: 1000000, bytesDown: 800000, writes: 1000, reads: 900}
		for j := range st.udp.ports {
			st.udp.ports[j] = udpPortTotals{flows: 20, unanswered: 1}
		}
	}
	o.mu.Unlock()
	raw := o.TakeReport(true)
	v := udpReport(t, raw)
	if len(v.Paths) != reportPaths {
		t.Fatalf("report has %d paths", len(v.Paths))
	}
	if len(raw) > 128<<10 {
		t.Fatalf("64-path report grew to %d bytes", len(raw))
	}
	var baseline map[string]any
	if err := json.Unmarshal(raw, &baseline); err != nil {
		t.Fatal(err)
	}
	delete(baseline, "is_quiet_sample")
	for _, value := range baseline["paths"].(map[string]any) {
		path := value.(map[string]any)
		for _, field := range []string{
			"downs_strong", "downs_weak", "udp_flows", "udp_flows_unanswered",
			"udp_bytes_up", "udp_bytes_down", "udp_writes", "udp_reads",
			"udp_first_reply_p50_ms", "udp_ports",
		} {
			delete(path, field)
		}
	}
	oldShape, err := json.Marshal(baseline)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= len(oldShape) {
		t.Fatalf("UDP fields did not grow the report: current %d, P2 shape %d", len(raw), len(oldShape))
	}
	t.Logf("64 paths: current %d bytes, P2 shape %d bytes, increase %d bytes", len(raw), len(oldShape), len(raw)-len(oldShape))
}

func BenchmarkUDPFlowUplink(b *testing.B) {
	o := newUDPObserver(b)
	defer o.Close()
	f := o.StartUDPFlow(context.Background(), "a1", 443).(*udpFlow)
	f.Uplink(1200, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Uplink(1200, 1)
	}
	b.StopTimer()
	f.Finished()
}
