package health

// Integrated fixture reconciled with accepted Stage 1 cf3b4a8d and the
// current quality spec. Root owns compilation and runtime checks.
// These are lifecycle/projection unit interactions, not UDP wire/socket proofs.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/proto"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

func qualityStage1Observer(t *testing.T, share float64) (*Observer, *fakeClock) {
	t.Helper()
	o, clk := newTestObserver()
	o.s = newSettings(&Config{
		Features: &FeatureConfig{Quality: proto.Bool(true), Report: proto.Bool(true), Events: proto.Bool(true), UdpReplies: proto.Bool(true)},
		Paths:    observed("a1", "a2"),
		Report:   &ReportConfig{Interval: int64(time.Second), QuietShare: share},
	})
	// Exercise Stage 1's actual once-per-run selection at the two deterministic
	// endpoints. The test never assigns report.quietSample or draws controller RNG.
	o.e = newEngine(o.s, 1)
	o.e.report.begin(clk.now())
	o.e.quality = newQualitySummary(o.e.report.started)
	o.qualityEpoch.Store(o.e.qualityEpoch)
	o.ctx = context.Background()
	o.done = make(chan struct{})
	o.runCtx, o.stopRun = context.WithCancel(o.ctx)
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		return tcpQualitySnapshot{rtt: 20 * time.Millisecond, present: qualityHasRTT}
	}
	// A failed nonblocking assertion may leave a tested worker holding o.mu.
	// This no-loop/no-provider fixture owns no native resources: cancellation
	// cleanup must not acquire that mutex. Explicit Close calls below still
	// exercise Stage 1's real UDP finalizer where it is part of the assertion.
	t.Cleanup(func() {
		o.once.Do(func() { close(o.done) })
		o.stopRun()
	})
	return o, clk
}

func qualityStage1UserEnd(o *Observer, clk *fakeClock, count int64) {
	tr, read, write := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 10*time.Millisecond)
	write.Add(100)
	clk.advance(20 * time.Millisecond)
	read.Add(count)
	tr.Finished(nil)
	tr.Finished(nil)
}

func qualityStage1Flow(t *testing.T, o *Observer, port net.Port) *udpFlow {
	t.Helper()
	flow, ok := o.StartUDPFlow(context.Background(), "a1", port).(*udpFlow)
	if !ok || flow == nil {
		t.Fatal("fixture did not admit its logical UDP association")
	}
	return flow
}

func qualityStage1Decode(t *testing.T, raw []byte) reportView {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("expected combined report")
	}
	var view reportView
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	return view
}

func qualityStage1JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func qualityStage1Snapshot(o *Observer, now time.Time) *reportView {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.e.snapshotReport(now)
}

func qualityStage1Decide(o *Observer) {
	o.mu.Lock()
	o.e.report.decide()
	o.mu.Unlock()
}

// Compare only the Stage 1 UDP projection. Legacy transport counters coexist
// with it and must not be interpreted as logical UDP payload counters.
func qualityStage1UDP(t *testing.T, got, want reportPath) {
	t.Helper()
	if got.UDPFlows != want.UDPFlows || got.UDPFlowsUnanswered != want.UDPFlowsUnanswered ||
		got.UDPBytesUp != want.UDPBytesUp || got.UDPBytesDown != want.UDPBytesDown ||
		got.UDPWrites != want.UDPWrites || got.UDPReads != want.UDPReads ||
		got.UDPFirstReplyP50Ms != want.UDPFirstReplyP50Ms || !reflect.DeepEqual(got.UDPPorts, want.UDPPorts) {
		t.Fatalf("UDP window projection: got %+v, want %+v", got, want)
	}
}

func qualityStage1ReportQuality(t *testing.T, view reportView, count int64, initial uint64) qualityPathView {
	t.Helper()
	if len(view.Quality) != 1 || view.Quality[0].Witness != nil || view.QualitySegmentsDropped != 0 {
		t.Fatal("combined report lost its sole quality segment or invented a witness/drop")
	}
	p := qualitySummaryPath(t, view.Quality[0], "a1")
	qualitySummaryInt(t, p.BytesDown, count)
	if p.TTFBCount != initial || p.DialCount != initial || p.TCPCount != initial {
		t.Fatalf("combined report timing phases: %+v", p)
	}
	return p
}

func qualityStage1Wait(t *testing.T, done <-chan struct{}, phase string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("fixture did not finish %s within its harness bound", phase)
	}
}

func qualityStage1CleanupJoin(t *testing.T, done <-chan struct{}, phase string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Keep trying every started worker even after one join times out. A
		// regression cannot send cleanup through Observer.Close's tested lock.
		t.Errorf("fixture worker did not finish %s during bounded cleanup", phase)
	}
}

func TestQualityStage1QuietGateIncludesUserQualityAndFinalUDP(t *testing.T) {
	for _, share := range []float64{0, 1} {
		t.Run(fmt.Sprintf("quiet_share_%v", share), func(t *testing.T) {
			o, clk := qualityStage1Observer(t, share)
			wantQuiet := share == 1
			if o.e.report.quietSample != wantQuiet {
				t.Fatal("constructor did not use configured quietShare")
			}
			qualityStage1UserEnd(o, clk, 512)
			answered := qualityStage1Flow(t, o, 53)
			answered.Uplink(12, 2)
			clk.advance(50 * time.Millisecond)
			answered.Downlink(7, 1)
			answered.Finished()
			answered.Finished()
			open := qualityStage1Flow(t, o, 19302)
			open.Uplink(8, 1)
			before := qualityStage1JSON(t, o.e.currentQuality(clk.now()).Paths)
			if o.e.report.decided || o.e.report.decisions != 0 || o.TakeReport(false) != nil {
				t.Fatal("traffic measurements made a routing decision or quiet interval due")
			}
			clk.advance(2 * time.Second)
			for range 2 {
				raw := o.PeekReport()
				if (raw != nil) != wantQuiet {
					t.Fatal("quietShare changed snapshot eligibility")
				}
				if wantQuiet {
					view := qualityStage1Decode(t, raw)
					if !view.IsQuietSample || view.Reason != reportSnapshot {
						t.Fatal("quiet snapshot lost its run bit or reason")
					}
					qualityStage1ReportQuality(t, view, 512, 1)
					qualityStage1UDP(t, view.Paths["a1"], reportPath{UDPFlows: 1, UDPBytesUp: 12, UDPBytesDown: 7,
						UDPWrites: 2, UDPReads: 1, UDPFirstReplyP50Ms: 50, UDPPorts: map[string]reportUDPPort{"dns": {Flows: 1}}})
				}
				if o.TakeReport(false) != nil || len(o.udpFlows) != 1 || open.closed ||
					o.e.path("a1").stats.udpFirstReply.total != 1 || !o.e.quality.open.nonempty() {
					t.Fatal("nil take or snapshot finalized UDP or consumed a measured window")
				}
			}
			if !bytes.Equal(before, qualityStage1JSON(t, o.e.currentQuality(clk.now()).Paths)) || o.e.report.quietSample != wantQuiet {
				t.Fatal("snapshot/nil take changed current history or resampled quiet selection")
			}
			finalRaw := o.TakeReport(true)
			if (finalRaw != nil) != wantQuiet {
				t.Fatal("quietShare changed final eligibility")
			}
			if wantQuiet {
				view := qualityStage1Decode(t, finalRaw)
				if !view.IsQuietSample || view.Reason != reportStop || view.Index != 1 {
					t.Fatal("quiet final lost run bit, stop reason or report index")
				}
				qualityStage1ReportQuality(t, view, 512, 1)
				qualityStage1UDP(t, view.Paths["a1"], reportPath{UDPFlows: 2, UDPFlowsUnanswered: 1,
					UDPBytesUp: 20, UDPBytesDown: 7, UDPWrites: 3, UDPReads: 1, UDPFirstReplyP50Ms: 50,
					UDPPorts: map[string]reportUDPPort{"dns": {Flows: 1}, "stun": {Flows: 1, Unanswered: 1}}})
			}
			stats := o.e.path("a1").stats.udp
			if stats.flows != 2 || stats.unanswered != 1 || stats.bytesUp != 20 || stats.bytesDown != 7 ||
				stats.writes != 3 || stats.reads != 1 || len(o.udpFlows) != 0 || !open.closed {
				t.Fatal("final did not close each logical association once, including an unsampled run")
			}
			open.Downlink(99, 1)
			open.Finished()
			_ = o.Close()
			_ = o.Close()
			if o.e.path("a1").stats.udp != stats || o.e.path("a1").udpFlowsActive != 0 ||
				o.PeekReport() != nil || o.TakeReport(false) != nil || o.TakeReport(true) != nil ||
				o.StartUDPFlow(context.Background(), "a1", 53) != nil || o.e.report.quietSample != wantQuiet {
				t.Fatal("late UDP I/O/finish/Close reopened or changed the terminal run")
			}
		})
	}
}

func TestQualityStage1WindowCutKeepsCurrentAndResetsUDPReplyHistogram(t *testing.T) {
	for _, share := range []float64{0, 1} {
		t.Run(fmt.Sprintf("quiet_share_%v", share), func(t *testing.T) {
			o, clk := qualityStage1Observer(t, share)
			wantQuiet := share == 1
			qualityStage1UserEnd(o, clk, 512)
			first := qualityStage1Flow(t, o, 53)
			first.Uplink(12, 2)
			clk.advance(50 * time.Millisecond)
			first.Downlink(7, 1)
			first.Finished()
			crossesCut := qualityStage1Flow(t, o, 443)
			crossesCut.Uplink(10, 1)
			currentBefore := qualityStage1JSON(t, o.e.currentQuality(clk.now()).Paths)
			qualityStage1Decide(o)
			clk.advance(2 * time.Second)
			snapshot := qualityStage1Snapshot(o, clk.now())
			if snapshot == nil || snapshot.IsQuietSample != wantQuiet {
				t.Fatal("eligible snapshot lost quiet selection")
			}
			frozen := qualityStage1JSON(t, snapshot)
			one := qualityStage1Decode(t, o.TakeReport(false))
			qualityStage1ReportQuality(t, one, 512, 1)
			qualityStage1UDP(t, one.Paths["a1"], reportPath{UDPFlows: 1, UDPBytesUp: 12, UDPBytesDown: 7,
				UDPWrites: 2, UDPReads: 1, UDPFirstReplyP50Ms: 50, UDPPorts: map[string]reportUDPPort{"dns": {Flows: 1}}})
			if one.Reason != reportInterval || one.IsQuietSample != wantQuiet || one.Index != 1 ||
				crossesCut.closed || len(o.udpFlows) != 1 || o.e.quality.open.nonempty() ||
				o.e.path("a1").stats.udpFirstReply.total != 0 ||
				!bytes.Equal(currentBefore, qualityStage1JSON(t, o.e.currentQuality(clk.now()).Paths)) || o.e.report.quietSample != wantQuiet {
				t.Fatal("interval cut finalized open UDP, retained a window histogram, or cleared/resampled current state")
			}
			empty := qualityStage1Decode(t, o.PeekReport())
			qualityStage1UDP(t, empty.Paths["a1"], reportPath{})
			if len(empty.Quality) != 0 || o.TakeReport(false) != nil || crossesCut.closed {
				t.Fatal("new window or nil take replayed quality/UDP or finalized the open association")
			}
			clk.advance(300 * time.Millisecond)
			crossesCut.Downlink(5, 1)
			crossesCut.Finished()
			qualityStage1UserEnd(o, clk, 256)
			qualityStage1Decide(o)
			clk.advance(2 * time.Second)
			two := qualityStage1Decode(t, o.TakeReport(false))
			qualityStage1ReportQuality(t, two, 256, 1)
			qualityStage1UDP(t, two.Paths["a1"], reportPath{UDPFlows: 1, UDPBytesUp: 10, UDPBytesDown: 5,
				UDPWrites: 1, UDPReads: 1, UDPFirstReplyP50Ms: 3000, UDPPorts: map[string]reportUDPPort{"quic": {Flows: 1}}})
			current := qualitySummaryPath(t, o.e.currentQuality(clk.now()), "a1")
			qualitySummaryInt(t, current.BytesDown, 768)
			if current.TTFBCount != 2 || two.Index != 2 || two.IsQuietSample != wantQuiet || o.e.report.quietSample != wantQuiet ||
				o.e.path("a1").stats.udpFirstReply.total != 0 || !bytes.Equal(frozen, qualityStage1JSON(t, snapshot)) {
				t.Fatal("second cut mixed reply histograms, reset current history/run selection, or mutated a snapshot")
			}
			last := qualityStage1Flow(t, o, 19302)
			last.Uplink(8, 1)
			final := qualityStage1Decode(t, o.TakeReport(true))
			qualityStage1UDP(t, final.Paths["a1"], reportPath{UDPFlows: 1, UDPFlowsUnanswered: 1, UDPBytesUp: 8,
				UDPWrites: 1, UDPPorts: map[string]reportUDPPort{"stun": {Flows: 1, Unanswered: 1}}})
			if len(final.Quality) != 0 || final.Index != 3 || final.IsQuietSample != wantQuiet || !last.closed ||
				o.e.report.quietSample != wantQuiet || o.PeekReport() != nil || o.TakeReport(true) != nil ||
				!bytes.Equal(frozen, qualityStage1JSON(t, snapshot)) {
				t.Fatal("final cut replayed old quality, lost the run bit, or reopened/mutated reports")
			}
		})
	}
}

// Three focused UDP/report boundaries, not the older path/witness sampler
// matrix. The injected sampler pauses scalar publication without holding a
// native socket Control callback, so Finish/Close are not socket-close joins.
func TestQualityStage1DeferredPublicationWithUDPReportBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		s2       bool
		boundary string
	}{
		{"s1_interval_udp_already_ended", false, "interval"},
		{"s2_final_closes_open_udp", true, "final"},
		{"s1_close_then_network_rejects", false, "close_network"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, clk := qualityStage1Observer(t, 1)
			flow := qualityStage1Flow(t, o, 53)
			flow.Uplink(12, 2)
			clk.advance(50 * time.Millisecond)
			flow.Downlink(7, 1)
			tr, read, write := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 10*time.Millisecond)
			write.Add(100)
			entered, release, sampled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			var calls atomic.Int32
			o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
				n := calls.Add(1)
				info := tcpQualitySnapshot{rtt: 20 * time.Millisecond, rxOutOfOrder: 1, rxTotal: 100,
					txRetransmit: 1, txTotal: 100, present: qualityHasRTT | qualityHasRX | qualityHasTX}
				if tc.s2 && n == 2 {
					info.loadRTT, info.rxOutOfOrder, info.rxTotal = 400*time.Millisecond, 11, 1100
					info.txRetransmit, info.txTotal = 10, 200
					info.present |= qualityHasLoadRTT
				}
				if (!tc.s2 && n == 1) || (tc.s2 && n == 2) {
					close(entered)
					<-release
				}
				return info
			}
			clk.advance(20 * time.Millisecond)
			counted := int64(512)
			chunk := counted
			if tc.s2 {
				read.Add(512)
				clk.advance(2 * time.Second)
				counted = 4 << 20
				chunk = counted - 512
			}
			go func() { read.Add(chunk); close(sampled) }()
			finished := make(chan struct{})
			boundaryDone := make(chan struct{})
			finishStarted := false
			boundaryStarted := false
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				qualityStage1CleanupJoin(t, sampled, "sampler cleanup")
				if finishStarted {
					qualityStage1CleanupJoin(t, finished, "Finished cleanup")
				}
				if boundaryStarted {
					qualityStage1CleanupJoin(t, boundaryDone, "UDP/report boundary cleanup")
				}
			})
			qualityStage1Wait(t, entered, "paused sampler entry")
			finishStarted = true
			go func() { tr.Finished(nil); tr.Finished(nil); close(finished) }()
			qualityStage1Wait(t, finished, "Finished while publication is blocked")
			if p := o.e.currentQuality(clk.now()).Paths["a1"]; p.BytesDown != nil ||
				tr.bytesRead.Load() != counted || len(o.trackers) != 0 || len(o.udpFlows) != 1 {
				t.Fatal("Finish consumed unpublished quality, lost its cutoff, or changed open logical UDP")
			}
			// A counted-read publication can straddle Finished. Challenge the
			// saved cutoff: deferred collection must not reload this live total.
			read.Add(128)
			if tr.bytesRead.Load() != counted+128 {
				t.Fatal("fixture did not advance the live TCP counter after its completion cutoff")
			}
			qualityStage1Decide(o)
			clk.advance(2 * time.Second)
			if tc.boundary == "interval" {
				flow.Finished()
			}
			old := qualityStage1Snapshot(o, clk.now())
			if old == nil {
				t.Fatal("boundary fixture has no eligible snapshot")
			}
			frozen := qualityStage1JSON(t, old)
			var boundaryReport reportView
			var boundaryRaw []byte
			var closeErr error
			boundaryStarted = true
			go func() {
				if tc.boundary == "close_network" {
					closeErr = o.Close()
				} else {
					boundaryRaw = o.TakeReport(tc.boundary == "final")
				}
				close(boundaryDone)
			}()
			qualityStage1Wait(t, boundaryDone, "UDP/report boundary while publication is blocked")
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			if tc.boundary == "close_network" {
				if !flow.closed || len(o.udpFlows) != 0 || o.e.path("a1").stats.udp.flows != 1 ||
					o.StartUDPFlow(context.Background(), "a1", 53) != nil {
					t.Fatal("Close did not independently finalize and close UDP admission")
				}
				o.NetworkChanged("wifi:after-close")
			} else {
				boundaryReport = qualityStage1Decode(t, boundaryRaw)
				qualityStage1UDP(t, boundaryReport.Paths["a1"], reportPath{UDPFlows: 1, UDPBytesUp: 12, UDPBytesDown: 7,
					UDPWrites: 2, UDPReads: 1, UDPFirstReplyP50Ms: 50, UDPPorts: map[string]reportUDPPort{"dns": {Flows: 1}}})
				for _, segment := range boundaryReport.Quality {
					if segment.Paths["a1"].BytesDown != nil {
						t.Fatal("boundary report consumed deferred quality end")
					}
				}
				if !flow.closed || len(o.udpFlows) != 0 {
					t.Fatal("completed/finalized UDP association remained open")
				}
			}
			releaseOnce.Do(func() { close(release) })
			qualityStage1Wait(t, sampled, "deferred quality publication")
			flow.Downlink(99, 1)
			flow.Finished()
			if o.e.path("a1").stats.udp.flows != 1 || o.e.path("a1").stats.udp.bytesDown != 7 ||
				o.e.path("a1").udpFlowsActive != 0 || !bytes.Equal(frozen, qualityStage1JSON(t, old)) {
				t.Fatal("deferred quality or late UDP completion changed old evidence/flow totals")
			}
			current := o.e.currentQuality(clk.now())
			if tc.boundary == "close_network" {
				if len(current.Paths) != 0 || current.Witness != nil || current.SamplesDropped != 0 ||
					o.e.qualityEpoch != 1 || o.qualityEpoch.Load() != 1 {
					t.Fatal("old deferred quality entered the new network after Close")
				}
				final := qualityStage1Decode(t, o.TakeReport(true))
				qualityStage1UDP(t, final.Paths["a1"], reportPath{UDPFlows: 1, UDPBytesUp: 12, UDPBytesDown: 7,
					UDPWrites: 2, UDPReads: 1, UDPFirstReplyP50Ms: 50, UDPPorts: map[string]reportUDPPort{"dns": {Flows: 1}}})
				if len(final.Quality) != 0 || o.PeekReport() != nil || o.TakeReport(true) != nil {
					t.Fatal("rejected deferred quality reopened or entered a final report")
				}
				return
			}
			p := qualitySummaryPath(t, current, "a1")
			qualitySummaryInt(t, p.BytesDown, counted)
			if p.TTFBCount != 1 || p.DialCount != 1 || p.TCPCount != 1 || tr.quality.socket.Load() != nil {
				t.Fatal("deferred quality lost/duplicated timing or retained the finished socket")
			}
			if tc.s2 && (p.BulkCount != 1 || p.BulkRTTP50Ms == nil || *p.BulkRTTP50Ms != 400 ||
				p.RXOutOfOrderFraction == nil || *p.RXOutOfOrderFraction != 0.01 ||
				p.TXRetransmitFraction == nil || *p.TXRetransmitFraction != 0.05) {
				t.Fatal("final UDP boundary lost the admitted S2/speed/fractions")
			}
			if tc.s2 {
				// The admitted mark was 2s after the 512-byte first reply. The
				// later report/release delay and post-Finished bytes add no speed.
				qualitySummaryInt(t, p.BulkKbpsP50, 20000)
				qualitySummaryInt(t, p.BulkKbpsP90, 20000)
				qualitySummaryInt(t, p.BulkKbpsMax, 16775)
			}
			if tc.boundary == "final" {
				if boundaryReport.Reason != reportStop || o.PeekReport() != nil || o.TakeReport(false) != nil || o.TakeReport(true) != nil {
					t.Fatal("post-final quality publication reopened its terminal report")
				}
			} else {
				later := qualityStage1Decode(t, o.PeekReport())
				qualityStage1ReportQuality(t, later, counted, 1)
				qualityStage1UDP(t, later.Paths["a1"], reportPath{})
				if later.Index != boundaryReport.Index+1 || o.e.path("a1").stats.udpFirstReply.total != 0 {
					t.Fatal("deferred quality did not use the available new window or replayed UDP histogram")
				}
			}
		})
	}
}

func TestQualityStage1IgnoreRetainsRawProbeAndRejectsOldQuality(t *testing.T) {
	o, clk := qualityStage1Observer(t, 1)
	te := newTestEngine(t, &Config{Report: &ReportConfig{QuietShare: 1, Interval: int64(time.Second)}},
		&SelectorConfig{Paths: pathSpecs("a1", 1), NetworkChangeAction: NetworkChangeMode_IGNORE, StartOutbound: "a1"}, "a1")
	o.s, o.e = te.s, te.engine
	o.e.report.begin(clk.now())
	o.e.quality = newQualitySummary(o.e.report.started)
	p := te.path("a1")
	p.setStatus(statusDegraded, "optional", clk.now())
	p.lastProbeEnd, p.lastRTT = clk.now(), 17*time.Millisecond
	p.lastProbeResult = probeResult{ok: true, profile: "home", reason: reasonWake.String(), state: statusDegraded, steps: []probeStepResult{
		{name: "transport", ok: true, connect: time.Millisecond, elapsed: 2 * time.Millisecond, bytes: 3, destination: 0},
		{name: "optional", connect: time.Millisecond, elapsed: 3 * time.Millisecond, bytes: 4, destination: -1, class: "short_body"},
	}}
	old, read, write := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 10*time.Millisecond)
	write.Add(100)
	clk.advance(20 * time.Millisecond)
	read.Add(512)
	rawResult := p.lastProbeResult.snapshot()
	probeEnd := p.lastProbeEnd
	beforeProbe := p.statusProbe(clk.now())
	beforeGeneration, beforeChoice := o.e.generation, te.sel.current()
	snapshot := qualityStage1Snapshot(o, clk.now())
	if snapshot == nil || beforeProbe == nil || qualitySummaryPath(t, o.e.currentQuality(clk.now()), "a1").TTFBCount != 1 {
		t.Fatal("IGNORE fixture lacks old raw probe/quality evidence")
	}
	frozen := qualityStage1JSON(t, snapshot)
	const nextNetwork = "cell:25001:next" // Opaque host id, including its apparent prefix.
	o.NetworkChanged(nextNetwork)
	if o.e.generation != beforeGeneration || te.sel.current() != beforeChoice || p.status != statusDegraded ||
		!reflect.DeepEqual(p.lastProbeResult, rawResult) || !p.lastProbeEnd.Equal(probeEnd) ||
		o.e.qualityEpoch != 1 || o.qualityEpoch.Load() != 1 || len(o.e.currentQuality(clk.now()).Paths) != 0 {
		t.Fatal("IGNORE mixed raw probe/controller lifetime with measurement epoch")
	}
	ev := te.lastOf(t, "network_change")
	if ev.Fields["network"] != nextNetwork || ev.Fields["is_known"] != false || ev.Fields["is_ignored"] != true {
		t.Fatal("opaque network change lost Stage 1 event lifetime/state")
	}
	for _, absent := range []string{"network_type", "network_operator"} {
		if _, present := ev.Fields[absent]; present {
			t.Fatalf("quality added %s to Stage 1 network event", absent)
		}
	}
	read.Add(128)
	old.Finished(nil)
	if got := o.e.currentQuality(clk.now()); len(got.Paths) != 0 || got.SamplesDropped != 0 {
		t.Fatal("stale same-generation connection contaminated new quality or counted a drop")
	}
	qualityStage1UserEnd(o, clk, 256)
	status := o.e.status(clk.now())
	var row *pathStatusView
	for i := range status.Paths {
		if status.Paths[i].Outbound == "a1" {
			row = &status.Paths[i]
		}
	}
	if row == nil || row.Probe == nil || row.Quality == nil || status.NetworkID != nextNetwork ||
		row.Probe.Profile != beforeProbe.Profile || row.Probe.Reason != beforeProbe.Reason || row.Probe.Reason != reasonWake.String() || row.Probe.State != beforeProbe.State ||
		row.Probe.AgoMs != clk.now().Sub(probeEnd).Milliseconds() || !reflect.DeepEqual(row.Probe.Steps, beforeProbe.Steps) {
		t.Fatal("current Status lost retained raw probe alongside new network quality")
	}
	statusBody := qualityStage1JSON(t, status)
	for _, absent := range []string{"network_type", "network_operator"} {
		if bytes.Contains(statusBody, []byte(`"`+absent+`":`)) || bytes.Contains(frozen, []byte(`"`+absent+`":`)) {
			t.Fatalf("Status/report quality interpreted opaque network id as %s", absent)
		}
	}
	if o.e.currentQuality(clk.now()).IsNetworkKnown {
		t.Fatal("first-seen opaque network became known through prefix interpretation")
	}
	qualitySummaryInt(t, row.Quality.BytesDown, 256)
	if row.Quality.TTFBCount != 1 || row.Quality.DialCount != 1 || row.Quality.TCPCount != 1 ||
		status.WitnessQuality != nil || !reflect.DeepEqual(p.lastProbeResult, rawResult) ||
		!bytes.Equal(frozen, qualityStage1JSON(t, snapshot)) {
		t.Fatal("new quality reused old samples, changed raw probe, or mutated an old report snapshot")
	}
	providerProbe := describeOutbound(p, "a1", 0, clk.now()).Probe
	if providerProbe == nil || providerProbe.Profile == nil || *providerProbe.Profile != "home" || providerProbe.Reason != reasonWake.String() || providerProbe.State != "degraded" ||
		!providerProbe.IsPassed || providerProbe.Steps == nil || len(*providerProbe.Steps) != 2 ||
		(*providerProbe.Steps)[0].Destination == nil || *(*providerProbe.Steps)[0].Destination != 0 ||
		(*providerProbe.Steps)[1].Destination != nil || (*providerProbe.Steps)[1].Error != "short_body" {
		t.Fatal("IGNORE retained raw probe lost provider pointer/presence semantics")
	}
}
