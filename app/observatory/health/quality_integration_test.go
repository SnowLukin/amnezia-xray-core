package health

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
)

// Use the established fake-clock/socket helpers without opening real sockets.
// The real constructor has its own test below: existing tracker-only helpers
// deliberately do not install a summary and must remain usable unchanged.
func qualityIntegrationObserver(skip bool) (*Observer, *fakeClock) {
	o, clk := qualityWitnessObserver(skip)
	o.e.report.begin(clk.now())
	if !skip {
		o.e.quality = newQualitySummary(o.e.report.started)
	}
	o.qualityEpoch.Store(o.e.qualityEpoch)
	return o, clk
}

func TestQualityIntegrationActualConstructor(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip_%v", skip), func(t *testing.T) {
			o := newObserver(context.Background(), &Config{Features: &FeatureConfig{Quality: proto.Bool(!skip), Report: proto.Bool(true)}, Paths: observed("a1"), Passive: &PassiveConfig{ShouldSkipQuality: skip}})
			t.Cleanup(func() { _ = o.Close() })
			if skip {
				if o.e.quality != nil {
					t.Fatal("shouldSkipQuality installed summary state")
				}
				return
			}
			if o.e.quality == nil || !o.e.quality.current.started.Equal(o.e.report.started) ||
				o.e.quality.current.network.known || o.e.quality.current.paths != nil || o.e.quality.open.paths != nil {
				t.Fatal("constructor omitted summary, changed its start, or eagerly allocated traffic state")
			}
			if o.qualityEpoch.Load() != o.e.qualityEpoch || o.e.qualityEpoch != o.e.quality.epoch {
				t.Fatal("constructor epochs disagree")
			}
		})
	}
}

func TestQualityIntegrationIgnoreChangesEpochWithoutRoutingReset(t *testing.T) {
	o, clk := qualityIntegrationObserver(false)
	te := newTestEngine(t, nil, &SelectorConfig{NetworkChangeAction: NetworkChangeMode_IGNORE, StartOutbound: "a1"}, "a1", "a2")
	o.s, o.e = te.s, te.engine
	o.e.report.begin(clk.now())
	o.e.quality = newQualitySummary(o.e.report.started)
	p := o.e.pathAt("a1", clk.now())
	p.setStatus(statusUp, "traffic", clk.now())
	p.quarantineLevel = 3
	p.quarantineUntil = clk.now().Add(time.Minute)
	job := &probeJob{tag: "a2", generation: o.e.generation}
	o.e.queue = append(o.e.queue, job)
	beforeGeneration, beforeChoice := o.e.generation, te.sel.current()
	tr, read, _ := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, time.Millisecond)
	clk.advance(time.Millisecond)
	read.Add(100)
	o.NetworkChanged("wifi:next")
	if o.e.generation != beforeGeneration || te.sel.current() != beforeChoice || p.status != statusUp ||
		p.quarantineLevel != 3 || len(o.e.queue) != 1 || o.e.queue[0] != job {
		t.Fatal("measurement epoch changed the IGNORE controller policy")
	}
	if o.qualityEpoch.Load() != 1 || o.e.qualityEpoch != 1 || o.e.quality.epoch != 1 {
		t.Fatal("IGNORE did not advance/mirror the independent quality epoch")
	}
	tr.Finished(nil)
	current := o.e.quality.currentView(clk.now())
	views, dropped := o.e.quality.report(clk.now(), false)
	if len(current.Paths) != 0 || current.SamplesDropped != 0 || len(views) != 1 || dropped != 0 {
		t.Fatal("old connection completion entered the new network or became an admission drop")
	}
	old := qualitySummaryPath(t, views[0], "a1")
	if old.TTFBCount != 1 || old.BytesDown != nil {
		t.Fatal("already accepted initial sample was lost or stale end was added to its closed segment")
	}
	tr, read, _ = qualityOpenPath(o, clk, qualityTestConn{}, time.Millisecond)
	clk.advance(time.Millisecond)
	read.Add(512)
	tr.Finished(nil)
	qualitySummaryInt(t, qualitySummaryPath(t, o.e.quality.currentView(clk.now()), "a1").BytesDown, 512)
	// The same network again is no change; an empty id after it is.
	o.NetworkChanged("wifi:next")
	o.NetworkChanged("")
	if o.e.generation != beforeGeneration || o.e.qualityEpoch != 2 || o.qualityEpoch.Load() != 2 {
		t.Fatal("repeat/empty changes failed to preserve IGNORE routing and advance quality once")
	}
}

func TestQualityIntegrationUnbuiltReportDoesNotCutWindow(t *testing.T) {
	o, clk := qualityIntegrationObserver(false)
	tr, read, _ := qualityOpenPath(o, clk, qualityTestConn{}, time.Millisecond)
	clk.advance(20 * time.Millisecond)
	read.Add(512)
	clk.advance(2 * time.Second)
	if o.e.takeReport(false, clk.now()) != nil || o.e.snapshotReport(clk.now()) != nil {
		t.Fatal("ordinary report unexpectedly became eligible without a decision")
	}
	if o.e.quality.open.paths["a1"].ttfb.total != 1 {
		t.Fatal("outer report rejection reset the quality window")
	}
	tr.Finished(nil)
}

func TestQualityIntegrationReportCutSplitsPhasesAndKeepsCurrent(t *testing.T) {
	o, clk := qualityIntegrationObserver(false)
	o.s.reportInterval = time.Second
	tr, read, _ := qualityOpenPath(o, clk, qualityTestConn{}, time.Millisecond)
	clk.advance(20 * time.Millisecond)
	read.Add(1024)
	clk.advance(2 * time.Second)
	o.e.report.decide() // Exercise the real due-only builder, independently of controller policy.
	snapshot := o.e.snapshotReport(clk.now())
	if snapshot == nil || len(snapshot.Quality) != 1 {
		t.Fatal("eligible snapshot omitted its internal quality")
	}
	initial := qualitySummaryPath(t, snapshot.Quality[0], "a1")
	if initial.TTFBCount != 1 || initial.BytesDown != nil {
		t.Fatal("snapshot did not contain only the already accepted initial phase")
	}
	taken := o.e.takeReport(false, clk.now())
	if taken == nil || len(taken.Quality) != 1 || o.e.quality.open.nonempty() {
		t.Fatal("successful outer report did not take only its quality window")
	}
	if qualitySummaryPath(t, o.e.quality.currentView(clk.now()), "a1").TTFBCount != 1 {
		t.Fatal("report cut reset current-network initial history")
	}
	clk.advance(2 * time.Second)
	read.Add(1 << 20)
	tr.Finished(nil)
	tr.Finished(nil)
	endSnapshot := o.e.snapshotReport(clk.now())
	if endSnapshot == nil || len(endSnapshot.Quality) != 1 {
		t.Fatal("completed connection did not enter the next report window")
	}
	end := qualitySummaryPath(t, endSnapshot.Quality[0], "a1")
	qualitySummaryInt(t, end.BytesDown, (1<<20)+1024)
	if end.TTFBCount != 0 || end.DialCount != 0 || end.BulkCount != 1 {
		t.Fatal("end repeated initial timing or duplicated the completed connection")
	}
	current := qualitySummaryPath(t, o.e.quality.currentView(clk.now()), "a1")
	if current.TTFBCount != 1 || current.BulkCount != 1 {
		t.Fatal("current-network summary failed to join phases across windows")
	}
	qualitySummaryInt(t, current.BytesDown, (1<<20)+1024)
	if initial.BytesDown != nil || qualitySummaryPath(t, taken.Quality[0], "a1").BulkCount != 0 {
		t.Fatal("later completion mutated earlier report snapshots")
	}
}

func TestQualityIntegrationUnansweredDialAndEmptySocketlessEnd(t *testing.T) {
	for _, socket := range []bool{false, true} {
		t.Run(fmt.Sprintf("socket_%v", socket), func(t *testing.T) {
			o, clk := qualityIntegrationObserver(false)
			conn := qualityTestConn{}
			if socket {
				conn.raw = &qualityTestRawConn{}
			}
			tr, _, _ := qualityOpenPath(o, clk, conn, 50*time.Millisecond)
			tr.Finished(context.Canceled)
			view := o.e.quality.currentView(clk.now())
			if !socket {
				if len(view.Paths) != 0 {
					t.Fatal("empty socketless connection invented measurements")
				}
				return
			}
			p := qualitySummaryPath(t, view, "a1")
			qualitySummaryInt(t, p.BytesDown, 0)
			if p.DialCount != 1 || p.TTFBCount != 0 || p.TCPCount != 0 {
				t.Fatal("unanswered raw-socket dial lost its known zero end or invented TTFB/S1")
			}
			if tr.quality.socket.Load() != nil {
				t.Fatal("completed tracker retained its socket after collection")
			}
		})
	}
}

func TestQualityIntegrationUDPProbeAndSkipHaveNoSummarySamples(t *testing.T) {
	for _, kind := range []string{"udp_path", "udp_witness", "probe_path", "probe_witness", "skip_path", "skip_witness"} {
		t.Run(kind, func(t *testing.T) {
			skip := kind == "skip_path" || kind == "skip_witness"
			o, clk := qualityIntegrationObserver(skip)
			// Keep a sink even for skip to prove the nil tracker guard, rather
			// than passing merely because the entire summary was disabled.
			if o.e.quality == nil {
				o.e.quality = newQualitySummary(clk.now())
			}
			ctx := context.Background()
			if kind == "udp_path" || kind == "udp_witness" {
				ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: net.UDPDestination(net.LocalHostIP, 53)}})
			}
			var probeBytes atomic.Int64
			if kind == "probe_path" || kind == "probe_witness" {
				ctx = withProbe(ctx, &probeBytes)
			}
			tag := "a1"
			if kind == "udp_witness" || kind == "probe_witness" || kind == "skip_witness" {
				tag = "direct"
			}
			tr := o.StartDial(ctx, tag, net.Destination{}, nil)
			read, _ := tr.Dialed(nil, nil, nil, nil)
			read.Add(2 << 20)
			tr.Finished(nil)
			view := o.e.quality.currentView(clk.now())
			if len(view.Paths) != 0 || view.Witness != nil || view.SamplesDropped != 0 {
				t.Fatal("unsupported flow produced a measured quality record/drop")
			}
		})
	}
}

func TestQualityIntegrationWitnessPublicationAndFullIdempotentEnd(t *testing.T) {
	o, clk := qualityIntegrationObserver(false)
	entered, release, firstDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	var calls atomic.Int32
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return tcpQualitySnapshot{rtt: 20 * time.Millisecond, minRTT: 10 * time.Millisecond, present: qualityHasRTT | qualityHasMinRTT}
	}
	w := o.StartDial(context.Background(), "direct", net.Destination{}, nil).(*witnessTracker)
	clk.advance(20 * time.Millisecond)
	read, write := w.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
	write.Add(100)
	clk.advance(5 * time.Millisecond)
	go func() { read.Add(512); close(firstDone) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("winner did not enter S1 sampler")
	}
	read.Add(64) // Loser sees firstClaim, while the winner has not published S1.
	o.mu.Lock()
	premature := o.e.quality.currentView(clk.now()).Witness
	o.mu.Unlock()
	if premature != nil || w.quality.initial.Load() {
		t.Fatal("loser consumed a dial-only initial sample before S1 publication")
	}
	unblock.Do(func() { close(release) })
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("winner failed to publish/collect initial sample")
	}
	initial := o.e.quality.currentView(clk.now()).Witness
	if initial == nil || initial.TTFBCount != 1 || initial.DialCount != 1 || initial.TCPCount != 1 {
		t.Fatal("winning witness read failed to collect its complete initial sample once")
	}
	clk.advance(2 * time.Second)
	read.Add(2 << 20)
	read.Add(4096) // Legacy witness success counter has stopped by now.
	w.Finished(nil)
	w.Finished(nil)
	want := int64(512 + 64 + (2 << 20) + 4096)
	end := o.e.quality.currentView(clk.now()).Witness
	if end == nil || end.TTFBCount != 1 || end.DialCount != 1 || end.BulkCount != 1 {
		t.Fatal("witness end duplicated/omitted timing or bulk sample")
	}
	qualitySummaryInt(t, end.BytesDown, want)
	if w.bytes.Load() >= want || w.quality.socket.Load() != nil {
		t.Fatal("full end reused the truncated legacy bytes or retained its socket")
	}
}

func TestQualityIntegrationCollectorChurnIsBoundedAndPolicyFree(t *testing.T) {
	o, clk := qualityIntegrationObserver(false)
	beforePaths, beforeEvents, beforeQueue := len(o.e.paths), o.e.report.counts.Events, len(o.e.queue)
	for i := 0; i < 1000; i++ {
		q := o.newConnectionQuality(false, 0)
		q.hasDial.Store(true)
		var bytesDown atomic.Int64
		bytesDown.Store(4096)
		o.mu.Lock()
		o.collectQualityEnd(q, fmt.Sprintf("rotating-%04d", i), false, &bytesDown)
		o.mu.Unlock()
	}
	for _, c := range []*qualityCollection{&o.e.quality.current, &o.e.quality.open} {
		if len(c.paths) != qualityCollectionPaths || c.tagBytes > qualityCollectionTagBytes ||
			c.samplesDropped != 2*(1000-qualityCollectionPaths) {
			t.Fatal("initial/end hook admission was unbounded or dropped phases were miscounted")
		}
	}
	if len(o.e.paths) != beforePaths || o.e.report.counts.Events != beforeEvents || len(o.e.queue) != beforeQueue {
		t.Fatal("measurement collector created controller paths/events/probes")
	}
	// This is a payload bound, not a measured heap assertion. Accounting for
	// at most 130 mutable accumulators and five 16KiB retained-key budgets
	// prevents a future struct expansion from silently invalidating the plan.
	// The peak and saturated speed histograms raised it from 256 KiB.
	payload := 2*(qualityCollectionPaths+1)*unsafe.Sizeof(qualityAccumulator{}) + 5*qualityCollectionTagBytes
	if payload > 320*1024 {
		t.Fatalf("bounded summary payload grew to %d bytes before maps/scalar views", payload)
	}
	if len(o.e.quality.currentView(clk.now()).Paths) != qualityCollectionPaths {
		t.Fatal("bounded sink failed to retain every admitted tag for projection")
	}
}
