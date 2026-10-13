package health

// Integrated source fixture; root owns compilation and runtime checks.
// Reconciled with accepted Stage 1 cf3b4a8d and the current quality spec.
// Paired traces target the same combined Stage 1 controller on both
// sides. They preserve S's legitimate audit/lone/wake/degraded behavior.
// Controlled fake-clock inputs are not runtime failover/CPU measurements.

import (
	"bytes"
	"context"
	"google.golang.org/protobuf/proto"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

type qualityStage1ExportPolicyPath struct {
	state             pathStatus
	reason, rescuedBy string
	since, quarantine time.Time
	level             int
	stats             pathStats
}

type qualityStage1ExportControllerTrace struct {
	events          []byte
	choices         []string
	choiceTimes     []time.Time
	jobs            []probeJob
	jobTimes, wakes []time.Time
	downs           []downMark
	paths           map[string]qualityStage1ExportPolicyPath
	counts          reportCounts
	queue           []probeJob
	running         int
	rngNext         int64
}

func qualityStage1ExportControllerFixture(t *testing.T, skip bool, concurrent int, switchWithoutProbe bool, threePaths bool) (*testEngine, *Observer, *fakeClock) {
	t.Helper()
	paths := pathSpecs("a", 1, "b", 3)
	candidates := []string{"a1", "b1"}
	if threePaths {
		paths = pathSpecs("a", 1, "b", 3, "c", 5)
		candidates = append(candidates, "c1")
	}
	cfg := &Config{Features: &FeatureConfig{Quality: proto.Bool(!skip), Report: proto.Bool(true), Events: proto.Bool(true)}, Passive: &PassiveConfig{ShouldSkipQuality: skip}, Schedule: &ScheduleConfig{
		MaxConcurrentProbes: int32(concurrent), Quarantine: &Backoff{InitialDelay: int64(time.Minute)}, IdleAfter: int64(time.Hour),
	}}
	sc := &SelectorConfig{Paths: paths, SkipProbeBeforeSwitch: switchWithoutProbe,
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(time.Hour), SkipRandomPhase: true}}
	te := newTestEngine(t, cfg, sc, candidates...)
	o, clk := newTestObserver()
	o.s, o.e = te.s, te.engine
	o.e.report.begin(clk.now())
	if !skip {
		o.e.quality = newQualitySummary(clk.now())
	}
	o.qualityEpoch.Store(o.e.qualityEpoch)
	o.generation.Store(o.e.generation)
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		return tcpQualitySnapshot{rtt: 2 * time.Second, minRTT: 10 * time.Millisecond, loadRTT: 7 * time.Second,
			rxOutOfOrder: 90, rxTotal: 100, txRetransmit: 900, txTotal: 1000,
			present: qualityHasRTT | qualityHasMinRTT | qualityHasLoadRTT | qualityHasRX | qualityHasTX}
	}
	// Same observed user traffic on both sides; only collection differs.
	// Slow download and adverse TCP_INFO cannot trigger controller policy.
	tr := o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker)
	r, w := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
	w.Add(100)
	r.Add(1024)
	clk.advance(30 * time.Second)
	r.Add((1 << 20) - 1024)
	tr.Finished(nil)
	if te.path("a1").status != statusUp || te.sel.current() != "a1" {
		t.Fatal("adverse measurement-looking traffic changed initial policy")
	}
	if skip {
		if tr.quality != nil || te.quality != nil {
			t.Fatal("skipQuality retained measurement allocation")
		}
	} else {
		q := qualitySummaryPath(t, te.quality.currentView(clk.now()), "a1")
		if q.TTFBCount != 1 || q.DialCount != 1 || q.TCPCount != 1 || q.BulkCount != 1 ||
			q.BulkKbpsMax == nil || *q.BulkKbpsMax >= 1000 || q.BulkRTTP50Ms == nil || *q.BulkRTTP50Ms != 8000 ||
			q.TXRetransmitFraction == nil || *q.TXRetransmitFraction != .9 {
			t.Fatal("enabled side did not actually collect adverse quality")
		}
	}
	return te, o, clk
}

func (trace *qualityStage1ExportControllerTrace) checkpoint(te *testEngine, now time.Time) {
	trace.choices = append(trace.choices, te.sel.current())
	trace.choiceTimes = append(trace.choiceTimes, now)
}

func (trace *qualityStage1ExportControllerTrace) tick(te *testEngine, clk *fakeClock, when time.Duration) []*probeJob {
	clk.t = at(when)
	jobs, wake := te.tick(clk.now())
	trace.wakes = append(trace.wakes, wake)
	for _, j := range jobs {
		trace.jobs = append(trace.jobs, *j)
		trace.jobTimes = append(trace.jobTimes, clk.now())
	}
	trace.checkpoint(te, clk.now())
	return jobs
}

func qualityStage1ExportControllerFinish(te *testEngine, trace qualityStage1ExportControllerTrace, now time.Time) qualityStage1ExportControllerTrace {
	trace.checkpoint(te, now)
	trace.downs = append(trace.downs, te.takeDowns()...)
	trace.paths = make(map[string]qualityStage1ExportPolicyPath, len(te.paths))
	for tag, p := range te.paths {
		trace.paths[tag] = qualityStage1ExportPolicyPath{state: p.status, reason: p.reason, rescuedBy: p.rescuedBy,
			since: p.statusSince, quarantine: p.quarantineUntil, level: p.quarantineLevel, stats: p.stats}
	}
	trace.counts = te.report.counts
	for _, j := range te.queue {
		trace.queue = append(trace.queue, *j)
	}
	trace.running = len(te.running)
	trace.events, _ = te.events.drain()
	// Exposes accidental quality RNG consumption using an identical seed;
	// it occurs only after the final decision, never controls the scenario.
	trace.rngNext = te.rnd.Int63()
	return trace
}

func qualityStage1ExportAssertControllerPair(t *testing.T, off, on qualityStage1ExportControllerTrace) {
	t.Helper()
	if !bytes.Equal(off.events, on.events) || !reflect.DeepEqual(off, on) {
		t.Fatalf("quality changed Stage 1 choices/times, probe priority/counts/budgets, down/rescue/quarantine, counters or RNG:\noff=%+v\non=%+v", off, on)
	}
}

func qualityStage1ExportAuditTrace(t *testing.T, skip bool, state pathStatus) qualityStage1ExportControllerTrace {
	t.Helper()
	te, _, clk := qualityStage1ExportControllerFixture(t, skip, 3, false, false)
	other := te.addSelector("other", newSelectorSettings(&SelectorConfig{Paths: pathSpecs("c1", 5)}))
	te.updateCandidates(other, []string{"c1"}, t0)
	te.serverNames(map[string]string{"a1": "a", "b1": "b", "c1": "c"})
	clk.t = at(3 * time.Minute)
	te.failN(3, "a1", failDial, clk.now())
	te.enqueue("c1", reasonRecheck, "", clk.now())
	trace := qualityStage1ExportControllerTrace{}
	trace.checkpoint(te, clk.now())
	jobs := trace.tick(te, clk, 3*time.Minute+time.Second)
	if te.path("a1").status != statusDown || len(jobs) != 3 || jobs[0].reason != reasonFailover || jobs[1].reason != reasonRecheck || jobs[2].reason != reasonAudit {
		t.Fatalf("S audit fixture lost strong down or failover/recheck/audit priority: %+v", jobs)
	}
	until := te.path("a1").quarantineUntil
	clk.advance(time.Second)
	te.onProbeResult(jobs[0], probeResult{ok: true}, clk.now())
	te.onProbeResult(jobs[1], probeResult{ok: true}, clk.now())
	te.onProbeResult(jobs[2], probeResult{state: state}, clk.now())
	p := te.path("a1")
	if te.sel.current() != "b1" || te.report.counts.AuditCount != 1 || p.stats.audits != 1 || p.status != state {
		t.Fatal("S audit completion lost its decision/counter behavior")
	}
	if state == statusDown {
		if p.quarantineLevel != 1 || !p.quarantineUntil.Equal(until) || te.report.counts.AuditPassed != 0 {
			t.Fatal("failed audit changed penalty")
		}
	} else if p.quarantineLevel != 0 || !p.quarantineUntil.IsZero() || p.rescuedBy != "audit" || te.report.counts.AuditPassed != 1 {
		t.Fatal("passed/degraded audit did not preserve rescue behavior")
	}
	return qualityStage1ExportControllerFinish(te, trace, clk.now())
}

func TestQualityStage1ExportAuditPriorityAndRescueAreInvariant(t *testing.T) {
	for _, state := range []pathStatus{statusUp, statusDown, statusDegraded} {
		t.Run(state.String(), func(t *testing.T) {
			qualityStage1ExportAssertControllerPair(t, qualityStage1ExportAuditTrace(t, true, state), qualityStage1ExportAuditTrace(t, false, state))
		})
	}
}

func qualityStage1ExportLoneWakeTrace(t *testing.T, skip, wake bool, state pathStatus) qualityStage1ExportControllerTrace {
	t.Helper()
	te, _, clk := qualityStage1ExportControllerFixture(t, skip, 1, true, false)
	clk.t = at(3 * time.Minute)
	wantReason := reasonLone
	if wake {
		wantReason = reasonWake
		te.userPresent(clk.now())
	} else {
		te.fail("a1", failDial, "", clk.now())
	}
	trace := qualityStage1ExportControllerTrace{}
	jobs := trace.tick(te, clk, 3*time.Minute)
	if len(jobs) != 1 || jobs[0].reason != wantReason || jobs[0].tag != "a1" || te.path("a1").status != statusUp {
		t.Fatalf("S lone/wake did not wait for exactly one proof: %+v", jobs)
	}
	clk.advance(time.Second)
	te.onProbeResult(jobs[0], probeResult{state: state}, clk.now())
	trace.checkpoint(te, clk.now())
	p := te.path("a1")
	wantChoice := "b1"
	if state == statusUp {
		wantChoice = "a1"
	}
	if p.status != state || te.sel.current() != wantChoice || te.queued("a1", reasonAudit) {
		t.Fatal("S lone/wake result lost its state/failover behavior or incorrectly scheduled audit")
	}
	if state == statusDown {
		trace.downs = append(trace.downs, te.takeDowns()...)
		if len(trace.downs) != 2 || len(te.silences) != 1 || trace.downs[0].tag != "a1" || trace.downs[1].tag != "a1" ||
			!trace.downs[0].pending || !trace.downs[0].watch || !trace.downs[1].pending || trace.downs[1].watch ||
			!trace.downs[0].at.Equal(clk.now()) || !trace.downs[1].at.Equal(clk.now()) {
			t.Fatal("lone/wake down lost pending close/watch timing")
		}
		clk.advance(te.s.passive.stallTimeout)
		te.dueSilences(clk.now())
		silent := te.takeDowns()
		if len(silent) != 1 || silent[0].tag != "a1" || !silent[0].silent || !silent[0].at.Equal(trace.downs[0].at) || len(te.silences) != 0 {
			t.Fatal("lone/wake down did not schedule exactly one later silent close")
		}
		trace.downs = append(trace.downs, silent...)
	}
	failed := int64(0)
	if state == statusDown {
		failed = 1
	}
	if wake {
		if te.report.counts.WakeProbeCount != 1 || te.report.counts.WakeProbeFailed != failed || p.stats.downsStrong != 0 || p.stats.downsWeak != 0 {
			t.Fatal("wake became passive evidence")
		}
	} else if te.report.counts.LoneProbeCount != 1 || te.report.counts.LoneProbeFailed != failed || p.stats.downsStrong != failed {
		t.Fatal("lone proof lost exactly-once strong evidence")
	}
	// Both S mechanisms reject duplicate completion; quality does not make
	// an obsolete probe look like a new result or create another failover.
	beforeState, beforeChoice, beforeLevel, beforeUntil := p.status, te.sel.current(), p.quarantineLevel, p.quarantineUntil
	beforeStats, beforeCounts := p.stats, te.report.counts
	beforeDowns, beforeSilences, beforeEvents := len(te.downs), len(te.silences), len(te.events.events)
	te.onProbeResult(jobs[0], probeResult{state: statusDown}, clk.now())
	if p.status != beforeState || te.sel.current() != beforeChoice || p.quarantineLevel != beforeLevel || !p.quarantineUntil.Equal(beforeUntil) ||
		!reflect.DeepEqual(p.stats, beforeStats) || !reflect.DeepEqual(te.report.counts, beforeCounts) ||
		len(te.downs) != beforeDowns || len(te.silences) != beforeSilences || len(te.events.events) != beforeEvents {
		t.Fatal("duplicate lone/wake completion changed policy, counters, close work or events")
	}
	return qualityStage1ExportControllerFinish(te, trace, clk.now())
}

func TestQualityStage1ExportLoneWakeDecisionsAndFailoverTimeAreInvariant(t *testing.T) {
	for _, wake := range []bool{false, true} {
		for _, state := range []pathStatus{statusUp, statusDown, statusDegraded} {
			name := "lone_" + state.String()
			if wake {
				name = "wake_" + state.String()
			}
			t.Run(name, func(t *testing.T) {
				qualityStage1ExportAssertControllerPair(t, qualityStage1ExportLoneWakeTrace(t, true, wake, state), qualityStage1ExportLoneWakeTrace(t, false, wake, state))
			})
		}
	}
}

func qualityStage1ExportDegradedTrace(t *testing.T, skip, allDegraded bool) qualityStage1ExportControllerTrace {
	t.Helper()
	te, _, clk := qualityStage1ExportControllerFixture(t, skip, 1, false, true)
	suspectWeak(te, "a1", 3*time.Minute)
	trace := qualityStage1ExportControllerTrace{}
	jobs := trace.tick(te, clk, 3*time.Minute+3*time.Second)
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatal("weak evidence did not start normal confirmation")
	}
	te.onProbeResult(jobs[0], degradedProbe(), clk.now())
	trace.checkpoint(te, clk.now())
	if te.path("a1").status != statusDegraded || te.sel.current() != "a1" || !te.path("a1").quarantineUntil.IsZero() {
		t.Fatal("degraded confirmation acted as down")
	}
	for i, tag := range []string{"b1", "c1"} {
		jobs = trace.tick(te, clk, 3*time.Minute+time.Duration(6+3*i)*time.Second)
		if len(jobs) != 1 || jobs[0].tag != tag || jobs[0].reason != reasonFailover {
			t.Fatal("degraded search lost candidate order or count")
		}
		r := degradedProbe()
		if i == 1 && !allDegraded {
			r = probeResult{ok: true}
		}
		te.onProbeResult(jobs[0], r, clk.now())
		trace.checkpoint(te, clk.now())
		if i == 0 && te.sel.current() != "a1" {
			t.Fatal("degraded candidate hid an untried up candidate")
		}
	}
	want := "c1"
	if allDegraded {
		want = "a1"
	}
	if te.sel.current() != want || te.sel.exhausted || len(te.downs) != 0 || te.path("a1").quarantineLevel != 0 {
		t.Fatal("degraded search abandoned its normal usable-path choice")
	}
	return qualityStage1ExportControllerFinish(te, trace, clk.now())
}

func TestQualityStage1ExportDegradedSearchAndUsableChoiceAreInvariant(t *testing.T) {
	for _, allDegraded := range []bool{false, true} {
		name := "up_candidate"
		if allDegraded {
			name = "all_degraded"
		}
		t.Run(name, func(t *testing.T) {
			qualityStage1ExportAssertControllerPair(t, qualityStage1ExportDegradedTrace(t, true, allDegraded), qualityStage1ExportDegradedTrace(t, false, allDegraded))
		})
	}
}
