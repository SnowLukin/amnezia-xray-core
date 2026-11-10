package health

// Integrated source fixture; root owns compilation and runtime checks.
// Q 9e2c01da2eeea6477d7946ab571f6415e2fee4e4 reconciled with accepted
// S cf3b4a8dac00c0c8154c5af2b79907cbc8a89948 and the current quality spec.
// Probe results below are controlled completed-job inputs, not HTTP traffic.

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

// withoutHistograms is the view as Status gives it: the report keeps the
// histograms, Status, which hosts read often, does not.
func withoutHistograms(v qualityPathView) qualityPathView {
	rv := reflect.ValueOf(&v).Elem()
	for i := range rv.NumField() {
		if strings.HasSuffix(rv.Type().Field(i).Name, "Hist") {
			rv.Field(i).SetZero()
		}
	}
	return v
}

func qualityStage1ExportFixture(t *testing.T) (*testEngine, *Observer, *fakeClock) {
	t.Helper()
	paths := []*PathDefinition{{Tag: "p-x", Cost: 1, Probe: "light"}, {Tag: "p-y", Cost: 2, Probe: "light"}}
	sc := &SelectorConfig{Paths: paths, SkipProbeBeforeSwitch: true}
	te := newProviderEngine(t, quietProvider(nil), sc, "p-x", "p-y")
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x", "p-y"}, paths: paths}, t0)
	o, clk := newTestObserver()
	o.s, o.e = te.s, te.engine
	o.e.report.begin(clk.now())
	o.s.reportInterval = time.Second
	o.e.quality = newQualitySummary(clk.now())
	o.qualityEpoch.Store(o.e.qualityEpoch)
	o.generation.Store(o.e.generation)
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		return tcpQualitySnapshot{present: qualityHasRTT | qualityHasMinRTT}
	}
	return te, o, clk
}

// StartDial and the actual read/finish hooks collect user quality. The fixture
// uses a zero-duration reply to preserve legacy zero TTFB while new quality
// still has a measured first-reply sample. No socket, syscall or traffic runs.
func qualityStage1ExportUser(t *testing.T, o *Observer, tag string, n int64, finish bool) (*connTracker, *trackerReadCounter) {
	t.Helper()
	tr, ok := o.StartDial(context.Background(), tag, net.Destination{}, nil).(*connTracker)
	if !ok || tr == nil || tr.quality == nil {
		t.Fatal("user fixture did not create a measured TCP tracker")
	}
	r, _ := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
	read := r.(*trackerReadCounter)
	read.Add(n)
	if finish {
		tr.Finished(nil)
	}
	return tr, read
}

func qualityStage1ExportBeginProbe(t *testing.T, te *testEngine, tag string, now time.Time) *probeJob {
	t.Helper()
	j := te.enqueue(tag, reasonHost, "auto", now)
	if j == nil {
		t.Fatal("host fixture did not enqueue its probe")
	}
	te.begin(j, now)
	te.dropQueued(tag, reasonHost)
	return j
}

// Exercise the combined probe-context classification and counter hooks before
// delivering a rich result through onProbeResult's normal snapshot capture.
// withProbe has a zero-start probeState; recordDial must not create wall-clock
// timers. No probe bytes may become user/witness quality or legacy user bytes.
func qualityStage1ExportProbeBytes(t *testing.T, o *Observer, tag string) int64 {
	t.Helper()
	before := o.e.quality.currentView(o.now())
	var count atomic.Int64
	tr := o.StartDial(withProbe(context.Background(), &count), tag, net.Destination{}, nil)
	if _, ok := tr.(*probeTracker); !ok {
		t.Fatalf("probe context created %T instead of probeTracker", tr)
	}
	r, w := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
	r.Add(2 << 20)
	w.Add(17)
	tr.Finished(nil)
	if count.Load() != (2<<20)+17 || !reflect.DeepEqual(before, o.e.quality.currentView(o.now())) {
		t.Fatal("probe tracker changed user quality or lost its separate byte counter")
	}
	return count.Load()
}

func qualityStage1ExportProvider(t *testing.T, te *testEngine, o *Observer, now time.Time) (providerRequest, []byte) {
	t.Helper()
	r := te.providerRequest(te.prov(), &refreshJob{refresh: 1, trigger: triggerHost}, now)
	body := mustRequestBody(t, o, &providerRuntime{cfg: te.prov().cfg}, &refreshJob{request: r})
	var captured providerRequest
	if err := json.Unmarshal(body, &captured); err != nil {
		t.Fatal(err)
	}
	return captured, body
}

func qualityStage1ExportStatusPath(t *testing.T, v statusView, tag string) pathStatusView {
	t.Helper()
	for _, row := range v.Paths {
		if row.Outbound == tag {
			return row
		}
	}
	t.Fatalf("Status omitted %q", tag)
	return pathStatusView{}
}

func qualityStage1ExportRequestPath(t *testing.T, v providerRequest, short string) requestOutbound {
	t.Helper()
	for _, row := range v.Outbounds {
		if row.Tag == short {
			return row
		}
	}
	t.Fatalf("provider omitted exact own short tag %q", short)
	return requestOutbound{}
}

func TestQualityStage1ExportCompletedProbeAndUserQualityCoexist(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result probeResult
	}{
		{"builtin_destination_zero", probeResult{ok: true, state: statusUp, rtt: 180 * time.Millisecond, steps: []probeStepResult{
			{name: "default", ok: true, connect: 12 * time.Millisecond, elapsed: 34 * time.Millisecond, bytes: 56, destination: 0},
		}}},
		{"optional_degraded", probeResult{ok: true, state: statusDegraded, profile: "full", rtt: 180 * time.Millisecond, steps: []probeStepResult{
			{name: "transport", ok: true, connect: 12 * time.Millisecond, elapsed: 34 * time.Millisecond, bytes: 56, destination: 0},
			{name: "optional", connect: 23 * time.Millisecond, elapsed: 45 * time.Millisecond, bytes: 67, destination: -1, class: "server_unverified"},
		}}},
		{"no_attempted_steps", probeResult{state: statusDown, class: "timeout", err: "https://QUALITY_STAGE1_PRIVATE_DESTINATION.invalid/raw-error"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te, o, clk := qualityStage1ExportFixture(t)
			qualityStage1ExportUser(t, o, "p-x", 512, true)
			initial := qualitySummaryPath(t, te.quality.currentView(clk.now()), "p-x")
			result := tc.result
			j := qualityStage1ExportBeginProbe(t, te, "p-x", clk.now())
			result.bytes = qualityStage1ExportProbeBytes(t, o, "p-x")
			// A witness-tagged probe has the same exclusion. It does not mean
			// a configured witness was observed, so all exports omit witness.
			qualityStage1ExportProbeBytes(t, o, "direct")
			te.onProbeResult(j, result, clk.now())
			// A separate observed path receives only a completed probe. Its
			// raw snapshot must be exportable without inventing user quality.
			probeOnly := qualityStage1ExportBeginProbe(t, te, "p-y", clk.now())
			probeOnlyBytes := qualityStage1ExportProbeBytes(t, o, "p-y")
			te.onProbeResult(probeOnly, probeResult{ok: true, state: statusUp, bytes: probeOnlyBytes,
				steps: []probeStepResult{{name: "default", ok: true, destination: 0}},
			}, clk.now())
			clk.advance(2 * time.Second)
			status := te.status(clk.now())
			row := qualityStage1ExportStatusPath(t, status, "p-x")
			only := qualityStage1ExportStatusPath(t, status, "p-y")
			if row.Probe == nil || row.Probe.Profile != result.profile || row.Probe.Reason != reasonHost.String() || row.Probe.State != result.pathState().String() || row.Probe.AgoMs != 2000 ||
				len(row.Probe.Steps) != len(result.steps) || row.Quality == nil || initial.TTFBMsHist == nil || !reflect.DeepEqual(*row.Quality, withoutHistograms(initial)) ||
				row.TTFBP50Ms != 0 || row.TTFBP90Ms != 0 || row.BytesDown != 512 || row.ConnsOK != 1 || row.Probes != 1 || row.ProbeBytes != result.bytes {
				t.Fatalf("completed-job Status conflated raw probe and user traffic: %+v", row)
			}
			if initial.TTFBCount != 1 || initial.DialCount != 1 || initial.TCPCount != 1 || initial.BulkCount != 0 ||
				initial.BytesDown == nil || *initial.BytesDown != 512 || status.WitnessQuality != nil || only.Quality != nil ||
				only.Probe == nil || only.Probe.Reason != reasonHost.String() || only.Probes != 1 || only.ProbeBytes != probeOnlyBytes || only.BytesDown != 0 || only.ConnsOK != 0 {
				t.Fatal("fixture lost real user samples or invented quality for probe/unmeasured traffic")
			}
			request, body := qualityStage1ExportProvider(t, te, o, clk.now())
			out := qualityStage1ExportRequestPath(t, request, "x")
			onlyOut := qualityStage1ExportRequestPath(t, request, "y")
			if out.Probe == nil || out.Probe.Profile == nil || *out.Probe.Profile != result.profile || out.Probe.Reason != reasonHost.String() || out.Probe.State != result.pathState().String() ||
				out.Probe.Steps == nil || !reflect.DeepEqual(*out.Probe.Steps, result.requestSteps()) || out.Probe.IsPassed != (result.pathState() != statusDown) ||
				out.Probe.AgoMs != 2000 || !reflect.DeepEqual(out.Quality, requestQualityOf(initial)) || out.TTFBP50Ms != 0 ||
				out.BytesDown != 512 || request.WitnessQuality != nil || onlyOut.Quality != nil || onlyOut.Probe == nil ||
				onlyOut.Probe.Profile == nil || *onlyOut.Probe.Profile != "" || onlyOut.Probe.Reason != reasonHost.String() || onlyOut.Probe.Steps == nil || len(*onlyOut.Probe.Steps) != 1 || onlyOut.BytesDown != 0 {
				t.Fatalf("completed-job provider export lost raw presence or conflated user quality: %+v", out)
			}
			if bytes.Contains(body, []byte("QUALITY_STAGE1_PRIVATE_DESTINATION")) || bytes.Contains(body, []byte("connect_ms")) {
				t.Fatal("provider exported private raw error text or snake_case step keys")
			}
			for i, step := range row.Probe.Steps {
				want := result.steps[i]
				if step.Name != want.name || step.IsPassed != want.ok || step.ConnectMs != want.connect.Milliseconds() || step.Ms != want.elapsed.Milliseconds() || step.Bytes != want.bytes {
					t.Fatal("Status lost an attempted raw step")
				}
				if want.ok {
					if step.Destination == nil || *step.Destination != want.destination || step.Error != "" {
						t.Fatal("successful destination zero became absent")
					}
				} else if step.Destination != nil || step.Error != want.class {
					t.Fatal("optional failure became a destination or lost its safe class")
				}
			}
			// An existing decision permits a report. Probe/quality capture itself
			// is not used to invent report eligibility in this fixture.
			te.report.decide()
			report := te.snapshotReport(clk.now())
			if report == nil || len(report.Quality) != 1 || report.Quality[0].Witness != nil ||
				!reflect.DeepEqual(qualitySummaryPath(t, report.Quality[0], "p-x"), initial) {
				t.Fatal("report omitted measured quality beside completed raw probe counts")
			}
			wantSteps := []reportProbeStep{}
			for _, step := range result.steps {
				count := reportProbeStep{Name: step.name}
				if step.ok {
					count.Passed = 1
				} else {
					count.Failed = 1
				}
				wantSteps = append(wantSteps, count)
			}
			legacy := report.Paths["p-x"]
			if legacy.Probes != 1 || legacy.ProbeBytes != result.bytes || legacy.BytesDown != 512 || legacy.TTFBP50Ms != 0 ||
				!reflect.DeepEqual(legacy.ProbeSteps, wantSteps) {
				t.Fatalf("report lost separate probe_steps/legacy/user-quality accounting: %+v", legacy)
			}
			if onlyReport := report.Paths["p-y"]; onlyReport.Probes != 1 || onlyReport.ProbeBytes != probeOnlyBytes || onlyReport.BytesDown != 0 ||
				!reflect.DeepEqual(onlyReport.ProbeSteps, []reportProbeStep{{Name: "default", Passed: 1}}) || len(report.Quality[0].Paths) != 1 {
				t.Fatal("probe-only path acquired report user quality or lost raw step accounting")
			}
		})
	}
}

func TestQualityStage1ExportProfileChangeClearsProbeOnly(t *testing.T) {
	te, o, clk := qualityStage1ExportFixture(t)
	qualityStage1ExportUser(t, o, "p-x", 512, true)
	j := qualityStage1ExportBeginProbe(t, te, "p-x", clk.now())
	te.onProbeResult(j, probeResult{ok: true, state: statusDegraded, profile: "light", steps: []probeStepResult{
		{name: "transport", ok: true, destination: 0}, {name: "optional", class: "timeout", destination: -1},
	}}, clk.now())
	p := te.path("p-x")
	qualityBefore := qualitySummaryPath(t, te.quality.currentView(clk.now()), "p-x")
	uses, sleeper, epoch := p.uses, p.sleeper, te.qualityEpoch
	clk.advance(time.Second)
	old := qualityStage1ExportBeginProbe(t, te, "p-x", clk.now())
	// begin records another use; profile-only replacement preserves this
	// actual handler lifecycle and clears only old probe-derived evidence.
	uses = p.uses
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x", "p-y"}, paths: []*PathDefinition{
		{Tag: "p-x", Cost: 1, Probe: "full"}, {Tag: "p-y", Cost: 2, Probe: "light"},
	}}, clk.now())
	if !old.invalidated || p.status != statusUnknown || p.reason != "probe_changed" || !p.lastProbeEnd.IsZero() ||
		p.statusProbe(clk.now()) != nil || p.uses != uses || p.sleeper != sleeper || te.qualityEpoch != epoch ||
		!reflect.DeepEqual(qualityBefore, qualitySummaryPath(t, te.quality.currentView(clk.now()), "p-x")) {
		t.Fatal("profile change retained obsolete raw evidence or destroyed user measurement/handler history")
	}
	clk.advance(time.Second)
	te.onProbeResult(old, probeResult{ok: true, state: statusDegraded, profile: "light", steps: []probeStepResult{
		{name: "old-optional", class: "timeout", destination: -1},
	}}, clk.now())
	status := qualityStage1ExportStatusPath(t, te.status(clk.now()), "p-x")
	request, _ := qualityStage1ExportProvider(t, te, o, clk.now())
	out := qualityStage1ExportRequestPath(t, request, "x")
	if status.Probe != nil || out.Probe != nil || status.State != "unknown" || status.Quality == nil ||
		!reflect.DeepEqual(*status.Quality, withoutHistograms(qualityBefore)) || !reflect.DeepEqual(out.Quality, requestQualityOf(qualityBefore)) {
		t.Fatal("stale old-profile result repopulated raw export or changed measured user quality")
	}
	clk.advance(time.Second)
	fresh := qualityStage1ExportBeginProbe(t, te, "p-x", clk.now())
	te.onProbeResult(fresh, probeResult{ok: true, state: statusDegraded, profile: "full", bytes: qualityStage1ExportProbeBytes(t, o, "p-x"), steps: []probeStepResult{
		{name: "transport", ok: true, destination: 0}, {name: "new-optional", class: "server_unverified", destination: -1},
	}}, clk.now())
	status = qualityStage1ExportStatusPath(t, te.status(clk.now()), "p-x")
	if status.Probe == nil || status.Probe.Profile != "full" || status.Probe.Reason != reasonHost.String() || status.Probe.State != "degraded" || status.Quality == nil ||
		!reflect.DeepEqual(*status.Quality, withoutHistograms(qualityBefore)) {
		t.Fatal("fresh new-profile result lost its raw snapshot beside retained user measurement history")
	}
}
