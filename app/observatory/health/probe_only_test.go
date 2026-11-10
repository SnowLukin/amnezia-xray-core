package health

import (
	"bufio"
	"context"
	"io"
	gonet "net"
	"net/http"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func probeOnlyTestEngine(t *testing.T, keep int, paths ...*PathDefinition) *testEngine {
	t.Helper()
	var tags []string
	for _, p := range paths {
		tags = append(tags, p.Tag)
	}
	te := newTestEngine(t, &Config{Schedule: tune(&ScheduleConfig{
		MaxConcurrentProbes: 4, ProbesPerMinute: 100, ProbesPerServerNamePerMinute: 100,
		ProbeOnly: &ProbeOnlyConfig{KeepVerified: int32(keep), RecheckInterval: int64(15 * time.Minute)}}, scheduleKnobs{spacing: time.Second, spacingJitter: -1, idleAfter: time.Hour})}, &SelectorConfig{Paths: paths}, tags...)
	te.providers = append(te.providers, newProvider(&providerSettings{tag: "source", balancer: "auto", minHealthy: 2}))
	return te
}

func TestProbeOnlyIsExcludedFromEverySelectionEntry(t *testing.T) {
	te := probeOnlyTestEngine(t, 1,
		&PathDefinition{Tag: "probe", Cost: 1, IsProbeOnly: true},
		&PathDefinition{Tag: "real", Cost: 2})
	if te.sel.current() != "real" || !reflect.DeepEqual(te.sel.candidates, []string{"real"}) ||
		!reflect.DeepEqual(te.sel.inventory, []string{"probe", "real"}) {
		t.Fatalf("selection includes a probe-only address: active=%q candidates=%v inventory=%v", te.sel.current(), te.sel.candidates, te.sel.inventory)
	}
	if te.pin("auto", "probe", t0) || te.healthy(te.sel) != 1 {
		t.Fatal("probe-only address was pinned or counted healthy")
	}
	te.networkID = "network"
	te.networks["network"] = map[string]string{"auto": "probe"}
	if te.rememberedFor(te.sel) != "" {
		t.Fatal("network memory offered a probe-only address")
	}
	te.switchTo(te.sel, "probe", "return", t0)
	te.sel.started = false
	te.sel.cfg.startOutbound = "probe"
	te.sel.cfg.startPinned = "probe"
	te.updateCandidates(te.sel, []string{"probe", "real"}, t0)
	if te.sel.current() != "real" || te.sel.pinned != "" {
		t.Fatal("start or return selected a probe-only address")
	}
	te.path("real").setStatus(statusDown, "test", t0)
	te.path("real").quarantineUntil = at(time.Hour)
	te.failover(te.sel, t0)
	if te.sel.current() != "" || !te.sel.exhausted {
		t.Fatalf("exhaustion fell back to probe credentials: %q", te.sel.current())
	}
}

func TestProbeOnlyCandidatesFollowPriceAndDeclarationOneAtATime(t *testing.T) {
	te := probeOnlyTestEngine(t, 4,
		&PathDefinition{Tag: "real", Cost: 1},
		&PathDefinition{Tag: "z", Cost: 2, IsProbeOnly: true},
		&PathDefinition{Tag: "a", Cost: 2, IsProbeOnly: true},
		&PathDefinition{Tag: "expensive", Cost: 3, IsProbeOnly: true})
	te.updateCandidates(te.sel, []string{"a", "expensive", "real", "z"}, t0)
	if te.probeOnlyRefreshReady(te.sel, t0) {
		t.Fatal("refresh preceded candidate results")
	}
	for i, want := range []string{"z", "a", "expensive"} {
		now := at(time.Duration(i*2) * time.Second)
		jobs, _ := te.tick(now)
		if len(jobs) != 1 || jobs[0].tag != want || jobs[0].reason != reasonCandidate {
			t.Fatalf("candidate %d: jobs=%+v, want %s", i, jobs, want)
		}
		if more, _ := te.tick(now); len(more) != 0 {
			t.Fatalf("concurrent candidate started: %+v", more)
		}
		te.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp}, now)
		if !te.probeOnlyRefreshReady(te.sel, now) {
			t.Fatal("first pass did not permit the min_healthy request")
		}
	}
	if te.probeOnlyVerified(te.sel, at(5*time.Second)) != 3 || te.healthy(te.sel) != 1 || te.sel.current() != "real" {
		t.Fatal("candidate results changed real health or selection")
	}
}

func TestProbeOnlyAllFailedStopsUntilNetworkEpochRearms(t *testing.T) {
	te := probeOnlyTestEngine(t, 1,
		&PathDefinition{Tag: "real", Cost: 1},
		&PathDefinition{Tag: "candidate", Cost: 2, IsProbeOnly: true})
	jobs, _ := te.tick(t0)
	te.onProbeResult(jobs[0], probeResult{state: statusDown, class: "timeout"}, t0)
	if !te.probeOnlyRefreshReady(te.sel, t0) {
		t.Fatal("all failures did not permit the refresh")
	}
	if jobs, _ := te.tick(at(time.Minute)); len(jobs) != 0 {
		t.Fatalf("failed candidates were retried endlessly: %+v", jobs)
	}
	te.noteDemand(at(2 * time.Minute))
	te.onNetworkChange("new", at(2*time.Minute))
	jobs, _ = te.tick(at(3 * time.Minute))
	var candidateJobs int
	for _, j := range jobs {
		if j.reason == reasonCandidate {
			candidateJobs++
		}
	}
	if candidateJobs != 1 {
		t.Fatalf("new network epoch did not retry failed inventory exactly once: %+v", jobs)
	}
}

func TestProbeOnlyVerifiedRechecksDespiteRealRecovery(t *testing.T) {
	te := probeOnlyTestEngine(t, 1,
		&PathDefinition{Tag: "real", Cost: 1},
		&PathDefinition{Tag: "candidate", Cost: 2, IsProbeOnly: true},
		&PathDefinition{Tag: "unused", Cost: 3, IsProbeOnly: true})
	jobs, _ := te.tick(t0)
	te.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp}, t0)
	if jobs, _ := te.tick(at(15*time.Minute - time.Nanosecond)); len(jobs) != 0 {
		t.Fatalf("verified candidate rechecked early: %+v", jobs)
	}
	jobs, _ = te.tick(at(15 * time.Minute))
	if len(jobs) != 1 || jobs[0].tag != "candidate" {
		t.Fatalf("recheck deadline missed: %+v", jobs)
	}
	te.providers[0].cfg.minHealthy = 1
	te.tick(at(15*time.Minute + time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp}, at(15*time.Minute+2*time.Second))
	if jobs[0].invalidated || te.probeOnlyVerified(te.sel, at(15*time.Minute+2*time.Second)) != 1 {
		t.Fatal("real recovery invalidated independent candidate proof")
	}
	if jobs, _ := te.tick(at(16 * time.Minute)); len(jobs) != 0 {
		t.Fatalf("fresh verified candidate was probed again: %+v", jobs)
	}
}

func TestProbeOnlyNetworkEpochInvalidatesEvenIgnoredNetwork(t *testing.T) {
	te := probeOnlyTestEngine(t, 1,
		&PathDefinition{Tag: "real", Cost: 1},
		&PathDefinition{Tag: "candidate", Cost: 2, IsProbeOnly: true})
	te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
	jobs, _ := te.tick(t0)
	generation := te.generation
	te.onNetworkChange("new", at(time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp}, at(2*time.Second))
	if te.generation != generation || te.probeOnlyVerified(te.sel, at(2*time.Second)) != 0 ||
		!te.path("candidate").lastProbeEnd.IsZero() {
		t.Fatal("ignored real network change preserved candidate proof")
	}
}

func TestProbeOnlyDoesNotAlterGroupRingOrFailureSeries(t *testing.T) {
	paths := []*PathDefinition{{Tag: "real1", Group: "g"}, {Tag: "real2", Group: "g"}, {Tag: "probe", Group: "g", IsProbeOnly: true}}
	te := newTestEngine(t, nil, &SelectorConfig{Paths: paths, Groups: []*PathGroup{{Name: "g", Cost: 1, FailedInRowToLeave: 2}}}, "real1", "real2", "probe")
	g := te.sel.groups["g"]
	position := g.position
	g.failed, g.lastFailed = 1, "real1"
	te.updateCandidates(te.sel, []string{"probe", "real2", "real1"}, t0)
	if !reflect.DeepEqual(g.members, []string{"real1", "real2"}) || g.position != position || g.failed != 1 {
		t.Fatalf("probe-only address changed ring: %+v", g)
	}
	te.providers = append(te.providers, newProvider(&providerSettings{balancer: "auto", minHealthy: 3}))
	jobs, _ := te.tick(t0)
	te.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp}, t0)
	if g.failed != 1 || g.lastFailed != "real1" {
		t.Fatal("candidate pass cleared real group failures")
	}
	allProbe := newTestEngine(t, nil, &SelectorConfig{Paths: []*PathDefinition{{Tag: "probe", Group: "g", IsProbeOnly: true}}, Groups: []*PathGroup{{Name: "g", Cost: 1}}}, "probe")
	if len(allProbe.sel.groups) != 0 || allProbe.sel.current() != "" {
		t.Fatal("probe-only group created a selectable ring")
	}
}

func TestCandidatePriorityAndSharedBudgets(t *testing.T) {
	order := []probeReason{reasonConnectivity, reasonConfirm, reasonFailover, reasonRecheck, reasonCandidate, reasonAudit, reasonReturnToCheaper, reasonHost}
	if !slices.IsSortedFunc(order, func(a, b probeReason) int { return int(a.priority() - b.priority()) }) {
		t.Fatal("candidate priority broke urgent probes or returns")
	}
	te := probeOnlyTestEngine(t, 1, &PathDefinition{Tag: "real", Cost: 1}, &PathDefinition{Tag: "candidate", Cost: 2, IsProbeOnly: true})
	te.s.schedule.perMinute = 1
	te.starts = []time.Time{t0}
	jobs, wake := te.tick(at(time.Second))
	if len(jobs) != 0 || !wake.Equal(at(time.Minute)) {
		t.Fatalf("candidate bypassed shared minute budget: jobs=%v wake=%v", jobs, wake)
	}
	te.starts = nil
	te.path("candidate").serverName = ""
	te.serverNameFor = func(string) string { return "same" }
	other := te.enqueue("real", reasonConfirm, "auto", at(time.Second))
	te.begin(other, at(time.Second))
	if jobs, _ := te.tick(at(2 * time.Second)); len(jobs) != 0 {
		t.Fatal("candidate ran alongside the same server name")
	}
}

func TestProbeOnlyProfileExecutesMandatoryBodyAndCapturesMode(t *testing.T) {
	cases := []struct {
		name     string
		only     bool
		minBytes int64
		state    pathStatus
		class    string
		hits     []string
	}{
		{name: "candidate", only: true, minBytes: 4, state: statusUp, hits: []string{"/mandatory"}},
		{name: "real", minBytes: 4, state: statusDegraded, hits: []string{"/mandatory", "/optional"}},
		{name: "candidate_short_body", only: true, minBytes: 5, state: statusDown, class: "short_body", hits: []string{"/mandatory"}},
		{name: "real_short_body", minBytes: 5, state: statusDown, class: "short_body", hits: []string{"/mandatory"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hits []string
			d := &probePhaseDispatcher{reportDial: true, serve: func(c gonet.Conn) {
				req, err := http.ReadRequest(bufio.NewReader(c))
				if err != nil {
					return
				}
				req.Body.Close()
				hits = append(hits, req.URL.Path)
				if req.URL.Path == "/optional" {
					io.WriteString(c, "HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n")
					return
				}
				io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndata")
			}}
			o := probePhaseObserver(time.Second, d)
			defer o.stopRun()
			mandatory := stepSettings("body", false, "http://example.test/mandatory")
			mandatory.minBytes = tc.minBytes
			o.s.probes = append(o.s.probes, probeSettings{name: "full", steps: []probeStepSettings{mandatory, stepSettings("optional", true, "http://example.test/optional")}})
			o.s.paths.Store("a1", pathSettings{probe: "full", isProbeOnly: tc.only})
			j := &probeJob{tag: "a1", reason: reasonCandidate}
			o.e.begin(j, t0)
			o.s.paths.Store("a1", pathSettings{probe: "full", isProbeOnly: !tc.only})
			r := o.probe(j.tag, j.profile)
			if r.ok != (tc.state != statusDown) || r.state != tc.state || r.class != tc.class ||
				j.isProbeOnly != tc.only || !reflect.DeepEqual(hits, tc.hits) || len(r.steps) != len(tc.hits) {
				t.Fatalf("profile/mode/body mismatch: %+v mode=%v hits=%v", r, j.isProbeOnly, hits)
			}
			if tc.state == statusDown && (r.err != "short body 4" || r.steps[0].ok) {
				t.Fatalf("mandatory body limit was not enforced: %+v", r)
			}
		})
	}
}

func TestProbeOnlyDispatchGuardRejectsUserContexts(t *testing.T) {
	o := probePhaseObserver(time.Second, &probePhaseDispatcher{})
	defer o.stopRun()
	o.s.paths.Store("candidate", pathSettings{isProbeOnly: true})
	o.s.paths.Store("real", pathSettings{})
	ctx := context.Background()
	var bytes atomic.Int64
	if o.AllowDispatch(ctx, "candidate", true) || !o.AllowDispatch(ctx, "real", false) ||
		o.AllowDispatch(withProbe(ctx, &bytes), "candidate", true) {
		t.Fatal("dispatch guard allowed a user or a generic probe context")
	}
}

func candidateJobsOf(jobs []*probeJob) []*probeJob {
	var out []*probeJob
	for _, j := range jobs {
		if j.reason == reasonCandidate {
			out = append(out, j)
		}
	}
	return out
}

// A credential that failed its check comes back after a pause that grows
// like a quarantine instead of waiting for a network change: the backend
// keeps a burned entry in the set to see it recover.
func TestProbeOnlyFailedRechecksWithGrowingPause(t *testing.T) {
	te := probeOnlyTestEngine(t, 2,
		&PathDefinition{Tag: "real", Cost: 1},
		&PathDefinition{Tag: "live", Cost: 2, IsProbeOnly: true},
		&PathDefinition{Tag: "burned", Cost: 3, IsProbeOnly: true})
	check := func(now time.Time, ok bool) {
		t.Helper()
		j, _ := te.tick(now)
		jobs := candidateJobsOf(j)
		if len(jobs) != 1 {
			t.Fatalf("at %v: %+v", now.Sub(t0), jobs)
		}
		r := probeResult{state: statusDown, class: "timeout"}
		if ok {
			r = probeResult{ok: true, state: statusUp}
		}
		te.onProbeResult(jobs[0], r, now)
	}
	check(t0, true)
	check(at(time.Second), false)
	burned := te.paths["burned"]
	first := burned.candidateRetryAt.Sub(at(time.Second))
	if first < 84*time.Second || first > 156*time.Second {
		t.Fatalf("first pause %v", first)
	}
	if j, _ := te.tick(burned.candidateRetryAt.Add(-time.Second)); len(candidateJobsOf(j)) != 0 {
		t.Fatalf("rechecked before the pause: %+v", j)
	}
	at1 := burned.candidateRetryAt
	check(at1, false)
	second := burned.candidateRetryAt.Sub(at1)
	if second < 168*time.Second || second > 312*time.Second {
		t.Fatalf("second pause %v", second)
	}
	at2 := burned.candidateRetryAt
	check(at2, true)
	if burned.candidateVerifiedAt != at2 || burned.candidateFailures != 0 || !burned.candidateRetryAt.IsZero() {
		t.Fatalf("recovery kept the pause: %v", burned.candidateRetryAt)
	}
}

// A credential that failed its check rests until its recheck, not in a
// quarantine: its event tells no quarantine, that of a real outbound does.
func TestFailedCandidateEventTellsNoQuarantine(t *testing.T) {
	te := probeOnlyTestEngine(t, 1,
		&PathDefinition{Tag: "real", Cost: 1},
		&PathDefinition{Tag: "burned", Cost: 2, IsProbeOnly: true})
	jobs, _ := te.tick(t0)
	if jobs = candidateJobsOf(jobs); len(jobs) != 1 {
		t.Fatalf("jobs %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{state: statusDown, class: "timeout"}, t0)
	f := te.lastOf(t, "path_state").Fields
	if f["outbound"] != "burned" || f["state"] != "down" || f["reason"] != "candidate" {
		t.Fatalf("event %v", f)
	}
	for _, key := range []string{"quarantine_ms", "quarantine_level", "is_provisional"} {
		if value, has := f[key]; has {
			t.Errorf("%s %v of an outbound not in quarantine", key, value)
		}
	}
	te.markDown(te.path("real"), "probe", at(time.Second))
	if f = te.lastOf(t, "path_state").Fields; f["outbound"] != "real" || f["quarantine_ms"].(float64) <= 0 || f["quarantine_level"] != 1.0 {
		t.Fatalf("event of a real outbound %v", f)
	}
}
