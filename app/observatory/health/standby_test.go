package health

import (
	"testing"
	"time"
)

func standbyTestEngine(t *testing.T, perMinute int32, paths ...*PathDefinition) *testEngine {
	t.Helper()
	var tags []string
	for _, p := range paths {
		tags = append(tags, p.Tag)
	}
	return newTestEngine(t, &Config{Schedule: tune(&ScheduleConfig{
		MaxConcurrentProbes: 4, ProbesPerMinute: perMinute, ProbesPerServerNamePerMinute: 100,
		Standby: &StandbyConfig{RecheckInterval: int64(30 * time.Minute)}}, scheduleKnobs{spacing: time.Second, spacingJitter: -1, idleAfter: time.Hour})}, &SelectorConfig{Paths: paths}, tags...)
}

// standbyJobs ticks the engine and returns the checks it started.
func standbyJobs(te *testEngine, now time.Time) ([]*probeJob, time.Time) {
	jobs, wake := te.tick(now)
	var out []*probeJob
	for _, j := range jobs {
		if j.reason == reasonStandby {
			out = append(out, j)
		}
	}
	return out, wake
}

func mustStandby(t *testing.T, te *testEngine, now time.Time, tag string) *probeJob {
	t.Helper()
	jobs, _ := standbyJobs(te, now)
	if len(jobs) != 1 || jobs[0].tag != tag {
		t.Fatalf("at %v: checks %+v, want one of %s", now.Sub(t0), jobs, tag)
	}
	return jobs[0]
}

func noStandby(t *testing.T, te *testEngine, now time.Time) time.Time {
	t.Helper()
	jobs, wake := standbyJobs(te, now)
	if len(jobs) != 0 {
		t.Fatalf("at %v: unexpected checks %+v", now.Sub(t0), jobs)
	}
	return wake
}

func twinPaths(extra ...*PathDefinition) []*PathDefinition {
	return append([]*PathDefinition{{Tag: "xray", Cost: 1}, {Tag: "twin", Cost: 2, IsLazy: true, IsStandby: true}}, extra...)
}

// A twin is checked once when it comes, then once a recheckInterval after
// the last probe that judged it; the result judges it and nothing else.
func TestStandbyChecksOnArrivalAndThenByInterval(t *testing.T) {
	te := standbyTestEngine(t, 100, twinPaths()...)
	j := mustStandby(t, te, t0, "twin")
	noStandby(t, te, at(time.Second))
	te.onProbeResult(j, probeResult{ok: true, state: statusUp}, at(2*time.Second))
	if p := te.path("twin"); p.status != statusUp || p.lastProbeResult.reason != "standby" {
		t.Fatalf("twin %v after %q", p.status, p.lastProbeResult.reason)
	}
	if te.sel.current() != "xray" {
		t.Fatalf("a check moved the balancer to %q", te.sel.current())
	}
	te.noteDemand(at(29 * time.Minute))
	if wake := noStandby(t, te, at(29*time.Minute)); !wake.Equal(at(30*time.Minute + 2*time.Second)) {
		t.Fatalf("next check at %v", wake.Sub(t0))
	}
	j = mustStandby(t, te, at(30*time.Minute+2*time.Second), "twin")
	te.onProbeResult(j, probeResult{state: statusDown, class: "timeout"}, at(30*time.Minute+3*time.Second))
	if p := te.path("twin"); p.status != statusDown || te.sel.current() != "xray" {
		t.Fatalf("failed check: twin %v, current %q", p.status, te.sel.current())
	}
	te.noteDemand(at(50 * time.Minute))
	noStandby(t, te, at(50*time.Minute))
}

// A probe of another reason judges the twin as well: the host asked for one.
func TestStandbyCountsEveryProbe(t *testing.T) {
	te := standbyTestEngine(t, 100, twinPaths()...)
	te.path("twin").standbyAt, te.path("twin").standbyEpoch = t0, te.candidateEpoch
	te.sel.standbySeen = map[string]bool{"twin": true}
	j := te.enqueue("twin", reasonHost, "auto", t0)
	te.begin(j, t0)
	te.queue = nil
	te.onProbeResult(j, probeResult{ok: true, state: statusUp}, at(20*time.Minute))
	te.noteDemand(at(40 * time.Minute))
	if wake := noStandby(t, te, at(40*time.Minute)); !wake.Equal(at(50 * time.Minute)) {
		t.Fatalf("next check at %v", wake.Sub(t0))
	}
}

// The choice of the balancer is not checked, and a check waiting in the
// queue is dropped once the balancer chooses the twin.
func TestStandbySkipsTheChoice(t *testing.T) {
	te := standbyTestEngine(t, 100, twinPaths()...)
	te.switchTo(te.sel, "twin", "failover", t0)
	noStandby(t, te, t0)
	te.switchTo(te.sel, "xray", "return", at(time.Second))
	te.updateStandby(te.sel, at(time.Second))
	if !te.queued("twin", reasonStandby) {
		t.Fatal("no check once the balancer left the twin")
	}
	te.switchTo(te.sel, "twin", "failover", at(time.Second))
	noStandby(t, te, at(2*time.Second))
	if te.queued("twin", reasonStandby) {
		t.Fatal("a check of the choice stayed in the queue")
	}
}

// A new network checks the twin again whatever its interval.
func TestStandbyChecksInANewNetwork(t *testing.T) {
	te := standbyTestEngine(t, 100, twinPaths()...)
	j := mustStandby(t, te, t0, "twin")
	te.onProbeResult(j, probeResult{ok: true, state: statusUp}, at(time.Second))
	noStandby(t, te, at(time.Minute))
	te.onNetworkChange("other", at(2*time.Minute))
	te.noteDemand(at(2 * time.Minute))
	var got *probeJob
	for i := 0; i < 5 && got == nil; i++ {
		now := at(2*time.Minute + time.Duration(i)*time.Second)
		jobs, _ := standbyJobs(te, now)
		for _, j := range jobs {
			if j.tag == "twin" {
				got = j
			}
		}
	}
	if got == nil {
		t.Fatal("the twin was not checked in the new network")
	}
}

// A twin that comes in a set, or comes back to it, is checked at once.
func TestStandbyChecksATwinThatCame(t *testing.T) {
	te := standbyTestEngine(t, 100, twinPaths()...)
	te.updateCandidates(te.sel, []string{"xray"}, t0)
	noStandby(t, te, t0)
	te.updateCandidates(te.sel, []string{"xray", "twin"}, at(time.Second))
	j := mustStandby(t, te, at(time.Second), "twin")
	te.onProbeResult(j, probeResult{ok: true, state: statusUp}, at(2*time.Second))
	te.updateCandidates(te.sel, []string{"xray"}, at(3*time.Second))
	noStandby(t, te, at(3*time.Second))
	te.updateCandidates(te.sel, []string{"xray", "twin"}, at(4*time.Second))
	mustStandby(t, te, at(4*time.Second), "twin")
}

// Checks go one at a time and spend the probe budget of the core.
func TestStandbyKeepsToTheLimits(t *testing.T) {
	te := standbyTestEngine(t, 1, twinPaths(&PathDefinition{Tag: "twin2", Cost: 3, IsLazy: true, IsStandby: true})...)
	j := mustStandby(t, te, t0, "twin")
	noStandby(t, te, at(time.Second))
	te.onProbeResult(j, probeResult{ok: true, state: statusUp}, at(2*time.Second))
	if wake := noStandby(t, te, at(3*time.Second)); wake.IsZero() || wake.After(at(time.Minute)) {
		t.Fatalf("budget wake at %v", wake.Sub(t0))
	}
	mustStandby(t, te, at(time.Minute), "twin2")
}

// Checks wait while the user is away, as candidates do, and come with the
// next connection.
func TestStandbyWaitsForTheUser(t *testing.T) {
	te := standbyTestEngine(t, 100, twinPaths()...)
	noStandby(t, te, at(2*time.Hour))
	if te.queued("twin", reasonStandby) {
		t.Fatal("a check waits in the queue while the user is away")
	}
	te.noteDemand(at(3 * time.Hour))
	mustStandby(t, te, at(3*time.Hour), "twin")
}

// Without standby an outbound probed on demand is not checked.
func TestStandbyNeedsKeepVerified(t *testing.T) {
	te := standbyTestEngine(t, 100, &PathDefinition{Tag: "xray", Cost: 1}, &PathDefinition{Tag: "twin", Cost: 2, IsLazy: true})
	for i := 0; i < 4; i++ {
		now := at(time.Duration(i) * 40 * time.Minute)
		te.noteDemand(now)
		noStandby(t, te, now)
	}
	if te.sel.cfg.isStandby("twin") || te.sel.standbySeen != nil {
		t.Fatal("an onDemand outbound counts as standby")
	}
}

func TestStandbyFromTheProviderAnswer(t *testing.T) {
	cfg := newProviderSettings(v2ProviderConfig())
	a, err := parseAnswer([]byte(`{"outbounds":[{"tag":"x","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":2,"probePolicy":"standby"}},{"tag":"y","protocol":"vless","selection":{"balancer":"auto","group":"g"}}],"groups":[{"name":"g","probe":"check","cost":3,"probePolicy":"standby"}]}`))
	if err == nil {
		_, err = checkHeads(cfg, a)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !a.paths[0].IsLazy || !a.paths[0].IsStandby || !a.groups[0].IsLazy || !a.groups[0].IsStandby {
		t.Fatalf("path %v group %v", a.paths[0], a.groups[0])
	}
	s := newSelectorSettings(&SelectorConfig{Paths: []*PathDefinition{a.paths[0]}})
	if tag := a.paths[0].Tag; !s.isLazy(tag) || !s.isStandby(tag) {
		t.Fatal("selector settings lost standby")
	}
}
