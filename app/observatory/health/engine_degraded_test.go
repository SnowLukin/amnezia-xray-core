package health

import (
	"testing"
	"time"
)

func degradedProbe() probeResult {
	return probeResult{ok: true, state: statusDegraded, profile: "full", steps: []probeStepResult{
		{name: "transport", ok: true, destination: 0},
		{name: "egress", class: "timeout", destination: -1},
	}}
}

func TestConfirmedDegradedSearchesForUpBeforeChoosing(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3, "c", 5)}, "a1", "b1", "c1")
	suspectWeak(te, "a1", 0)
	jobs, _ := te.tick(at(3 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("confirm jobs: %+v", jobs)
	}
	r := degradedProbe()
	te.onProbeResult(jobs[0], r, at(3*time.Second))
	r.steps[1].name = "changed"
	if te.path("a1").status != statusDegraded || te.path("a1").reason != "egress" || te.path("a1").lastProbeResult.steps[1].name != "egress" {
		t.Fatalf("degraded state or snapshot: %+v", te.path("a1"))
	}
	if te.path("a1").falseAlarms != 0 || !te.path("a1").quarantineUntil.IsZero() || te.sel.current() != "a1" {
		t.Fatal("degraded was treated as a false alarm or a down path")
	}
	events := te.eventsOf(t, "probe_result")
	if len(events) != 1 || events[0].Fields["state"] != "degraded" || events[0].Fields["profile"] != "full" {
		t.Fatalf("probe result surface: %+v", events)
	}
	steps, ok := events[0].Fields["steps"].([]any)
	if !ok || len(steps) != 2 || steps[1].(map[string]any)["error"] != "timeout" {
		t.Fatalf("probe steps surface: %+v", events[0].Fields["steps"])
	}
	jobs, _ = te.tick(at(6 * time.Second))
	if len(jobs) != 1 || jobs[0].tag != "b1" {
		t.Fatalf("first candidate: %+v", jobs)
	}
	te.onProbeResult(jobs[0], degradedProbe(), at(6*time.Second))
	if te.sel.current() != "a1" {
		t.Fatalf("switched to degraded while an unknown candidate remained: %q", te.sel.current())
	}
	jobs, _ = te.tick(at(9 * time.Second))
	if len(jobs) != 1 || jobs[0].tag != "c1" {
		t.Fatalf("second candidate: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(9*time.Second))
	if te.sel.current() != "c1" {
		t.Fatalf("did not prefer up over cheaper degraded: %q", te.sel.current())
	}
}

func TestAllDegradedKeepsCheapestLivePath(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3, "c", 5)}, "a1", "b1", "c1")
	suspectWeak(te, "a1", 0)
	for _, now := range []time.Time{at(3 * time.Second), at(6 * time.Second), at(9 * time.Second)} {
		jobs, _ := te.tick(now)
		if len(jobs) != 1 {
			t.Fatalf("jobs at %v: %+v", now, jobs)
		}
		te.onProbeResult(jobs[0], degradedProbe(), now)
	}
	if te.sel.current() != "a1" || te.sel.exhausted || te.path("a1").status != statusDegraded {
		t.Fatalf("all degraded: current=%q exhausted=%v state=%v", te.sel.current(), te.sel.exhausted, te.path("a1").status)
	}
}

func TestReturnProbesKeepDegradedCandidateEligibleWithoutSwitchingFromUp(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), ReturnToCheaper: tune(&ReturnToCheaperConfig{
		Interval: int64(time.Minute)}, returnKnobs{fixedPhase: true, successes: 2, spacing: time.Second})}
	te := newTestEngine(t, &Config{Schedule: tune(&ScheduleConfig{}, scheduleKnobs{idleAfter: time.Hour})}, sc, "a1", "b1")
	te.markDegraded(te.path("a1"), degradedProbe(), t0)
	te.markUp(te.path("b1"), "probe", t0)
	te.switchTo(te.sel, "b1", "test", at(time.Second))
	jobs, _ := te.tick(at(3 * time.Minute))
	if len(jobs) != 1 || jobs[0].reason != reasonReturnToCheaper || jobs[0].tag != "a1" {
		t.Fatalf("degraded cheap path was not rechecked: %+v", jobs)
	}
	te.onProbeResult(jobs[0], degradedProbe(), at(3*time.Minute))
	if te.sel.current() != "b1" || te.path("a1").status != statusDegraded || te.path("a1").quarantined(at(3*time.Minute)) {
		t.Fatal("return from up to degraded or degraded path was quarantined")
	}
	for _, now := range []time.Time{at(5 * time.Minute), at(5*time.Minute + 3*time.Second)} {
		jobs, _ = te.tick(now)
		if len(jobs) != 1 || jobs[0].reason != reasonReturnToCheaper {
			t.Fatalf("return probe at %v: %+v", now, jobs)
		}
		te.onProbeResult(jobs[0], probeResult{ok: true}, now)
	}
	if te.sel.current() != "a1" {
		t.Fatalf("up candidate did not return after two probes: %q", te.sel.current())
	}
}

func TestStaleDegradedProbeDoesNotRestorePreviousNetworkSnapshot(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	suspectWeak(te, "a1", 0)
	jobs, _ := te.tick(at(3 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("confirm jobs: %+v", jobs)
	}
	te.onNetworkChange("new", at(4*time.Second))
	te.onProbeResult(jobs[0], degradedProbe(), at(5*time.Second))
	p := te.path("a1")
	if p.status == statusDegraded || p.lastProbeResult.profile != "" || !p.lastProbeEnd.IsZero() {
		t.Fatalf("stale probe changed current network: state=%v result=%+v end=%v", p.status, p.lastProbeResult, p.lastProbeEnd)
	}
}

func TestStaleProbeCannotOverwriteFreshProbeFields(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	oldGeneration := te.generation
	te.onNetworkChange("new", at(time.Second))
	p := te.path("a1")
	p.probing = true
	oldCanceled := &probeJob{id: 101, tag: "a1", reason: reasonConfirm, generation: oldGeneration}
	te.onProbeResult(oldCanceled, probeResult{canceled: true}, at(2*time.Second))
	if !p.probing {
		t.Fatal("canceled old probe cleared current probe's running flag")
	}
	fresh := &probeJob{id: 102, tag: "a1", reason: reasonHost, generation: te.generation}
	te.onProbeResult(fresh, probeResult{ok: true, rtt: 100 * time.Millisecond, profile: "full"}, at(3*time.Second))
	old := &probeJob{id: 103, tag: "a1", reason: reasonConfirm, generation: oldGeneration}
	te.onProbeResult(old, probeResult{err: "old failure", class: "timeout", rtt: time.Second}, at(4*time.Second))
	if !p.lastProbeEnd.Equal(at(3*time.Second)) || p.lastRTT != 100*time.Millisecond || p.lastError != "" || p.lastClass != "" || p.lastProbeResult.profile != "full" {
		t.Fatalf("stale probe overwrote fresh fields: end=%v rtt=%v err=%q class=%q result=%+v",
			p.lastProbeEnd, p.lastRTT, p.lastError, p.lastClass, p.lastProbeResult)
	}
}

func TestDegradedSuppressesWeakConfirmationButStrongFailuresStillRuleItOut(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.markDegraded(te.path("a1"), degradedProbe(), t0)
	te.onSuccess("a1", at(time.Second))
	if te.path("a1").status != statusDegraded {
		t.Fatal("traffic erased a fresh degraded result")
	}
	suspectWeak(te, "a1", 2*time.Second)
	if te.path("a1").status != statusDegraded || te.queued("a1", reasonConfirm) {
		t.Fatal("weak evidence started another confirmation on degraded")
	}
	te.failN(3, "a1", failDial, at(5*time.Second))
	if te.path("a1").status != statusDown {
		t.Fatalf("strong failures left degraded path live: %v", te.path("a1").status)
	}
}
