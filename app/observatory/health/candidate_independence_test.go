package health

import (
	"testing"
	"time"
)

func candidateIndependentEngine(t *testing.T, enabled bool, pc *ProviderConfig) (*engine, *selector) {
	t.Helper()
	f := v2NoEvidence()
	f.Candidates = v2Bool(enabled)
	paths := []*PathDefinition{
		{Tag: "real", Balancer: "auto", Cost: 1},
		{Tag: "candidate", Balancer: "auto", Cost: 2, IsProbeOnly: true},
		{Tag: "unused", Balancer: "auto", Cost: 3, IsProbeOnly: true},
	}
	cfg := &Config{Features: f, Paths: paths, Schedule: &ScheduleConfig{
		ProbeOnly: &ProbeOnlyConfig{KeepVerified: 1, RecheckInterval: int64(15 * time.Minute)},
	}}
	if pc != nil {
		cfg.Providers = []*ProviderConfig{pc}
	}
	e := newEngine(newSettings(cfg), 1)
	s := e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: paths, Features: v2NoClosures()}))
	e.updateCandidates(s, []string{"real", "candidate", "unused"}, t0)
	return e, s
}

func TestV2CandidatesIndependentOfProviderMinHealthy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		provider   bool
		minHealthy *bool
		threshold  int32
	}{
		{name: "base_only"},
		{name: "provider_default_off", provider: true},
		{name: "provider_explicit_off", provider: true, minHealthy: v2Bool(false)},
		{name: "provider_shortage", provider: true, minHealthy: v2Bool(true), threshold: 2},
		{name: "provider_enough_real", provider: true, minHealthy: v2Bool(true), threshold: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var pc *ProviderConfig
			if tc.provider {
				pc = v2ProviderConfig()
				pc.Features.MinHealthy = tc.minHealthy
				pc.Triggers = &ProviderTriggers{MinHealthyOutbounds: tc.threshold}
			}
			e, s := candidateIndependentEngine(t, true, pc)
			e.noteDemand(t0)
			jobs, _ := e.tick(t0)
			if len(jobs) != 1 || jobs[0].tag != "candidate" || jobs[0].reason != reasonCandidate {
				t.Fatalf("initial candidate verification requires provider trigger: %v", jobs)
			}
			e.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp}, t0)
			if e.probeOnlyVerified(s, t0) != 1 || s.current() != "real" || e.healthy(s) != 1 {
				t.Fatal("candidate proof affected real selection or usable health")
			}
			before := t0.Add(15*time.Minute - time.Nanosecond)
			e.noteDemand(before)
			jobs, wake := e.tick(before)
			if len(jobs) != 0 || !wake.Equal(t0.Add(15*time.Minute)) {
				t.Fatalf("recheck deadline: jobs=%v wake=%v", jobs, wake)
			}
			now := t0.Add(15 * time.Minute)
			e.noteDemand(now)
			jobs, _ = e.tick(now)
			if len(jobs) != 1 || jobs[0].tag != "candidate" || jobs[0].reason != reasonCandidate {
				t.Fatalf("expired proof requires provider trigger: %v", jobs)
			}
		})
	}
}

func TestV2CandidatesOffHasNoJobsOrTimer(t *testing.T) {
	e, s := candidateIndependentEngine(t, false, nil)
	e.noteDemand(t0)
	for _, now := range []time.Time{t0, t0.Add(15 * time.Minute), t0.Add(time.Hour)} {
		e.noteDemand(now)
		jobs, wake := e.tick(now)
		if len(jobs) != 0 || len(e.queue) != 0 || !wake.IsZero() || s.probeOnlyActive {
			t.Fatalf("disabled candidate work: jobs=%v queue=%v wake=%v", jobs, e.queue, wake)
		}
	}
}

func TestV2CandidateMaintenancePausesWithoutInvalidatingRunningProof(t *testing.T) {
	for _, gate := range []string{"no_demand", "idle", "pin", "memory", "network"} {
		t.Run(gate, func(t *testing.T) {
			e, s := candidateIndependentEngine(t, true, nil)
			e.noteDemand(t0)
			jobs, _ := e.tick(t0)
			if len(jobs) != 1 {
				t.Fatal("fixture did not start candidate")
			}
			j := jobs[0]
			pause := t0.Add(time.Second)
			switch gate {
			case "no_demand":
				e.lastDemand.Store(0)
			case "idle":
				e.noteDemand(pause.Add(-e.s.schedule.idleAfter - time.Second))
			case "pin":
				s.pinned = "real"
			case "memory":
				e.memoryLow = true
			case "network":
				e.networkDown = true
			}
			// A second queued candidate must not start in this same tick.
			e.enqueue("unused", reasonCandidate, s.tag, pause)
			jobs, wake := e.tick(pause)
			if len(jobs) != 0 || len(e.queue) != 0 || !wake.IsZero() || !e.candidateCurrent(j) {
				t.Fatalf("paused maintenance: jobs=%v queue=%v wake=%v current=%v", jobs, e.queue, wake, e.candidateCurrent(j))
			}
			e.onProbeResult(j, probeResult{ok: true, state: statusUp}, pause)
			if e.probeOnlyVerified(s, pause) != 1 {
				t.Fatal("temporary pause discarded a current running proof")
			}
			e.networkDown, e.memoryLow, s.pinned = false, false, ""
			now := pause.Add(15 * time.Minute)
			e.noteDemand(now)
			jobs, _ = e.tick(now)
			if len(jobs) != 1 || jobs[0].reason != reasonCandidate {
				t.Fatalf("maintenance did not resume after %s: %v", gate, jobs)
			}
		})
	}
}

func TestV2CandidatesWaitForFirstDemand(t *testing.T) {
	e, _ := candidateIndependentEngine(t, true, nil)
	jobs, wake := e.tick(t0)
	if len(jobs) != 0 || !wake.IsZero() || len(e.queue) != 0 {
		t.Fatalf("candidate maintenance started without demand: jobs=%v wake=%v", jobs, wake)
	}
	e.noteDemand(t0)
	jobs, _ = e.tick(t0)
	if len(jobs) != 1 || jobs[0].reason != reasonCandidate {
		t.Fatalf("first demand did not start candidate: %v", jobs)
	}
}
