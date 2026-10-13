package health

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func censusTestEngine(t *testing.T, fraction float64, paths ...*PathDefinition) *engine {
	t.Helper()
	for _, path := range paths {
		path.Balancer = "auto"
	}
	cfg := &Config{Paths: paths, Census: &CensusConfig{SampleFraction: fraction, StartAfter: int64(time.Second)},
		Features: &FeatureConfig{Report: v2Bool(true)},
		Schedule: &ScheduleConfig{IdleAfter: int64(time.Hour), MaxConcurrentProbes: 4, ProbesPerMinute: 100, ProbesPerServerNamePerMinute: 3},
		Probes:   []*ProbeProfile{{Name: "full", Steps: []*ProbeStep{{Name: "mandatory", Destinations: []string{"http://probe.test/"}}, {Name: "optional", IsOptional: true, Destinations: []string{"http://optional.test/"}}}}}}
	for _, p := range paths {
		p.Probe = "full"
	}
	e := newEngine(newSettings(cfg), 1)
	e.s.schedule.spacing, e.s.schedule.spacingJitter = time.Nanosecond, 0
	e.report.begin(t0)
	e.censusNetworkStarted(t0)
	s := e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: paths}))
	var tags []string
	for _, p := range paths {
		tags = append(tags, p.Tag)
	}
	e.noteDemand(t0)
	e.updateCandidates(s, tags, t0)
	return e
}

func censusStartedJob(t *testing.T, e *engine, now time.Time) *probeJob {
	t.Helper()
	jobs, _ := e.tick(now)
	if len(jobs) != 1 || jobs[0].reason != reasonCensus {
		t.Fatalf("want one census, got %+v", jobs)
	}
	return jobs[0]
}

func TestCensusLotteryIndependentRoutingRNGAndOncePerRun(t *testing.T) {
	a := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1})
	b := censusTestEngine(t, 0, &PathDefinition{Tag: "a", Cost: 1})
	if !a.census.selected || b.census.selected {
		t.Fatal("forced sample did not match")
	}
	for i := 0; i < 100; i++ {
		if a.rnd.Int63() != b.rnd.Int63() {
			t.Fatal("census consumed selection RNG")
		}
	}
	for _, fraction := range []float64{0, 1} {
		e := censusTestEngine(t, fraction, &PathDefinition{Tag: "a", Cost: 1})
		for i := 0; i < 10; i++ {
			e.noteDemand(at(time.Duration(i) * time.Second))
			jobs, _ := e.tick(at(time.Duration(i) * time.Second))
			for _, j := range jobs {
				e.onProbeResult(j, probeResult{ok: true}, at(time.Duration(i)*time.Second))
			}
		}
		if fraction == 0 && e.report.counts.CensusProbes != 0 {
			t.Fatal("unsampled probes")
		}
		if fraction == 1 && e.report.counts.CensusProbes != 1 {
			t.Fatal("repeated census")
		}
	}
}

func TestCensusContinuousNetworkTrafficAndKnownNetwork(t *testing.T) {
	e := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1})
	e.lastDemand.Store(0)
	if jobs, _ := e.tick(at(2 * time.Second)); len(jobs) != 0 {
		t.Fatal("started without user traffic")
	}
	e.noteDemand(at(2 * time.Second))
	j := censusStartedJob(t, e, at(2*time.Second))
	e.onProbeResult(j, probeResult{ok: true}, at(2*time.Second))
	e.tick(at(2 * time.Second))
	e.onNetworkChange("A", at(63*time.Second))
	e.queue = nil
	if jobs, _ := e.tick(at(64 * time.Second)); len(jobs) != 0 {
		t.Fatal("old traffic started new network")
	}
	e.noteDemand(at(64 * time.Second))
	j = censusStartedJob(t, e, at(64*time.Second))
	e.onProbeResult(j, probeResult{ok: true}, at(64*time.Second))
	e.tick(at(64 * time.Second))
	e.onNetworkChange("B", at(125*time.Second))
	e.queue = nil
	e.noteDemand(at(125 * time.Second))
	j = censusStartedJob(t, e, at(126*time.Second))
	e.onProbeResult(j, probeResult{ok: true}, at(126*time.Second))
	e.tick(at(126 * time.Second))
	e.onNetworkChange("A", at(127*time.Second))
	e.queue = nil
	e.noteDemand(at(127 * time.Second))
	if jobs, _ := e.tick(at(128 * time.Second)); len(jobs) != 0 {
		t.Fatal("return to network repeated census")
	}
}

func TestCensusEnumerationGroupDemandRecentAndActualLimit(t *testing.T) {
	paths := []*PathDefinition{{Tag: "recent", Cost: 0}, {Tag: "g1", Cost: 1, Group: "relay"}, {Tag: "g2", Cost: 1, Group: "relay"}, {Tag: "demand", Cost: 2, IsLazy: true}, {Tag: "secret", Cost: 3, IsLazy: true}, {Tag: "only", Cost: 4, IsProbeOnly: true}}
	for i := 0; i < 20; i++ {
		paths = append(paths, &PathDefinition{Tag: fmt.Sprintf("p%02d", i), Cost: float32(5 + i)})
	}
	e := censusTestEngine(t, 1, paths...)
	s := e.selectors["auto"]
	s.cfg = newSelectorSettings(&SelectorConfig{Paths: paths, Groups: []*PathGroup{{Name: "relay", Cost: 1, FailedInRowToLeave: 2}}})
	e.syncGroups(s)
	e.s.census.onDemand["demand"] = true
	e.paths["recent"].success(at(900 * time.Millisecond))
	g := s.groups["relay"]
	position := g.position
	first := g.members[(g.position+1)%len(g.members)]
	e.path(first).quarantineUntil = at(time.Hour)
	e.path(first).status = statusDown
	for i := 0; i < 40; i++ {
		now := at(time.Second + time.Duration(i)*time.Second)
		jobs, _ := e.tick(now)
		for _, j := range jobs {
			if j.reason != reasonCensus {
				t.Fatalf("unexpected %s", j.reason)
			}
			if j.tag == "only" && (len(j.profile.steps) != 1 || j.profile.steps[0].isOptional) {
				t.Fatal("probe-only optional steps")
			}
			e.onProbeResult(j, probeResult{ok: true}, now)
		}
	}
	v := e.takeCensus(at(time.Minute))
	if v == nil || !v.IsComplete {
		t.Fatalf("incomplete %+v", v)
	}
	if g.position != position {
		t.Fatal("census advanced group cursor")
	}
	if v.Paths["relay"].Member == first || v.Paths["relay"].Member == "" {
		t.Fatal("quarantined group member")
	}
	if v.Paths["recent"].Source != "recent" {
		t.Fatal("recent active path was probed")
	}
	if v.Paths["demand"].Result != "ok" || v.Paths["secret"].Reason != "on_demand" {
		t.Fatal("on-demand whitelist")
	}
	if e.report.counts.CensusProbes != 16 || v.Paths["p19"].Reason != "limit" {
		t.Fatalf("actual probe cap %d", e.report.counts.CensusProbes)
	}
}

func TestCensusResultDoesNotChangeControllerEvidenceQuarantineSelectionProviderOrQuality(t *testing.T) {
	for _, ok := range []bool{false, true} {
		t.Run(fmt.Sprint(ok), func(t *testing.T) {
			e := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1})
			p := e.path("a")
			p.serverName = "server"
			p.lastRTT = 10 * time.Millisecond
			p.quarantineUntil = at(time.Hour)
			p.failures = []failure{{at: t0, kind: failDial}}
			p.status = statusSuspect
			p.candidateEpoch = 3
			e.quality = newQualitySummary(t0)
			pBefore := *p
			qualityBefore := *e.quality
			selectionBefore := e.selectors["auto"].current()
			provider := newProvider(&providerSettings{tag: "source", balancer: "auto"})
			e.providers = append(e.providers, provider)
			providerBefore := *provider
			j := censusStartedJob(t, e, at(time.Second))
			e.onProbeResult(j, probeResult{ok: ok, bytes: 4096, class: "dial_timeout"}, at(2*time.Second))
			if !reflect.DeepEqual(pBefore, *p) {
				t.Fatalf("controller/evidence/proof/quarantine mutated: before=%+v after=%+v", pBefore, *p)
			}
			if selectionBefore != e.selectors["auto"].current() {
				t.Fatal("selection changed")
			}
			if !reflect.DeepEqual(qualityBefore, *e.quality) {
				t.Fatal("quality mutated")
			}
			if !reflect.DeepEqual(providerBefore, *provider) {
				t.Fatal("provider mutated")
			}
			if e.report.counts.CensusProbeBytes != 4096 || p.stats.probes != 0 {
				t.Fatal("census accounting leaked into ordinary probes")
			}
		})
	}
}

func TestCensusPreemptionBelowEveryOrdinaryReasonKeepsBudgetsAndIntent(t *testing.T) {
	for _, reason := range []probeReason{reasonConfirm, reasonFailover, reasonHost, reasonConnectivity, reasonUDPSwitch} {
		t.Run(reason.String(), func(t *testing.T) {
			e := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1}, &PathDefinition{Tag: "b", Cost: 2})
			j := censusStartedJob(t, e, at(time.Second))
			cancelled := false
			j.cancel = func() { cancelled = true }
			if reason == reasonConnectivity {
				e.enqueueConnectivity(at(time.Second))
			} else {
				e.enqueue("b", reason, "auto", at(time.Second))
			}
			if !cancelled || !j.invalidated || e.running[j.id] != nil {
				t.Fatal("ordinary work waited for census")
			}
			if len(e.starts) != 1 || len(e.startsByName["a"]) != 1 {
				t.Fatal("preemption erased charged budget")
			}
			if len(e.spacedUntil) != 0 {
				t.Fatal("census spacing delays ordinary work")
			}
			e.onProbeResult(j, probeResult{canceled: true, bytes: 100}, at(time.Second))
			if e.census.active.paths["a"].Reason != "priority" || e.census.active == nil {
				t.Fatal("lost interrupted row/remaining intent")
			}
			e.queue = nil
			j = censusStartedJob(t, e, at(2*time.Second))
			if j.tag != "b" {
				t.Fatal("remaining intent not preserved")
			}
			e.onProbeResult(j, probeResult{ok: true}, at(2*time.Second))
			e.tick(at(3 * time.Second))
			if v := e.takeCensus(at(3 * time.Second)); v == nil || v.IsComplete {
				t.Fatal("preempted snapshot claims complete")
			}
		})
	}
}

func TestCensusInterruptReasonsAndLateCanceledResult(t *testing.T) {
	for _, reason := range []string{"network_change", "network_lost", "idle", "memory", "close"} {
		t.Run(reason, func(t *testing.T) {
			e := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1}, &PathDefinition{Tag: "b", Cost: 2})
			j := censusStartedJob(t, e, at(time.Second))
			ctx, cancel := context.WithCancel(context.Background())
			j.cancel = cancel
			e.interruptCensus(reason, at(2*time.Second))
			if ctx.Err() == nil || e.census.active != nil || len(e.queue) != 0 || len(e.running) != 0 {
				t.Fatal("census interruption left work")
			}
			v := e.takeCensus(at(3 * time.Second))
			if v == nil || v.IsComplete || v.Paths["b"].Reason != reason {
				t.Fatalf("snapshot %+v", v)
			}
			e.onProbeResult(j, probeResult{ok: true, bytes: 99}, at(4*time.Second))
			if e.paths["a"].status != statusUnknown || e.report.counts.CensusProbeBytes != 99 {
				t.Fatal("late census result changed path/accounting")
			}
			if e.takeCensus(at(5*time.Second)) != nil {
				t.Fatal("snapshot repeated")
			}
		})
	}
}

func TestCensusSharedBudgetsOneServerPerMinuteAndOneAtOnce(t *testing.T) {
	e := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1}, &PathDefinition{Tag: "b", Cost: 2})
	e.serverNameFor = func(string) string { return "shared" }
	j := censusStartedJob(t, e, at(time.Second))
	if jobs, _ := e.tick(at(2 * time.Second)); len(jobs) != 0 {
		t.Fatal("parallel census")
	}
	e.onProbeResult(j, probeResult{ok: true}, at(2*time.Second))
	if jobs, wake := e.tick(at(3 * time.Second)); len(jobs) != 0 || !wake.Equal(at(61*time.Second)) {
		t.Fatalf("server quota jobs=%v wake=%v", jobs, wake)
	}
	j = censusStartedJob(t, e, at(61*time.Second))
	if j.tag != "b" {
		t.Fatal("second server-name probe")
	}
	if e.report.counts.CensusProbes != 2 {
		t.Fatal("quota count")
	}
}

func TestCensusSnapshotsShareWindowAndCaptureMetadata(t *testing.T) {
	e := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1})
	e.hostLabels = map[string]string{"type": "wifi"}
	j := censusStartedJob(t, e, at(time.Second))
	e.onProbeResult(j, probeResult{ok: true}, at(time.Second))
	e.tick(at(2 * time.Second))
	e.onNetworkChange("B", at(62*time.Second))
	e.queue = nil
	e.hostLabels = map[string]string{"type": "mobile"}
	e.noteDemand(at(62 * time.Second))
	j = censusStartedJob(t, e, at(63*time.Second))
	e.onProbeResult(j, probeResult{ok: true}, at(63*time.Second))
	e.tick(at(64 * time.Second))
	v := e.peekCensus(at(65 * time.Second))
	if v == nil || v.Network.Host["type"] != "wifi" || len(v.Additional) != 1 || v.Additional[0].Network.Host["type"] != "mobile" {
		t.Fatalf("metadata/window %+v", v)
	}
	if got := e.takeCensus(at(65 * time.Second)); got == nil || len(got.Additional) != 1 || e.peekCensus(at(66*time.Second)) != nil {
		t.Fatal("did not consume one window")
	}
}

func TestCensusResumesAfterExpiredAudit(t *testing.T) {
	e := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1}, &PathDefinition{Tag: "b", Cost: 2})
	e.s.schedule.perMinute = 1
	first := censusStartedJob(t, e, at(time.Second))
	e.onProbeResult(first, probeResult{ok: true}, at(time.Second))
	p := e.path("a")
	p.status = statusDown
	audit := e.enqueue("a", reasonAudit, "", at(2*time.Second))
	audit.auditDown = p.downID
	jobs, wake := e.tick(at(2 * time.Second))
	if len(jobs) != 0 || !wake.Equal(at(2*time.Second).Add(auditWindow+time.Nanosecond)) {
		t.Fatalf("setup expected pending audit behind quota: jobs=%v wake=%v", jobs, wake)
	}
	now := wake
	jobs, wake = e.tick(now)
	// The audit expires now; census must then use its own minute-quota wake,
	// not lose that wake and remain pending until the one-hour idle deadline.
	want := at(61 * time.Second)
	if len(jobs) != 0 || !wake.Equal(want) {
		t.Fatalf("expired audit stranded census: jobs=%v wake=%v want=%v queue=%v", jobs, wake, want, e.queue)
	}
}
