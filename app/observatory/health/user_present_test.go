package health

import (
	"testing"
	"time"
)

func newWakeEngine(t *testing.T) *testEngine {
	t.Helper()
	return newLoneEngine(t)
}

func startWake(t *testing.T, te *testEngine, now time.Time) *probeJob {
	t.Helper()
	te.userPresent(now)
	jobs, _ := te.tick(now)
	for _, j := range jobs {
		if j.reason == reasonWake && j.tag == "a1" {
			return j
		}
	}
	t.Fatalf("wake probe did not start: %v", jobTags(jobs))
	return nil
}

func TestUserPresentResults(t *testing.T) {
	for _, state := range []pathStatus{statusUp, statusDown, statusDegraded} {
		t.Run(state.String(), func(t *testing.T) {
			te := newWakeEngine(t)
			j := startWake(t, te, at(3*time.Minute))
			p := te.path("a1")
			te.onProbeResult(j, probeResult{state: state}, at(3*time.Minute+time.Second))
			if p.status != state || p.probing || p.stats.downsStrong != 0 || p.stats.downsWeak != 0 {
				t.Fatalf("wake changed state or passive evidence counters: %+v", p)
			}
			if state == statusDown {
				if te.sel.current() != "b1" || p.quarantineLevel != 1 || te.queued("a1", reasonAudit) {
					t.Fatalf("wake failure did not fail over: %+v", te.sel)
				}
				closed := te.takeDowns()
				if len(closed) != 2 || closed[0].tag != "a1" || closed[1].tag != "a1" ||
					!closed[0].pending || !closed[0].watch || !closed[1].pending || closed[1].watch ||
					!closed[0].at.Equal(closed[1].at) || len(te.silences) != 1 {
					t.Fatalf("wake down did not close pending or watch silent connections: %+v", closed)
				}
				te.dueSilences(at(3*time.Minute + time.Second + te.s.passive.stallTimeout))
				if closed := te.takeDowns(); len(closed) != 1 || !closed[0].silent {
					t.Fatalf("wake down did not close silent connections: %+v", closed)
				}
			} else if state == statusUp && te.sel.current() != "a1" {
				t.Fatal("healthy wake moved the selected path")
			} else if state == statusDegraded {
				if te.sel.current() != "b1" || len(te.downs) != 0 || p.quarantineLevel != 0 {
					t.Fatal("degraded wake did not use ordinary candidate search")
				}
			}
			failed := boolCount(state == statusDown)
			if te.report.counts.WakeProbeCount != 1 || te.report.counts.WakeProbeFailed != failed || p.stats.probesFailed != failed {
				t.Fatalf("wake counters: %+v %+v", te.report.counts, p.stats)
			}
			te.onProbeResult(j, probeResult{state: statusDown}, at(4*time.Minute))
			if te.report.counts.WakeProbeFailed != failed || p.stats.probes != 1 {
				t.Fatal("duplicate completion counted twice")
			}
		})
	}
}

func TestUserPresentAllDegradedRemainUsable(t *testing.T) {
	te := newWakeEngine(t)
	te.markDegraded(te.path("b1"), probeResult{state: statusDegraded}, at(0))
	j := startWake(t, te, at(3*time.Minute))
	te.onProbeResult(j, probeResult{state: statusDegraded}, at(3*time.Minute+time.Second))
	if te.sel.current() != "a1" || te.sel.exhausted || len(te.downs) != 0 || te.path("a1").quarantineLevel != 0 {
		t.Fatal("all degraded paths abandoned the cheaper usable path")
	}
}

func TestUserPresentEligibility(t *testing.T) {
	cases := []struct {
		name    string
		change  func(*testEngine)
		skipped int64
	}{
		{"fresh", func(te *testEngine) { te.onSuccess("a1", at(3*time.Minute-time.Second)) }, 1},
		{"freshness_boundary", func(te *testEngine) { te.onSuccess("a1", at(2*time.Minute)) }, 1},
		{"interval", func(te *testEngine) { te.path("a1").lastWakeAt = at(time.Minute) }, 1},
		{"pinned", func(te *testEngine) { te.pin("auto", "a1", at(time.Minute)) }, 0},
		{"exhausted", func(te *testEngine) { te.exhaust(te.sel, at(time.Minute)) }, 0},
		{"empty", func(te *testEngine) { te.sel.setActive("") }, 0},
		{"network_lost", func(te *testEngine) { te.onNetworkLost(at(time.Minute)) }, 0},
		{"doubt", func(te *testEngine) { te.doubt = true }, 0},
		{"retired", func(te *testEngine) { te.retiring["a1"] = true }, 0},
		{"replacing", func(te *testEngine) { te.replacing["a1"] = true }, 0},
		{"queued_other", func(te *testEngine) { te.enqueue("a1", reasonHost, "", at(3*time.Minute)) }, 0},
		{"running_other", func(te *testEngine) {
			j := te.enqueue("a1", reasonHost, "", at(3*time.Minute))
			te.begin(j, at(3*time.Minute))
			te.dropQueued("a1", reasonHost)
		}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			te := newWakeEngine(t)
			tc.change(te)
			te.userPresent(at(3 * time.Minute))
			if te.queued("a1", reasonWake) || te.report.counts.WakeSkipped != tc.skipped {
				t.Fatalf("ineligible user presence queued or counted incorrectly: %+v", te.report.counts)
			}
		})
	}
	for _, clearSuccess := range []bool{false, true} {
		te := newWakeEngine(t)
		if clearSuccess {
			te.path("a1").lastSuccess = time.Time{}
		}
		te.userPresent(at(time.Minute + time.Nanosecond))
		if !te.queued("a1", reasonWake) {
			t.Fatal("stale or missing success did not queue")
		}
	}
}

func TestUserPresentDeduplicatesSelectorsAndCalls(t *testing.T) {
	te := newWakeEngine(t)
	cfg := fixtureSelector(&SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, []string{"a1", "b1"})
	other := te.addSelector("other", legacyScenarioSelector(cfg, nil))
	te.updateCandidates(other, []string{"a1", "b1"}, at(0))
	for i := range 10 {
		te.userPresent(at(3*time.Minute + time.Duration(i)*time.Second))
	}
	if len(te.queue) != 1 || te.report.counts.WakeSkipped != 9 || te.report.counts.WakeProbeCount != 0 {
		t.Fatalf("shared path or repeated calls duplicated wake: %+v", te.report.counts)
	}
	jobs, _ := te.tick(at(4 * time.Minute))
	if len(jobs) != 1 || jobs[0].reason != reasonWake {
		t.Fatalf("wake jobs: %v", jobTags(jobs))
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(4*time.Minute+time.Second))
	te.userPresent(at(3 * time.Minute).Add(te.s.wake.minInterval))
	if te.queued("a1", reasonWake) || te.report.counts.WakeSkipped != 10 {
		t.Fatal("limit started at enqueue rather than actual start")
	}
	te.userPresent(at(4 * time.Minute).Add(te.s.wake.minInterval))
	if !te.queued("a1", reasonWake) {
		t.Fatal("interval boundary did not release wake limit")
	}
}

func TestUserPresentCanceledQueuedJobSpendsNoInterval(t *testing.T) {
	te := newWakeEngine(t)
	te.starts = make([]time.Time, te.s.schedule.perMinute)
	for i := range te.starts {
		te.starts[i] = at(3 * time.Minute)
	}
	te.userPresent(at(3 * time.Minute))
	jobs, _ := te.tick(at(3 * time.Minute))
	if len(jobs) != 0 || !te.queued("a1", reasonWake) {
		t.Fatal("wake ignored shared budget")
	}
	te.onSuccess("a1", at(3*time.Minute+time.Second))
	if te.queued("a1", reasonWake) || !te.path("a1").lastWakeAt.IsZero() || te.report.counts.WakeProbeCount != 0 {
		t.Fatal("queued cancellation spent wake limit")
	}
	startWake(t, te, at(5*time.Minute))
}

func TestUserPresentStaleResultsCannotDecide(t *testing.T) {
	changes := []struct {
		name   string
		change func(*testEngine)
	}{
		{"traffic", func(te *testEngine) { te.onSuccess("a1", at(3*time.Minute+time.Second)) }},
		{"down", func(te *testEngine) { te.markDown(te.path("a1"), "traffic", at(3*time.Minute+time.Second)) }},
		{"choice_and_back", func(te *testEngine) {
			te.switchTo(te.sel, "b1", "host", at(3*time.Minute+time.Second))
			te.switchTo(te.sel, "a1", "host", at(3*time.Minute+2*time.Second))
		}},
		{"pin_and_release", func(te *testEngine) {
			te.pin("auto", "a1", at(3*time.Minute+time.Second))
			te.releaseBy("auto", "host", at(3*time.Minute+2*time.Second))
		}},
		{"exhausted", func(te *testEngine) { te.exhaust(te.sel, at(3*time.Minute+time.Second)) }},
		{"network", func(te *testEngine) { te.onNetworkChange("new", at(3*time.Minute+time.Second)) }},
		{"ignored_network", func(te *testEngine) {
			te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
			te.onNetworkChange("new", at(3*time.Minute+time.Second))
		}},
		{"lost", func(te *testEngine) { te.onNetworkLost(at(3*time.Minute + time.Second)) }},
		{"doubt", func(te *testEngine) { te.doubt = true }},
		{"provider_replace", func(te *testEngine) { te.invalidateProbeJobs("a1", at(3*time.Minute+time.Second)) }},
		{"profile", func(te *testEngine) { te.resetProbeProfile("a1", at(3*time.Minute+time.Second)) }},
	}
	for _, tc := range changes {
		for _, state := range []pathStatus{statusDown, statusUp, statusDegraded} {
			t.Run(tc.name+"/"+state.String(), func(t *testing.T) {
				te := newWakeEngine(t)
				j := startWake(t, te, at(3*time.Minute))
				tc.change(te)
				p := te.path("a1")
				before, current, level, downs := p.status, te.sel.current(), p.quarantineLevel, len(te.downs)
				te.onProbeResult(j, probeResult{state: state}, at(3*time.Minute+3*time.Second))
				if p.status != before || te.sel.current() != current || p.quarantineLevel != level || len(te.downs) != downs || te.queued("a1", reasonWake) {
					t.Fatal("stale wake changed path or requeued")
				}
				if te.report.counts.WakeProbeCount != 1 || te.report.counts.WakeProbeFailed != boolCount(state == statusDown) || p.stats.probesFailed != boolCount(state == statusDown) {
					t.Fatalf("stale counters: %+v", te.report.counts)
				}
			})
		}
	}
}

func TestUserPresentQueuedGuardAndCanceledCompletion(t *testing.T) {
	te := newWakeEngine(t)
	te.userPresent(at(3 * time.Minute))
	te.sel.setActive("b1")
	jobs, _ := te.tick(at(3 * time.Minute))
	if len(jobs) != 0 || te.queued("a1", reasonWake) || te.report.counts.WakeProbeCount != 0 {
		t.Fatal("obsolete queued wake started")
	}
	te.sel.setActive("a1")
	j := startWake(t, te, at(4*time.Minute))
	te.onProbeResult(j, probeResult{canceled: true}, at(4*time.Minute+time.Second))
	te.onProbeResult(j, probeResult{state: statusDown}, at(4*time.Minute+2*time.Second))
	if te.report.counts.WakeProbeCount != 1 || te.report.counts.WakeProbeFailed != 0 || te.path("a1").stats.probes != 0 || te.path("a1").probing {
		t.Fatal("canceled completion counted a result")
	}
}

func TestUserPresentIdlePriorityAndServerNameBudget(t *testing.T) {
	if reasonWake.String() != "wake" || reasonWake.priority() != reasonNetwork.priority() || reasonFailover.priority() >= reasonWake.priority() || reasonWake.priority() >= reasonRemembered.priority() {
		t.Fatal("wake priority differs from network change")
	}
	te := newWakeEngine(t)
	te.s.schedule.idleAfter = 2 * time.Minute
	te.noteDemand(at(0))
	j := startWake(t, te, at(3*time.Minute))
	if !te.idle(at(3 * time.Minute)) {
		t.Fatal("fixture did not become idle")
	}
	te.onProbeResult(j, probeResult{ok: true}, at(3*time.Minute+time.Second))
	te.path("a1").lastWakeAt = time.Time{}
	te.path("a1").serverName = "shared"
	te.startsByName["shared"] = make([]time.Time, te.s.schedule.probesPerServerNamePerMinute)
	for i := range te.startsByName["shared"] {
		te.startsByName["shared"][i] = at(5 * time.Minute)
	}
	te.userPresent(at(5 * time.Minute))
	jobs, _ := te.tick(at(5 * time.Minute))
	if len(jobs) != 0 || !te.queued("a1", reasonWake) {
		t.Fatal("wake ignored server-name budget")
	}
}

func TestUserPresentSettingsDefaultsAndZero(t *testing.T) {
	for _, tc := range []struct {
		cfg                 *Config
		freshness, interval time.Duration
	}{
		{&Config{}, time.Minute, 3 * time.Hour},
		{&Config{Wake: &WakeConfig{}}, time.Minute, 3 * time.Hour},
		{&Config{Wake: &WakeConfig{Freshness: -1, MinInterval: -1}}, 0, 0},
		{&Config{Wake: &WakeConfig{Freshness: int64(2 * time.Minute), MinInterval: int64(7 * time.Minute)}}, 2 * time.Minute, 7 * time.Minute},
	} {
		s := newSettings(tc.cfg)
		if s.wake.freshness != tc.freshness || s.wake.minInterval != tc.interval {
			t.Fatalf("wake settings: %+v", s.wake)
		}
	}
}

func TestUserPresentAddsNoAutomaticWork(t *testing.T) {
	te := newWakeEngine(t)
	te.s.schedule.idleAfter = 2 * time.Minute
	jobs, wake := te.tick(at(3 * time.Minute))
	if len(jobs) != 0 || !wake.IsZero() || te.report.counts.WakeProbeCount != 0 || te.report.counts.WakeSkipped != 0 {
		t.Fatalf("wake work appeared without a host call: jobs=%v wake=%v", jobTags(jobs), wake)
	}
}

func TestUserPresentSharesConcurrencyAndSpacing(t *testing.T) {
	te := newWakeEngine(t)
	other := te.enqueue("b1", reasonHost, "", at(3*time.Minute))
	te.begin(other, at(3*time.Minute))
	te.dropQueued("b1", reasonHost)
	te.userPresent(at(3 * time.Minute))
	jobs, _ := te.tick(at(3 * time.Minute))
	if len(jobs) != 0 || !te.queued("a1", reasonWake) || !te.path("a1").lastWakeAt.IsZero() {
		t.Fatal("wake ignored concurrent probe or spent limit waiting for it")
	}
	te.onProbeResult(other, probeResult{ok: true}, at(3*time.Minute+time.Millisecond))
	jobs, _ = te.tick(at(3*time.Minute + time.Millisecond))
	if len(jobs) != 0 {
		t.Fatal("wake ignored shared spacing")
	}
	jobs, _ = te.tick(at(3*time.Minute + 4*time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonWake {
		t.Fatal("wake did not start when shared budget released")
	}
}

func TestUserPresentQueuedReplacementDoesNotRequeue(t *testing.T) {
	for _, change := range []func(*testEngine){
		func(te *testEngine) { te.invalidateProbeJobs("a1", at(3*time.Minute+time.Second)) },
		func(te *testEngine) { te.resetProbeProfile("a1", at(3*time.Minute+time.Second)) },
		func(te *testEngine) {
			te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
			te.onNetworkChange("new", at(3*time.Minute+time.Second))
		},
	} {
		te := newWakeEngine(t)
		te.userPresent(at(3 * time.Minute))
		change(te)
		jobs, _ := te.tick(at(3*time.Minute + 2*time.Second))
		if len(jobs) != 0 || te.queued("a1", reasonWake) || !te.path("a1").lastWakeAt.IsZero() || te.report.counts.WakeProbeCount != 0 {
			t.Fatal("replaced queued wake was requeued or spent a start")
		}
		te.userPresent(at(3*time.Minute + 3*time.Second))
		if !te.queued("a1", reasonWake) {
			t.Fatal("a new user presence call inherited the canceled queue limit")
		}
	}
}

func TestUserPresentPromotesDeferredNetworkProbe(t *testing.T) {
	for _, sniLimited := range []bool{false, true} {
		t.Run(map[bool]string{false: "available", true: "server_name_limited"}[sniLimited], func(t *testing.T) {
			te := newWakeEngine(t)
			te.s.schedule.idleAfter = 2 * time.Minute
			te.onNetworkChange("new", at(3*time.Minute))
			te.enqueue("a1", reasonRemembered, "auto", at(3*time.Minute))
			other := te.enqueue("b1", reasonRemembered, "auto", at(3*time.Minute))
			jobs, _ := te.tick(at(3 * time.Minute))
			if len(jobs) != 0 || !te.queued("a1", reasonNetwork) || !te.idle(at(3*time.Minute)) {
				t.Fatal("network-change fixture did not retain deferred work in idle")
			}
			if sniLimited {
				te.path("a1").serverName = "wake-server"
				te.startsByName["wake-server"] = make([]time.Time, te.s.schedule.probesPerServerNamePerMinute)
				for i := range te.startsByName["wake-server"] {
					te.startsByName["wake-server"][i] = at(3 * time.Minute)
				}
			}
			for range 10 {
				te.userPresent(at(3 * time.Minute))
			}
			if te.queued("a1", reasonNetwork) || te.queued("a1", reasonRemembered) || !te.queued("a1", reasonWake) || len(te.queue) != 2 || te.report.counts.WakeSkipped != 9 {
				t.Fatal("deferred jobs were not replaced by one guarded wake")
			}
			jobs, _ = te.tick(at(3 * time.Minute))
			if sniLimited {
				if len(jobs) != 0 || !te.path("a1").lastWakeAt.IsZero() || te.report.counts.WakeProbeCount != 0 {
					t.Fatal("promoted wake bypassed server-name limit")
				}
				jobs, _ = te.tick(at(4 * time.Minute))
			}
			if len(jobs) != 1 || jobs[0].reason != reasonWake || jobs[0].tag != "a1" || len(te.queue) != 1 || te.queue[0] != other || !te.idle(at(4*time.Minute)) || te.report.counts.WakeProbeCount != 1 {
				t.Fatal("user presence failed to start exactly one wake or released other idle work")
			}
			started := te.path("a1").lastWakeAt
			te.onProbeResult(jobs[0], probeResult{ok: true}, started.Add(time.Second))
			te.userPresent(started.Add(te.s.wake.minInterval - time.Minute))
			if te.queued("a1", reasonWake) || te.report.counts.WakeSkipped != 10 {
				t.Fatal("promoted wake lost actual-start interval limit")
			}
			te.userPresent(started.Add(te.s.wake.minInterval))
			if !te.queued("a1", reasonWake) {
				t.Fatal("promoted wake interval did not expire at its boundary")
			}
		})
	}
}

func TestUserPresentKeepsSufficientExistingProbe(t *testing.T) {
	for _, tc := range []struct {
		name    string
		idle    bool
		running bool
		reason  probeReason
	}{
		{"idle_host", true, false, reasonHost},
		{"awake_network", false, false, reasonNetwork},
		{"awake_remembered", false, false, reasonRemembered},
		{"idle_running_network", true, true, reasonNetwork},
		{"idle_running_host", true, true, reasonHost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := newWakeEngine(t)
			if tc.idle {
				te.s.schedule.idleAfter = 2 * time.Minute
			}
			j := te.enqueue("a1", tc.reason, "auto", at(0))
			if tc.running {
				te.dropQueued("a1", tc.reason)
				te.begin(j, at(3*time.Minute))
			}
			deferred := te.enqueue("a1", reasonRemembered, "auto", at(3*time.Minute))
			te.userPresent(at(3 * time.Minute))
			if te.queued("a1", reasonWake) || !te.queued("a1", tc.reason) || te.report.counts.WakeSkipped != 0 {
				t.Fatal("sufficient existing probe was replaced or counted as a wake skip")
			}
			if deferred != nil && !te.queued("a1", reasonRemembered) {
				t.Fatal("a mixed deferred queue was partially removed")
			}
		})
	}
}
