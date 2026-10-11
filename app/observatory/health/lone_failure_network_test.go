package health

import (
	"testing"
	"time"
)

func newNetworkLoneEngine(t *testing.T, concurrent int32) *testEngine {
	t.Helper()
	cfg := connectivityConfig()
	cfg.Schedule = &ScheduleConfig{IdleAfter: int64(time.Hour), MaxConcurrentProbes: concurrent}
	return newTestEngine(t, cfg, &SelectorConfig{
		Paths: pathSpecs("a1", 1, "b1", 3), SkipProbeBeforeSwitch: true,
		NetworkChangeAction: NetworkChangeMode_FORGET,
	}, "a1", "b1")
}

func TestLoneFailureNoProofUsesPathAge(t *testing.T) {
	for _, elapsed := range []time.Duration{0, 2 * time.Minute, 2*time.Minute + time.Nanosecond} {
		t.Run(elapsed.String(), func(t *testing.T) {
			te := newTestEngine(t, nil, nil, "a1")
			te.fail("a1", failDial, "", at(elapsed))
			jobs, _ := te.tick(at(elapsed))
			want := elapsed > 2*time.Minute
			if (len(jobs) == 1) != want || (want && jobs[0].reason != reasonLone) {
				t.Fatalf("path age %v: jobs %+v, want lone %v", elapsed, jobs, want)
			}
			if p := te.path("a1"); !p.lastSuccess.IsZero() || !p.created.Equal(t0) {
				t.Fatalf("failure invented a success or moved creation: %+v", p)
			}
		})
	}
}

func TestLoneFailureNewPathKeepsOrdinaryNetworkGuard(t *testing.T) {
	te := newNetworkLoneEngine(t, 2)
	for second := 0; second < 3; second++ {
		te.fail("a1", failDial, "", at(time.Duration(second)*time.Second))
		if te.queued("a1", reasonLone) {
			t.Fatal("startup failure queued a lone probe")
		}
	}
	jobs, _ := te.tick(at(3 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConnectivity {
		t.Fatalf("three failures did not wait for the network: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(4*time.Second))
	if !te.networkDown || te.path("a1").status == statusDown || len(te.downs) != 0 {
		t.Fatal("startup outage ruled out the selected path")
	}
}

func TestLoneFailureWaitsForNetworkBeforeStarting(t *testing.T) {
	for _, concurrent := range []int32{1, 2} {
		for _, ok := range []bool{false, true} {
			te := newNetworkLoneEngine(t, concurrent)
			te.onSuccess("a1", t0)
			now := at(3 * time.Minute)
			te.fail("a1", failDial, "", now)
			jobs, _ := te.tick(now)
			if len(jobs) != 1 || jobs[0].reason != reasonConnectivity {
				t.Fatalf("concurrent %d: lone bypassed the network check: %+v", concurrent, jobs)
			}
			if more, _ := te.tick(now.Add(time.Second)); len(more) != 0 {
				t.Fatalf("lone started while the network check was running: %+v", more)
			}
			if p := te.path("a1"); !p.lastLoneAt.IsZero() || p.stats.loneProbes != 0 {
				t.Fatal("waiting for the network consumed the lone interval")
			}
			te.onProbeResult(jobs[0], probeResult{ok: ok}, now.Add(2*time.Second))
			more, _ := te.tick(now.Add(2 * time.Second))
			if !ok {
				if len(more) != 0 || !te.networkDown || te.queued("a1", reasonLone) ||
					te.path("a1").status == statusDown || len(te.downs) != 0 || len(te.silences) != 0 {
					t.Fatal("failed network check allowed a path decision")
				}
				continue
			}
			if len(more) != 1 || more[0].reason != reasonLone {
				t.Fatalf("passed network check did not release lone: %+v", more)
			}
			te.onProbeResult(more[0], probeResult{ok: false}, now.Add(3*time.Second))
			te.onProbeResult(more[0], probeResult{ok: false}, now.Add(4*time.Second))
			p := te.path("a1")
			if p.status != statusDown || p.provisional || p.stats.downsStrong != 1 ||
				p.quarantineLevel != 1 || te.sel.current() != "b1" || te.queued("a1", reasonAudit) {
				t.Fatalf("failed lone did not make one strong decision: %+v", p)
			}
			if !p.lastLoneAt.Equal(now.Add(2 * time.Second)) {
				t.Fatal("lone interval did not start at the actual probe start")
			}
		}
	}
}

func TestLoneFailureConnectivityFreshness(t *testing.T) {
	for _, age := range []time.Duration{0, 4 * defaultConnectivityTimeout, 4*defaultConnectivityTimeout + time.Nanosecond} {
		t.Run(age.String(), func(t *testing.T) {
			te := newNetworkLoneEngine(t, 2)
			te.onSuccess("a1", t0)
			now := at(3 * time.Minute)
			te.onConnectivity(probeResult{ok: true}, now.Add(-age))
			te.fail("a1", failDial, "", now)
			jobs, _ := te.tick(now)
			want := reasonLone
			if age > 4*defaultConnectivityTimeout {
				want = reasonConnectivity
			}
			if len(jobs) != 1 || jobs[0].reason != want {
				t.Fatalf("check age %v: jobs %+v, want %v", age, jobs, want)
			}
		})
	}
	te := newNetworkLoneEngine(t, 2)
	te.onSuccess("a1", t0)
	te.connectivityIgnored = true
	j := startLone(t, te, at(3*time.Minute))
	te.onProbeResult(j, probeResult{ok: true}, at(3*time.Minute+time.Second))
	if te.path("a1").status != statusUp || len(te.path("a1").failures) != 0 {
		t.Fatal("ignored network check prevented a successful lone from clearing failures")
	}
}

func TestLoneFailureRechecksNetworkAfterBudgetWait(t *testing.T) {
	te := newNetworkLoneEngine(t, 1)
	te.onSuccess("a1", t0)
	now := at(3 * time.Minute)
	te.onConnectivity(probeResult{ok: true}, now)
	if _, _, err := te.requestProbe("auto", []string{"b1"}, now); err != nil {
		t.Fatal(err)
	}
	jobs, _ := te.tick(now)
	if len(jobs) != 1 || jobs[0].tag != "b1" {
		t.Fatalf("setup: host probe did not occupy the slot: %+v", jobs)
	}
	te.fail("a1", failDial, "", now)
	if more, _ := te.tick(now); len(more) != 0 {
		t.Fatal("lone bypassed the occupied slot")
	}
	later := now.Add(20 * time.Second)
	te.onProbeResult(jobs[0], probeResult{ok: false}, later)
	jobs, _ = te.tick(later)
	if len(jobs) != 1 || jobs[0].reason != reasonConnectivity {
		t.Fatalf("budget wait reused an outdated network check: %+v", jobs)
	}
}

func TestLoneFailureNetworkWaitRevalidatesEvidence(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testEngine, time.Time)
	}{
		{"success", func(te *testEngine, now time.Time) { te.onSuccess("a1", now) }},
		{"choice", func(te *testEngine, now time.Time) { te.switchTo(te.sel, "b1", "host", now) }},
		{"doubt", func(te *testEngine, now time.Time) { te.doubt = true }},
		{"lost", func(te *testEngine, now time.Time) { te.onNetworkLost(now) }},
		{"changed", func(te *testEngine, now time.Time) { te.onNetworkChange("next", now) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			te := newNetworkLoneEngine(t, 2)
			te.onSuccess("a1", t0)
			now := at(3 * time.Minute)
			te.fail("a1", failDial, "", now)
			jobs, _ := te.tick(now)
			if len(jobs) != 1 || jobs[0].reason != reasonConnectivity {
				t.Fatalf("setup: %+v", jobs)
			}
			tc.change(te, now.Add(time.Second))
			te.tick(now.Add(time.Second))
			te.onProbeResult(jobs[0], probeResult{ok: true}, now.Add(2*time.Second))
			more, _ := te.tick(now.Add(2 * time.Second))
			for _, j := range more {
				if j.reason == reasonLone {
					t.Fatal("obsolete lone survived the network wait")
				}
			}
			if te.path("a1").stats.loneProbes != 0 || te.path("a1").status == statusDown {
				t.Fatal("obsolete lone changed the path")
			}
		})
	}
}

func TestLoneFailureNoProofAgeRestartsWithNetwork(t *testing.T) {
	for _, ignored := range []bool{false, true} {
		te := newTestEngine(t, nil, &SelectorConfig{NetworkChangeAction: NetworkChangeMode_FORGET}, "a1")
		if ignored {
			te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
		}
		changed := at(10 * time.Minute)
		te.onNetworkChange("next", changed)
		te.fail("a1", failDial, "", changed)
		if te.queued("a1", reasonLone) {
			t.Fatal("a new network inherited the old path age")
		}
		te.fail("a1", failDial, "", changed.Add(2*time.Minute))
		if te.queued("a1", reasonLone) {
			t.Fatal("lone started at the exact freshness boundary")
		}
		te.fail("a1", failDial, "", changed.Add(2*time.Minute+time.Nanosecond))
		jobs, _ := te.tick(changed.Add(2*time.Minute + time.Nanosecond))
		if len(jobs) != 1 || jobs[0].reason != reasonLone {
			t.Fatalf("an old path without successes lost background failure detection: %+v", jobs)
		}
		if p := te.path("a1"); !p.created.Equal(t0) || !p.lastSuccess.IsZero() {
			t.Fatal("network change invented a success or reset path creation")
		}
	}
}

func TestLoneFailureOldConnectivityResultCannotAuthorizeNewNetwork(t *testing.T) {
	for _, ignored := range []bool{false, true} {
		te := newNetworkLoneEngine(t, 2)
		if ignored {
			te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
		}
		te.onSuccess("a1", t0)
		now := at(3 * time.Minute)
		te.fail("a1", failDial, "", now)
		jobs, _ := te.tick(now)
		if len(jobs) != 1 || jobs[0].reason != reasonConnectivity {
			t.Fatalf("setup: %+v", jobs)
		}
		te.onNetworkChange("next", now.Add(time.Second))
		later := now.Add(3 * time.Minute)
		te.onProbeResult(jobs[0], probeResult{ok: true}, later)
		te.fail("a1", failDial, "", later)
		jobs, _ = te.tick(later)
		if len(jobs) != 1 || jobs[0].reason != reasonConnectivity || !te.lastConnectivityOK.IsZero() {
			t.Fatalf("old network result authorized lone in the next network: %+v", jobs)
		}
	}
}

func TestConnectivityOldResultKeepsPendingSuspicionMoving(t *testing.T) {
	te := newNetworkLoneEngine(t, 1)
	te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
	te.failN(3, "a1", failDial, t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0].reason != reasonConnectivity {
		t.Fatalf("setup: %+v", jobs)
	}
	te.onNetworkChange("next", at(time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(2*time.Second))
	if te.path("a1").status != statusSuspect || !te.lastConnectivityOK.IsZero() {
		t.Fatal("a result from the previous network decided the pending suspicion")
	}
	jobs, _ = te.tick(at(2 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConnectivity {
		t.Fatalf("pending suspicion lost its network check: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(3*time.Second))
	if te.path("a1").status != statusDown || te.path("a1").stats.downsStrong != 1 || te.sel.current() != "b1" {
		t.Fatal("the current network result did not release the ordinary decision")
	}
}

func TestLoneFailureBackgroundDialsSurviveTheOrdinaryWindow(t *testing.T) {
	te := newNetworkLoneEngine(t, 1)
	te.onSuccess("a1", t0)
	te.fail("a1", failDial, "", at(90*time.Second))
	if jobs, _ := te.tick(at(90 * time.Second)); len(jobs) != 0 {
		t.Fatalf("fresh background path was probed: %+v", jobs)
	}
	te.fail("a1", failDial, "", at(3*time.Minute))
	if len(te.path("a1").failures) != 1 {
		t.Fatal("background failures unexpectedly reached the ordinary threshold")
	}
	jobs, _ := te.tick(at(3 * time.Minute))
	if len(jobs) != 1 || jobs[0].reason != reasonConnectivity {
		t.Fatalf("background lone lost the network preflight: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(3*time.Minute+time.Second))
	jobs, _ = te.tick(at(3*time.Minute + time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonLone {
		t.Fatalf("sparse failures never got their lone probe: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(3*time.Minute+2*time.Second))
	if te.sel.current() != "b1" || te.path("a1").stats.downsStrong != 1 || te.queued("a1", reasonAudit) {
		t.Fatal("one background failure and a failed probe did not switch the path")
	}
}

func TestLoneFailureAgeRestartsAfterNetworkRecovery(t *testing.T) {
	te := newLoneEngine(t)
	te.onNetworkLost(at(9 * time.Minute))
	te.endPause(at(10 * time.Minute))
	te.fail("a1", failDial, "", at(10*time.Minute))
	if te.queued("a1", reasonLone) {
		t.Fatal("lone counted time spent without a network")
	}
	j := startLone(t, te, at(12*time.Minute+time.Nanosecond))
	if j.reason != reasonLone || !te.path("a1").lastSuccess.Equal(t0) {
		t.Fatal("network recovery either lost lone detection or invented proof of life")
	}
}
