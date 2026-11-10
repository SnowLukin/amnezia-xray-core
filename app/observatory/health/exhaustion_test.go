package health

import (
	"slices"
	"testing"
	"time"
)

// chosen is what Select hands the router: the choice, or the outbound an
// exhausted balancer holds; "" sends connections to the fallback.
func (te *testEngine) chosen(now time.Time) string {
	if cur := te.sel.current(); cur != "" {
		return cur
	}
	return te.holding(te.sel, now)
}

// outageSecond plays one second of a user who keeps trying while the network
// is there or not: every probe and the check pass only with network, and so
// does a new connection through whatever the router gets.
func (te *testEngine) outageSecond(now time.Time, network bool) {
	te.noteDemand(now)
	jobs, _ := te.tick(now)
	for _, j := range jobs {
		te.onProbeResult(j, probeResult{ok: network}, now)
	}
	if tag := te.chosen(now); tag != "" {
		if network {
			te.onConnSuccess(tag, now, now)
		} else {
			te.fail(tag, failDialTimeout, "", now)
		}
	}
}

// The network goes away without the host noticing and without a check:
// every outbound fails. The balancer keeps the last one for a while rather
// than sending connections to the fallback, and once the network is back the
// core finds a working outbound within a minute, whatever the quarantines.
func TestSilentOutageHoldsAndRecoversWithinAMinute(t *testing.T) {
	for _, tc := range []struct {
		name       string
		candidates []string
	}{
		{"two outbounds", []string{"a1", "b1"}},
		{"one outbound", []string{"a1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, tc.candidates...)
			exhaustedAt := -1
			for s := 0; s < 600; s++ {
				now := at(time.Duration(s) * time.Second)
				te.outageSecond(now, false)
				if te.sel.exhausted && exhaustedAt < 0 {
					exhaustedAt = s
				}
				if exhaustedAt >= 0 && s < exhaustedAt+50 && te.chosen(now) == "" {
					t.Fatalf("%d s after exhaustion connections go to the fallback, want the last outbound held", s-exhaustedAt)
				}
			}
			if exhaustedAt < 0 {
				t.Fatal("the balancer never ran out of outbounds")
			}
			back := -1
			for s := 600; s < 700 && back < 0; s++ {
				now := at(time.Duration(s) * time.Second)
				te.outageSecond(now, true)
				if te.sel.current() != "" {
					back = s - 600
				}
			}
			if back < 0 || back > 61 {
				t.Fatalf("an outbound chosen %d s after the network came back, want within a minute", back)
			}
		})
	}
}

// With a check, the balancer that ran out of outbounds holds the last one
// while the check finds no network, and a connection begun after the down
// that works brings it back.
func TestOutageWithCheckHoldsWhileTheNetworkIsGone(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	// The check passed just before: the failures are taken as the outbound's.
	te.onConnectivity(probeResult{ok: true}, at(0))
	paused := 0
	for s := 1; s < 50; s++ {
		now := at(time.Duration(s) * time.Second)
		te.outageSecond(now, false)
		if te.networkDown {
			paused++
			if te.chosen(now) != "a1" {
				t.Fatalf("%d s into the outage the router gets %q while paused, want a1 held", s, te.chosen(now))
			}
		}
	}
	if !te.sel.exhausted || paused < 30 {
		t.Fatalf("exhausted %v, paused %d s, want an exhausted balancer paused for the network", te.sel.exhausted, paused)
	}
	te.outageSecond(at(50*time.Second), true)
	if te.sel.current() != "a1" || te.networkDown {
		t.Fatalf("after the network came back active %q paused %v, want a1 at once", te.sel.current(), te.networkDown)
	}
}

// The check keeps failing past maxPause: the core no longer waits on it, the
// hold ends, and once the network is back the check and the rechecks bring
// an outbound back within the check's longest pause.
func TestLongOutageWithCheckRecovers(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onConnectivity(probeResult{ok: true}, at(0))
	for s := 1; s < 600; s++ {
		te.outageSecond(at(time.Duration(s)*time.Second), false)
	}
	if !te.sel.exhausted || te.chosen(at(600*time.Second)) != "" {
		t.Fatalf("exhausted %v router gets %q after ten minutes, want the fallback", te.sel.exhausted, te.chosen(at(600*time.Second)))
	}
	back := -1
	for s := 600; s < 700 && back < 0; s++ {
		te.outageSecond(at(time.Duration(s)*time.Second), true)
		if te.sel.current() != "" {
			back = s - 600
		}
	}
	if back < 0 || back > 40 {
		t.Fatalf("an outbound chosen %d s after the network came back, want within 40 s", back)
	}
}

// With a check that passes, every outbound is dead in a live network: the
// hold ends at once and the fallback gets the connections.
func TestCheckThatPassesEndsTheHold(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onConnectivity(probeResult{ok: true}, at(0))
	for s := 1; s < 30; s++ {
		now := at(time.Duration(s) * time.Second)
		te.noteDemand(now)
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			te.onProbeResult(j, probeResult{ok: j.reason == reasonConnectivity}, now)
		}
		if tag := te.chosen(now); tag != "" {
			te.fail(tag, failDialTimeout, "", now)
		}
	}
	if !te.sel.exhausted || te.chosen(at(30*time.Second)) != "" {
		t.Fatalf("exhausted %v router gets %q, want the fallback in a live network", te.sel.exhausted, te.chosen(at(30*time.Second)))
	}
}

// Every outbound fails in a live network for ten minutes, long enough for
// the quarantines to grow. While the user keeps trying, the core goes round
// them often enough that the first to work again is taken within a minute.
func TestExhaustedRechecksWhileTheUserWaits(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onConnectivity(probeResult{ok: true}, at(0))
	works := map[string]bool{}
	probesInLastMinute := 0
	back := -1
	for s := 1; s < 1500 && back < 0; s++ {
		now := at(time.Duration(s) * time.Second)
		if s == 900 {
			works["b1"] = true
		}
		te.noteDemand(now)
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			ok := works[j.tag]
			if j.reason == reasonConnectivity {
				ok = true
			} else if s >= 840 && s < 900 {
				probesInLastMinute++
			}
			te.onProbeResult(j, probeResult{ok: ok}, now)
		}
		if tag := te.chosen(now); tag != "" && !works[tag] {
			te.fail(tag, failDialTimeout, "", now)
		}
		if s >= 900 && te.sel.current() == "b1" {
			back = s - 900
		}
	}
	if back < 0 || back > 61 {
		t.Fatalf("b1 chosen %d s after it began to work, want within a minute", back)
	}
	if probesInLastMinute > 3 {
		t.Fatalf("%d probes in a minute of the outage, want the slow pace", probesInLastMinute)
	}
}

// Without demand the exhausted balancer probes nothing.
func TestExhaustedIdleProbesNothing(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onConnectivity(probeResult{ok: true}, at(0))
	for s := 1; s < 30; s++ {
		now := at(time.Duration(s) * time.Second)
		te.noteDemand(now)
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			te.onProbeResult(j, probeResult{ok: j.reason == reasonConnectivity}, now)
		}
		if tag := te.chosen(now); tag != "" {
			te.fail(tag, failDialTimeout, "", now)
		}
	}
	if !te.sel.exhausted {
		t.Fatal("not exhausted")
	}
	for s := 30; s < 3600; s++ {
		jobs, _ := te.tick(at(time.Duration(s) * time.Second))
		for _, j := range jobs {
			if j.reason != reasonConnectivity && s > 30+int(defaultIdleAfter/time.Second) {
				t.Fatalf("%s probed at %d s with nobody waiting", j.tag, s)
			}
			te.onProbeResult(j, probeResult{ok: j.reason == reasonConnectivity}, at(time.Duration(s)*time.Second))
		}
	}
}

// The network flickers: back for a moment, then gone again before the check
// that the moment asked for. The check stays in use; only a check that keeps
// failing between successes is ruled wrong.
func TestFlickeringNetworkKeepsTheCheck(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	for s := 0; s < 300; s++ {
		now := at(time.Duration(s) * time.Second)
		te.noteDemand(now)
		if s%60 == 30 {
			// A connection that got through in the moment the network was back.
			te.onSuccess("a1", now)
		}
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			te.onProbeResult(j, probeResult{ok: false}, now)
		}
		te.fail("a1", failDialTimeout, "", now)
		if te.connectivityState() == "ignored" {
			t.Fatalf("connectivity ignored at %d s in a network that flickers", s)
		}
	}
}

// The host reports the network lost and never that it is back. With a
// check, the core finds out by itself.
func TestNetworkLostEndsWithTheCheck(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onNetworkLost(at(time.Second))
	resumed := -1
	for s := 2; s < 120 && resumed < 0; s++ {
		now := at(time.Duration(s) * time.Second)
		te.noteDemand(now)
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			te.onProbeResult(j, probeResult{ok: true}, now)
		}
		if !te.networkDown {
			resumed = s
		}
	}
	if resumed < 0 || resumed > 40 {
		t.Fatalf("pause after NetworkLost ended at %d s without the host, want by the check within 40 s", resumed)
	}
}

// Without a check, a balancer that ran out of outbounds before the host
// reported the network lost holds its last outbound while the pause lasts:
// the first connection that works ends both.
func TestNetworkLostWhileExhaustedEndsWithTraffic(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	for s := 0; s < 120; s++ {
		te.outageSecond(at(time.Duration(s)*time.Second), false)
	}
	if !te.sel.exhausted {
		t.Fatal("not exhausted")
	}
	te.onNetworkLost(at(120 * time.Second))
	back := -1
	for s := 121; s < 200 && back < 0; s++ {
		te.outageSecond(at(time.Duration(s)*time.Second), true)
		if te.sel.current() != "" && !te.networkDown {
			back = s - 121
		}
	}
	if back < 0 || back > 2 {
		t.Fatalf("out of the pause and exhaustion %d s after the network came back, want at once", back)
	}
}

// A probe begun while the network was gone ends after user traffic through
// the outbound worked again: its failure is older than that proof.
func TestProbeFailureOlderThanWorkingTrafficKeepsThePath(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	te.onNetworkChange("other", at(time.Second))
	jobs, _ := te.tick(at(2 * time.Second))
	if len(jobs) == 0 {
		t.Fatal("no probe after the network change")
	}
	te.onConnSuccess("a1", at(3*time.Second), at(3*time.Second))
	for _, j := range jobs {
		te.onProbeResult(j, probeResult{ok: false, class: "dial_timeout"}, at(4*time.Second))
	}
	if p := te.path("a1"); p.status != statusUp || te.sel.current() != "a1" {
		t.Fatalf("a1 %v active %q, want up and chosen", p.status, te.sel.current())
	}
	te.enqueue("a1", reasonRecheck, "auto", at(5*time.Second))
	jobs, _ = te.tick(at(5 * time.Second))
	for _, j := range jobs {
		te.onProbeResult(j, probeResult{ok: false, class: "dial_timeout"}, at(6*time.Second))
	}
	if len(jobs) == 0 || te.path("a1").status != statusDown {
		t.Fatalf("a probe begun after the traffic failed: %d jobs, a1 %v, want down", len(jobs), te.path("a1").status)
	}
}

// Probes fail because their own destination is out of reach, while user
// connections through the outbound work: a connection begun after the down
// that works undoes it.
func TestNewConnectionThatWorksUndoesTheDown(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	te.onNetworkChange("other", at(time.Second))
	jobs, _ := te.tick(at(2 * time.Second))
	for _, j := range jobs {
		te.onProbeResult(j, probeResult{ok: false}, at(2*time.Second))
	}
	if te.path("a1").status != statusDown {
		t.Fatalf("a1 %v, want down after the failed network probe", te.path("a1").status)
	}
	te.onConnSuccess("a1", at(time.Second), at(3*time.Second))
	if te.path("a1").status == statusUp {
		t.Fatal("a connection begun before the down undid it")
	}
	te.onConnSuccess("a1", at(3*time.Second), at(4*time.Second))
	if p := te.path("a1"); p.status != statusUp || te.sel.current() != "a1" {
		t.Fatalf("a1 %v active %q after a new connection worked, want up and chosen", p.status, te.sel.current())
	}
}

// The backend form: one user slot and a probe-only account. The slot dies,
// the balancer runs out and holds it for user traffic, yet the refresh goes
// through the probe-only road and never waits on the held slot. With every
// road dead it moves on to the next via instead.
func TestHeldSlotIsNoRoadToTheBackend(t *testing.T) {
	paths := []*PathDefinition{{Tag: "p-slot", Cost: 1}, {Tag: "p-probe", Cost: 2, IsProbeOnly: true}}
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: paths}, "p-slot", "p-probe")
	p := te.prov()
	p.cfg.whenExhausted, p.cfg.probeOnlyRoads = true, true
	p.cfg.exhaustedDelay = durationRange{}
	reserveProof(te.engine, "p-probe", at(time.Second))
	for s := 2; !te.sel.exhausted; s++ {
		if s > 60 {
			t.Fatal("the balancer never ran out of outbounds")
		}
		te.fail("p-slot", failDialTimeout, "", at(time.Duration(s)*time.Second))
	}
	now := at(time.Minute)
	if got := te.chosen(now); got != "p-slot" {
		t.Fatalf("user traffic goes to %q, want the held p-slot", got)
	}
	expectRefresh(t, te, now, "exhausted")
	if roads, skip := te.resolveProviderVia(p.cfg, "auto", now); skip != "" || !slices.Equal(roads, []string{"p-probe"}) {
		t.Fatalf("roads %v skip %q, want the probe-only road", roads, skip)
	}
	te.paths["p-probe"].status = statusDown
	if roads, skip := te.resolveProviderVia(p.cfg, "auto", now); skip != "balancer_exhausted" || len(roads) != 0 {
		t.Fatalf("roads %v skip %q with every road dead, want the next via", roads, skip)
	}
}
