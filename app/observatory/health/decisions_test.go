package health

import (
	"testing"
	"time"
)

func drive(e *engine, from, to time.Time, step time.Duration, each func(now time.Time),
	answer func(j *probeJob, now time.Time) bool, done func() bool) time.Time {
	now := from
	for ; now.Before(to); now = now.Add(step) {
		if each != nil {
			each(now)
		}
		jobs, _ := e.tick(now)
		for _, j := range jobs {
			ok := true
			if answer != nil {
				ok = answer(j, now)
			}
			e.onProbeResult(j, probeResult{ok: ok, rtt: 50 * time.Millisecond, bytes: 40_000}, now)
		}
		if done != nil && done() {
			return now
		}
	}
	return now
}

// A network change while an returnToCheaper probe is in flight: the new returnToCheaper
// probe is deduplicated against the stale running one, the stale result is
// dropped, and returnToCheaperTarget stays set forever.
func TestReturnToCheaperSurvivesNetworkChangeDuringItsProbe(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	var inFlight *probeJob
	for now := at(5 * time.Minute); inFlight == nil && now.Before(at(7*time.Minute)); now = now.Add(time.Second) {
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			if j.reason == reasonReturnToCheaper && j.tag == "a1" {
				inFlight = j
			} else {
				te.onProbeResult(j, probeResult{ok: true}, now)
			}
		}
	}
	if inFlight == nil {
		t.Fatal("setup: no returnToCheaper probe started")
	}
	te.onNetworkChange("cafe", at(7*time.Minute))
	te.tick(at(7*time.Minute + time.Second))
	te.onProbeResult(inFlight, probeResult{ok: true}, at(7*time.Minute+2*time.Second))
	end := drive(te.engine, at(7*time.Minute+3*time.Second), at(3*time.Hour), time.Second,
		func(now time.Time) { te.noteDemand(now) }, nil, func() bool { return te.sel.current() == "a1" })
	if te.sel.current() != "a1" {
		t.Fatalf("a1 works in the new network, %v later the balancer is on %q, returnToCheaperTarget %q, returnToCheaperAt %v",
			end.Sub(at(7*time.Minute)), te.sel.current(), te.sel.returnToCheaperTarget, te.sel.returnToCheaperAt.Sub(t0))
	}
}

// The same with a failover probe (onNetworkChange: forget): the balancer
// waits for a probe that never answers and stays on a dead outbound.
func TestFailoverSurvivesNetworkChangeDuringItsProbe(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), NetworkChangeAction: NetworkChangeMode_FORGET}
	te := newTestEngine(t, nil, sc, "a1", "a2", "b1")
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	if len(jobs) != 1 || jobs[0].tag != "a2" || jobs[0].reason != reasonFailover {
		t.Fatalf("setup: %+v", jobs)
	}
	te.onNetworkChange("lte", at(2*time.Second))
	te.failN(3, "a1", failDial, at(3*time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: false, err: "timeout"}, at(9*time.Second))
	drive(te.engine, at(10*time.Second), at(time.Minute), time.Second,
		func(now time.Time) {
			te.noteDemand(now)
			if te.sel.current() == "a1" {
				te.fail("a1", failDial, "", now)
			}
		},
		func(j *probeJob, now time.Time) bool { return j.tag != "a1" }, nil)
	if te.sel.current() == "a1" {
		t.Fatalf("a minute later new connections still go to dead a1 (%v), waiting for probes of %v that are not queued (queue %d, running %d)",
			te.path("a1").status, te.sel.failoverProbing, len(te.queue), len(te.running))
	}
}

// The default recheck mode: Wi-Fi dies, the failover probe of a2 hangs,
// the host reports LTE where a1 is blocked and a2 works.
func TestFailoverAfterNetworkChangeInRecheckMode(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	if len(jobs) != 1 || jobs[0].tag != "a2" || jobs[0].reason != reasonFailover {
		t.Fatalf("setup: %+v", jobs)
	}
	te.onNetworkChange("lte", at(2*time.Second))
	te.failN(3, "a1", failDial, at(3*time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: false, err: "timeout"}, at(9*time.Second))
	end := drive(te.engine, at(9*time.Second), at(time.Minute), time.Second,
		func(now time.Time) {
			te.noteDemand(now)
			if te.sel.current() == "a1" {
				te.fail("a1", failDial, "", now)
			}
		},
		func(j *probeJob, now time.Time) bool { return j.tag != "a1" }, func() bool { return te.sel.current() != "a1" })
	if te.sel.current() != "a2" {
		t.Fatalf("a2 is cheaper and works on lte; left a1 at %v for %q, a1 quarantine level %d",
			end.Sub(t0), te.sel.current(), te.path("a1").quarantineLevel)
	}
}

// gc after half an hour without new connections drops the only balancer
// and then every path with its statistics.
func TestLongSilenceKeepsBalancerAndPathStats(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	te.path("a1").stats.bytesDown = 50 << 20
	te.tick(at(time.Second))
	te.tick(at(31 * time.Minute))
	if p, ok := te.paths["a1"]; !ok || p.stats.bytesDown == 0 {
		t.Fatalf("after 31 min without new connections a1 and its 50 MB of statistics are gone (balancer kept: %v)", te.selectors["auto"] != nil)
	}
}

// A second markDown of an outbound that is already down (here the network
// probe of the chosen outbound fails after traffic already took it down)
// restarts failover: the in-flight probe of the cheaper candidate is
// abandoned and the outbound's quarantine level is bumped twice.
func TestSecondRuleOutKeepsCandidateProbe(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.onNetworkChange("lte", at(0))
	jobs, _ := te.tick(at(0))
	if len(jobs) != 1 || jobs[0].reason != reasonNetwork || jobs[0].tag != "a1" {
		t.Fatalf("setup: %+v", jobs)
	}
	te.failN(3, "a1", failDial, at(time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(2*time.Second))
	level := te.path("a1").quarantineLevel
	drive(te.engine, at(3*time.Second), at(time.Minute), time.Second,
		func(now time.Time) { te.noteDemand(now) },
		func(j *probeJob, now time.Time) bool { return j.tag != "a1" }, nil)
	if te.sel.current() != "a2" || level != 1 {
		t.Fatalf("a2 is cheaper and passed its probe, but the balancer is on %q; a1 quarantine level %d after one incident",
			te.sel.current(), level)
	}
}

// A network change while nobody uses the tunnel starts probes at once.
func TestNoProbesWhileIdleOnNetworkChange(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onNetworkChange("lte", at(time.Hour))
	jobs, _ := te.tick(at(time.Hour))
	if len(jobs) != 0 {
		t.Fatalf("idle for an hour, a network change started %d probe(s): %s %s", len(jobs), jobs[0].tag, jobs[0].reason)
	}
}

// A cheaper candidate that appears later (providers, AddOutbound) is never
// tried: returnToCheaperAt was zero because nothing was cheaper at the last switch.
func TestCheaperCandidateAddedLaterIsUsed(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "b1")
	te.sel.cfg.paths["a1"] = &costEntrySettings{cost: 1}
	te.sel.cfg.baseOrder = append([]string{"a1"}, te.sel.cfg.baseOrder...)
	te.updateCandidates(te.sel, []string{"a1", "b1"}, at(time.Second))
	drive(te.engine, at(2*time.Second), at(2*time.Hour), time.Second,
		func(now time.Time) { te.noteDemand(now) }, nil, func() bool { return te.sel.current() == "a1" })
	if te.sel.current() != "a1" {
		t.Fatalf("cheaper a1 added and working, two hours later still on %q (returnToCheaperAt zero: %v)", te.sel.current(), te.sel.returnToCheaperAt.IsZero())
	}
}

// The failover candidate leaves the candidates while its probe is in
// flight: failoverProbing is cleared and nothing moves the balancer on.
func TestFailoverCandidateRemovedDuringItsProbe(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	if len(jobs) != 1 || jobs[0].tag != "a2" {
		t.Fatalf("setup: %+v", jobs)
	}
	te.updateCandidates(te.sel, []string{"a1", "b1"}, at(2*time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(3*time.Second))
	drive(te.engine, at(4*time.Second), at(time.Minute), time.Second,
		func(now time.Time) {
			te.noteDemand(now)
			if te.sel.current() == "a1" {
				te.fail("a1", failDial, "", now)
			}
		},
		func(j *probeJob, now time.Time) bool { return j.tag != "a1" }, nil)
	if te.sel.current() == "a1" {
		t.Fatalf("a minute later still on dead a1 (%v, quarantined %v), b1 is never probed", te.path("a1").status, te.path("a1").quarantined(at(time.Minute)))
	}
}

// With one balancer, gc after half an hour without new connections makes
// the next network change a no-op: the previous network's quarantine is kept
// and no return to the cheaper outbound is scheduled.
func TestNetworkChangeAfterLongSilenceIsApplied(t *testing.T) {
	cfg := &Config{Schedule: &ScheduleConfig{Quarantine: &Backoff{InitialDelay: int64(40 * time.Minute), MaxDelay: int64(time.Hour), JitterFraction: 0.0001}}}
	te := newTestEngine(t, cfg, returnToCheaperConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	// The phone sleeps; a leftover timer or a late failure runs the engine.
	te.tick(at(31 * time.Minute))
	_, kept := te.selectors["auto"]
	t.Logf("after gc: kept %v, a1 %v quarantined until %v", kept, te.path("a1").status, te.path("a1").quarantineUntil.Sub(t0))
	gen := te.generation
	te.onNetworkChange("lte", at(33*time.Minute))
	end := drive(te.engine, at(33*time.Minute+time.Second), at(38*time.Minute), time.Second,
		func(now time.Time) { te.noteDemand(now) },
		nil, func() bool { return te.sel.current() == "a1" })
	if te.sel.current() != "a1" {
		t.Fatalf("a1 works on lte, %v after the change still on %q; generation %d -> %d, a1 %v quarantined for %v more",
			end.Sub(at(33*time.Minute)), te.sel.current(), gen, te.generation, te.path("a1").status, te.path("a1").quarantineUntil.Sub(end))
	}
}

// Strong evidence is ignored while a confirming probe for weak evidence
// waits; the per-server-name budget can hold that probe for most of a minute.
func TestStrongEvidenceIsNotMutedByWaitingConfirm(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	back := drive(te.engine, at(5*time.Minute), at(10*time.Minute), time.Second,
		func(now time.Time) { te.noteDemand(now) }, nil, func() bool { return te.sel.current() == "a1" })
	if te.sel.current() != "a1" {
		t.Fatal("setup: no return to a1")
	}
	// a1 freezes again right after the return: stalls first, then dials fail.
	te.fail("a1", failStall, "x.test", back.Add(time.Second))
	te.fail("a1", failStall, "y.test", back.Add(2*time.Second))
	te.fail("a1", failPartialStall, "x.test", back.Add(3*time.Second))
	strong := back.Add(5 * time.Second)
	te.failN(3, "a1", failDial, strong)
	left := drive(te.engine, strong, strong.Add(2*time.Minute), 500*time.Millisecond,
		func(now time.Time) {
			te.noteDemand(now)
			if te.sel.current() == "a1" {
				te.fail("a1", failDial, "", now)
			}
		},
		func(j *probeJob, now time.Time) bool { return j.tag != "a1" }, func() bool { return te.sel.current() != "a1" })
	if d := left.Sub(strong); d > 10*time.Second {
		t.Fatalf("every new dial to a1 failed, yet new connections went to a1 for %v after the strong evidence", d)
	}
}

// The return penalty earned in one network delays the return in another.
func TestPenaltyDoesNotCrossNetworks(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	te.onNetworkChange("home", at(0))
	failoverToB(t, te, at(time.Second))
	back := drive(te.engine, at(6*time.Minute), at(20*time.Minute), time.Second,
		func(now time.Time) { te.noteDemand(now) }, nil, func() bool { return te.sel.current() == "a1" })
	failoverToB(t, te, back.Add(time.Minute))
	if te.sel.penaltyFor("a1") <= 1 {
		t.Fatalf("setup: no penalty")
	}
	change := back.Add(4 * time.Minute)
	te.onNetworkChange("cafe", change)
	end := drive(te.engine, change, change.Add(time.Hour), time.Second,
		func(now time.Time) { te.noteDemand(now) }, nil, func() bool { return te.sel.current() == "a1" })
	if d := end.Sub(change); d > 2*time.Minute {
		t.Fatalf("a1 works in the new network, the return took %v (penalty %v from home)", d, te.sel.penaltyFor("a1"))
	}
}

// penaltyServed survives a switch: the next return skips the penalty.
func TestPenaltyIsNotSkippedAfterSwitch(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1", "b2")
	failoverToB(t, te, at(0))
	te.sel.penalty["a1"] = 4
	te.events.drain()
	drive(te.engine, at(5*time.Minute), at(6*time.Minute), time.Second, func(now time.Time) { te.noteDemand(now) }, nil, nil)
	if countEvents(te.eventTypes(), "return_delayed") != 1 {
		t.Fatal("setup: no delay")
	}
	te.failN(3, "b1", failDial, at(6*time.Minute))
	started := drive(te.engine, at(6*time.Minute), at(20*time.Minute), time.Second,
		func(now time.Time) { te.noteDemand(now) }, nil,
		func() bool { return te.sel.returnToCheaperTarget == "a1" })
	if started.Before(at(20 * time.Minute)) {
		t.Fatalf("penalty 4 (15 min more) is pending, but after the move to %q the return to a1 started at %v", te.sel.current(), started.Sub(t0))
	}
}

// Probe results while the host reported no network still take outbounds
// down and walk the whole failover chain.
func TestNetworkLostPausesFailover(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.failN(3, "a1", failDial, at(0))
	te.onNetworkLost(at(500 * time.Millisecond))
	drive(te.engine, at(time.Second), at(30*time.Second), time.Second,
		func(now time.Time) { te.noteDemand(now) },
		func(j *probeJob, now time.Time) bool { return false }, nil)
	if te.sel.exhausted || te.path("a2").status == statusDown || te.path("b1").status == statusDown {
		t.Fatalf("no network, yet a2 %v b1 %v exhausted %v", te.path("a2").status, te.path("b1").status, te.sel.exhausted)
	}
}
