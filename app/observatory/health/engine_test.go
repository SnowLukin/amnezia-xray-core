package health

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

var t0 = time.Unix(1_800_000_000, 0)

func at(d time.Duration) time.Time { return t0.Add(d) }

type testEngine struct {
	*engine
	sel  *selector
	conn uint32
}

func newTestEngine(t *testing.T, cfg *Config, sc *SelectorConfig, candidates ...string) *testEngine {
	t.Helper()
	return newSeededTestEngine(t, 1, cfg, sc, candidates...)
}

// newSeededTestEngine is newTestEngine whose random choices follow seed, as
// those of another device would.
func newSeededTestEngine(t *testing.T, seed int64, cfg *Config, sc *SelectorConfig, candidates ...string) *testEngine {
	t.Helper()
	if cfg == nil {
		cfg = &Config{}
	}
	if sc == nil {
		sc = &SelectorConfig{}
	}
	copySC := fixtureSelector(sc, candidates)
	cfg.Paths = append(cfg.Paths, copySC.Paths...)
	e := newEngine(legacyScenarioSettings(cfg), seed)
	s := e.addSelector("auto", legacyScenarioSelector(copySC, cfg))
	e.noteDemand(t0)
	e.updateCandidates(s, candidates, t0)
	return &testEngine{engine: e, sel: s}
}

func fixtureSelector(sc *SelectorConfig, candidates []string) *SelectorConfig {
	copySC := proto.Clone(sc).(*SelectorConfig)
	copySC.Paths = nil
	for _, candidate := range candidates {
		for _, spec := range sc.Paths {
			if strings.Contains(candidate, spec.Tag) {
				path := proto.Clone(spec).(*PathDefinition)
				path.Tag = candidate
				path.Balancer = "auto"
				copySC.Paths = append(copySC.Paths, path)
				break
			}
		}
		if len(sc.Paths) == 0 {
			copySC.Paths = append(copySC.Paths, &PathDefinition{Tag: candidate, Balancer: "auto", Cost: 1})
		}
	}
	return copySC
}

// fail reports a failure of a new user connection.
func (te *testEngine) fail(tag string, kind failureKind, target string, now time.Time) {
	te.conn++
	te.onFailure(tag, kind, target, te.conn, now)
}

func (te *testEngine) failN(n int, tag string, kind failureKind, now time.Time) {
	for i := 0; i < n; i++ {
		te.fail(tag, kind, "", now)
	}
}

func pathSpecs(pairs ...any) []*PathDefinition {
	var out []*PathDefinition
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, &PathDefinition{Tag: pairs[i].(string), Cost: float32(pairs[i+1].(int))})
	}
	return out
}

func observed(tags ...string) []*PathDefinition {
	out := make([]*PathDefinition, 0, len(tags))
	for _, tag := range tags {
		out = append(out, &PathDefinition{Tag: tag, Cost: 1})
	}
	return out
}

func quarantine(d time.Duration) *ScheduleConfig {
	return &ScheduleConfig{Quarantine: &Backoff{InitialDelay: int64(d)}, IdleAfter: int64(time.Hour)}
}

func TestZeroSuspectCooldownStaysOffAfterFalseAlarms(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: &ScheduleConfig{SuspectCooldown: -1}}, nil, "a1")
	p := te.path("a1")
	for i := range 3 {
		now := at(time.Duration(i+1) * time.Second)
		te.backOffWeakEvidence(p, now)
		if p.cooldownUntil.After(now) {
			t.Fatalf("false alarm %d set cooldown to %v", i+1, p.cooldownUntil.Sub(now))
		}
	}
}

// run ticks the engine at now, answers every started probe with the given
// results by tag (missing tags succeed) and repeats until nothing starts.
func (te *testEngine) run(t *testing.T, now time.Time, results map[string]bool) []*probeJob {
	t.Helper()
	var all []*probeJob
	for i := 0; i < 50; i++ {
		jobs, _ := te.tick(now)
		if len(jobs) == 0 {
			return all
		}
		for _, j := range jobs {
			ok, set := results[j.tag]
			if !set {
				ok = true
			}
			te.onProbeResult(j, probeResult{ok: ok, rtt: 50 * time.Millisecond, bytes: 40_000}, now)
			all = append(all, j)
		}
		now = now.Add(3 * time.Second)
	}
	t.Fatal("probe loop did not settle")
	return nil
}

func (te *testEngine) eventTypes() []string {
	raw, _ := te.events.drain()
	var evs []Event
	_ = json.Unmarshal(raw, &evs)
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

func TestStartPicksCheapestWithoutProbing(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("b", 3, "a", 1)}, "b1", "a1", "a2")
	if got := te.sel.current(); got != "a1" {
		t.Fatalf("start picked %q, want a1", got)
	}
	if jobs, _ := te.tick(t0); len(jobs) != 0 {
		t.Fatalf("start probed %d outbounds", len(jobs))
	}
}

func TestStartWithPassedOutbound(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), StartOutbound: "b1"}, "a1", "b1")
	if te.sel.current() != "b1" || te.sel.pinned != "" {
		t.Fatalf("start %q pinned %q, want b1 unpinned", te.sel.current(), te.sel.pinned)
	}
}

func TestUnmatchedTagCostsTheMost(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "x1", "b1", "a1")
	if te.sel.current() != "a1" {
		t.Fatalf("start picked %q, want a1", te.sel.current())
	}
	if te.sel.hasCandidate("x1") {
		t.Fatal("outbound without path became a candidate")
	}
}

func TestPathLevelFailuresSwitchAfterProbingCandidate(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	for i := 0; i < 3; i++ {
		te.fail("a1", failDialTimeout, "", at(time.Duration(i)*time.Second))
	}
	if te.path("a1").status != statusDown {
		t.Fatalf("a1 is %v, want down", te.path("a1").status)
	}
	if te.sel.current() != "a1" {
		t.Fatalf("switched before the candidate was probed")
	}
	jobs := te.run(t, at(3*time.Second), map[string]bool{"a1": false})
	if !slices.Equal(reasons(jobs), []string{"failover:a2", "audit:a1"}) {
		t.Fatalf("unexpected probes %+v", jobs)
	}
	if te.sel.current() != "a2" {
		t.Fatalf("active %q, want a2", te.sel.current())
	}
}

func TestDialRetriesOfOneConnectionCountOnce(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	for i := 0; i < 5; i++ {
		te.onFailure("a1", failDial, "", 7, at(time.Duration(i)*200*time.Millisecond))
	}
	if te.path("a1").status == statusDown || len(te.path("a1").failures) != 1 {
		t.Fatalf("retries of one connection counted %d times", len(te.path("a1").failures))
	}
	te.onFailure("a1", failDial, "", 8, at(2*time.Second))
	te.onFailure("a1", failDial, "", 9, at(3*time.Second))
	if te.path("a1").status != statusDown {
		t.Fatalf("three connections failed, a1 is %v", te.path("a1").status)
	}
}

func TestFailedCandidateMovesToNext(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.failN(3, "a1", failDial, at(0))
	te.run(t, at(time.Second), map[string]bool{"a2": false})
	if te.sel.current() != "b1" {
		t.Fatalf("active %q, want b1", te.sel.current())
	}
	if te.path("a2").status != statusDown {
		t.Fatalf("a2 is %v, want down", te.path("a2").status)
	}
}

func TestOneTargetDoesNotMakeSuspect(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1", "a2")
	for i := 0; i < 10; i++ {
		te.fail("a1", failNoResponse, "dead.test", at(time.Duration(i)*time.Second))
	}
	if te.path("a1").status == statusSuspect || te.path("a1").status == statusDown {
		t.Fatalf("one dead site made a1 %v", te.path("a1").status)
	}
}

func suspectWeak(te *testEngine, tag string, base time.Duration) {
	te.fail(tag, failStall, "x.test", at(base))
	te.fail(tag, failStall, "y.test", at(base+time.Second))
	te.fail(tag, failPartialStall, "x.test", at(base+2*time.Second))
}

func TestWeakEvidenceIsConfirmedAndFalseAlarmBacksOff(t *testing.T) {
	cfg := &Config{Schedule: &ScheduleConfig{SuspectCooldown: int64(time.Minute)}}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1", "a2")
	suspectWeak(te, "a1", 0)
	jobs := te.run(t, at(3*time.Second), nil)
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("want one confirming probe, got %+v", jobs)
	}
	if te.sel.current() != "a1" || te.path("a1").status != statusUp {
		t.Fatalf("false alarm moved traffic: active %q, a1 %v", te.sel.current(), te.path("a1").status)
	}
	// Inside the cooldown the same evidence is ignored.
	suspectWeak(te, "a1", 30*time.Second)
	if jobs, _ := te.tick(at(33 * time.Second)); len(jobs) != 0 {
		t.Fatalf("probed again inside the cooldown")
	}
	// After it, a second false alarm doubles the cooldown.
	suspectWeak(te, "a1", 70*time.Second)
	te.run(t, at(73*time.Second), nil)
	if got := te.path("a1").cooldownUntil.Sub(at(73 * time.Second)); got != 2*time.Minute {
		t.Fatalf("cooldown after second false alarm %v, want 2m", got)
	}
}

func TestTrafficThatAnswersASuspectIsAFalseAlarm(t *testing.T) {
	cfg := &Config{Schedule: &ScheduleConfig{SuspectCooldown: int64(time.Minute)}}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1", "a2")
	suspectWeak(te, "a1", 0)
	te.onSuccess("a1", at(2500*time.Millisecond))
	if jobs := te.run(t, at(3*time.Second), nil); len(jobs) != 0 {
		t.Fatalf("probed an outbound that traffic had just answered: %+v", jobs)
	}
	if te.path("a1").status != statusUp {
		t.Fatalf("a1 is %v, want up", te.path("a1").status)
	}
	suspectWeak(te, "a1", 30*time.Second)
	if jobs, _ := te.tick(at(33 * time.Second)); len(jobs) != 0 {
		t.Fatalf("probed again inside the cooldown")
	}
}

func TestDownOutboundAsksToEndWaitingConnections(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	downs := te.takeDowns()
	if len(downs) != 1 || downs[0].tag != "a1" || !downs[0].at.Equal(at(0)) {
		t.Fatalf("down marks %+v, want a1 at the decision", downs)
	}
	// Connections still went to a1 while b1 was probed; they end at the switch.
	te.run(t, at(time.Second), map[string]bool{"a1": false})
	downs = te.takeDowns()
	if te.sel.current() != "b1" || len(downs) != 1 || downs[0].tag != "a1" || !downs[0].at.Equal(at(time.Second)) {
		t.Fatalf("active %q, down marks %+v, want a1 at the switch", te.sel.current(), downs)
	}
}

func TestWaitingConnectionsAreKeptWhenAskedOrWhenTheNetworkIsInDoubt(t *testing.T) {
	cfg := &Config{Passive: &PassiveConfig{ShouldKeepPendingOnDown: true}}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	te.run(t, at(time.Second), nil)
	for _, d := range te.takeDowns() {
		if d.pending {
			t.Fatalf("keepPendingOnDown still marked %+v", d)
		}
	}
	te = newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.doubt = true
	te.failN(3, "a1", failDial, at(0))
	if downs := te.takeDowns(); len(downs) != 0 {
		t.Fatalf("a provisional decision marked %+v", downs)
	}
}

func TestStrongEvidenceIgnoresFalseAlarmCooldown(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.path("a1").cooldownUntil = at(time.Hour)
	te.path("a1").falseAlarms = 7
	te.failN(3, "a1", failDial, at(time.Minute))
	if te.path("a1").status != statusDown {
		t.Fatalf("dial failures were muted by the cooldown: a1 %v", te.path("a1").status)
	}
}

func TestSuccessForgetsFailures(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1", "a2")
	te.fail("a1", failDial, "", at(0))
	te.fail("a1", failDial, "", at(time.Second))
	te.onSuccess("a1", at(2*time.Second))
	te.fail("a1", failDial, "", at(3*time.Second))
	if te.path("a1").status == statusDown {
		t.Fatalf("failures before a success counted")
	}
}

func TestFailuresOutsideWindowAreForgotten(t *testing.T) {
	cfg := &Config{Passive: &PassiveConfig{Window: int64(10 * time.Second)}}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1", "a2")
	te.fail("a1", failDial, "", at(0))
	te.fail("a1", failDial, "", at(5*time.Second))
	te.fail("a1", failDial, "", at(20*time.Second))
	if te.path("a1").status == statusDown {
		t.Fatalf("old failures counted")
	}
}

func TestWeakEvidenceOnOutboundNotChosenIsDropped(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	suspectWeak(te, "b1", 0)
	if te.path("b1").status == statusSuspect || len(te.path("b1").failures) != 0 {
		t.Fatalf("old connections left b1 %v with %d failures", te.path("b1").status, len(te.path("b1").failures))
	}
	if jobs, _ := te.tick(at(time.Minute)); len(jobs) != 0 {
		t.Fatalf("probed an outbound that is not chosen")
	}
}

func TestChosenAgainAfterQuarantineIsJudgedAgain(t *testing.T) {
	cfg := &Config{Schedule: quarantine(30 * time.Second)}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), SkipProbeBeforeSwitch: true}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	if te.sel.current() != "b1" {
		t.Fatalf("setup: want b1, got %q", te.sel.current())
	}
	te.failN(3, "b1", failDial, at(2*time.Minute))
	if te.sel.current() != "a1" {
		t.Fatalf("want a1 after its quarantine, got %q", te.sel.current())
	}
	te.failN(3, "a1", failDial, at(2*time.Minute+time.Second))
	if te.path("a1").status != statusDown || te.sel.current() == "a1" && !te.sel.exhausted {
		t.Fatalf("new failures of a1 were ignored: a1 %v active %q", te.path("a1").status, te.sel.current())
	}
}

func TestSharedCandidateAnswersEveryBalancer(t *testing.T) {
	e := newEngine(newSettings(&Config{}), 1)
	sc := &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3)}
	s1 := e.addSelector("s1", newSelectorSettings(sc))
	s2 := e.addSelector("s2", newSelectorSettings(sc))
	e.noteDemand(t0)
	e.updateCandidates(s1, []string{"a1", "b1"}, t0)
	e.updateCandidates(s2, []string{"a1", "b1"}, t0)
	for i := uint32(1); i <= 3; i++ {
		e.onFailure("a1", failDial, "", i, at(0))
	}
	jobs, _ := e.tick(at(time.Second))
	if len(jobs) != 1 {
		t.Fatalf("want one shared probe, got %d", len(jobs))
	}
	e.onProbeResult(jobs[0], probeResult{ok: true}, at(2*time.Second))
	for _, s := range []*selector{s1, s2} {
		if s.current() != "b1" || len(s.failoverProbing) != 0 {
			t.Fatalf("%s on %q, waiting for %v", s.tag, s.current(), s.failoverProbing)
		}
	}
}

func TestExhaustedThenRecheckRecovers(t *testing.T) {
	cfg := &Config{Schedule: &ScheduleConfig{Quarantine: &Backoff{InitialDelay: int64(30 * time.Second), Multiplier: 2, MaxDelay: int64(time.Hour)}}}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	te.run(t, at(time.Second), map[string]bool{"a1": false, "b1": false})
	if !te.sel.exhausted || te.sel.current() != "" {
		t.Fatalf("want exhausted, active %q", te.sel.current())
	}
	// Users keep trying, so the engine is not idle. Past the quarantine of
	// 30 s with its 30% spread and past the probe budget that the rechecks
	// during it spent (exhaustion_test.go), the cheapest is rechecked first.
	te.noteDemand(at(90 * time.Second))
	jobs := te.run(t, at(90*time.Second), map[string]bool{"a1": true, "b1": true})
	if len(jobs) == 0 || jobs[0].reason != reasonRecheck || jobs[0].tag != "a1" {
		t.Fatalf("want recheck of the cheapest, got %+v", jobs)
	}
	if te.sel.current() != "a1" || te.sel.exhausted {
		t.Fatalf("not recovered: active %q", te.sel.current())
	}
}

func TestIdleExhaustedDoesNotProbe(t *testing.T) {
	cfg := &Config{Schedule: &ScheduleConfig{IdleAfter: int64(time.Minute), Quarantine: &Backoff{InitialDelay: int64(10 * time.Second)}}}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	te.failN(3, "a1", failDial, at(0))
	if jobs, _ := te.tick(at(2 * time.Hour)); len(jobs) != 0 {
		t.Fatalf("idle engine probed")
	}
}

func returnToCheaperConfig() *SelectorConfig {
	return &SelectorConfig{
		Paths:       pathSpecs("a", 1, "b", 3),
		MinHoldTime: v2Int64(int64(time.Minute)),
		ReturnToCheaper: &ReturnToCheaperConfig{
			Interval:          int64(5 * time.Minute),
			SkipRandomPhase:   true,
			ProbesToReturn:    3,
			ProbeInterval:     int64(20 * time.Second),
			PenaltyWindow:     int64(5 * time.Minute),
			PenaltyMultiplier: 2,
			MaxPenaltyDelay:   int64(time.Hour),
		},
	}
}

func failoverToB(t *testing.T, te *testEngine, now time.Time) {
	t.Helper()
	te.failN(3, "a1", failDial, now)
	te.run(t, now.Add(time.Second), nil)
	if te.sel.current() != "b1" {
		t.Fatalf("failover went to %q", te.sel.current())
	}
}

func TestReturnToCheaperNeedsSuccessesInARow(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	te.run(t, at(time.Second), map[string]bool{"a1": false})
	if te.sel.current() != "b1" {
		t.Fatalf("failover went to %q", te.sel.current())
	}
	// Before the interval nothing happens.
	if jobs, _ := te.tick(at(4 * time.Minute)); len(jobs) != 0 {
		t.Fatalf("returnToCheaper before interval: %+v", jobs)
	}
	var returnToCheapers []time.Time
	for now := at(5 * time.Minute); now.Before(at(10*time.Minute)) && te.sel.current() != "a1"; now = now.Add(time.Second) {
		for _, j := range te.run(t, now, map[string]bool{"a1": true, "b1": true}) {
			if j.reason == reasonReturnToCheaper {
				returnToCheapers = append(returnToCheapers, now)
			}
		}
	}
	if te.sel.current() != "a1" {
		t.Fatalf("did not return to a1")
	}
	if len(returnToCheapers) != 3 {
		t.Fatalf("returned after %d probes, want 3", len(returnToCheapers))
	}
	for i := 1; i < len(returnToCheapers); i++ {
		if gap := returnToCheapers[i].Sub(returnToCheapers[i-1]); gap < 13*time.Second {
			t.Fatalf("returnToCheaper probes %v apart, want about 20s", gap)
		}
	}
	if te.sel.returnedTo != "a1" {
		t.Fatalf("return not recorded")
	}
}

func TestReturnToCheaperFailureQuarantinesAndPenaltyDelaysNextReturn(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	te.run(t, at(time.Second), map[string]bool{"a1": false})
	if te.sel.current() != "b1" {
		t.Fatalf("failover went to %q", te.sel.current())
	}
	now := at(6 * time.Minute)
	for i := 0; i < 10 && te.sel.current() != "a1"; i++ {
		te.run(t, now, map[string]bool{"a1": true, "b1": true})
		now = now.Add(21 * time.Second)
	}
	if te.sel.current() != "a1" {
		t.Fatalf("did not return")
	}
	// a1 fails right after the return: penalty doubles the next wait.
	failAt := now.Add(time.Minute)
	te.failN(3, "a1", failDial, failAt)
	te.run(t, failAt.Add(time.Second), map[string]bool{"a1": false})
	if te.sel.current() != "b1" {
		t.Fatalf("failover went to %q", te.sel.current())
	}
	if te.sel.penaltyFor("a1") != 2 {
		t.Fatalf("penalty %v, want 2", te.sel.penaltyFor("a1"))
	}
	var delayed bool
	for now := failAt.Add(time.Minute); now.Before(failAt.Add(20 * time.Minute)); now = now.Add(time.Second) {
		te.tick(now)
		for _, ev := range te.eventTypes() {
			if ev == "return_delayed" {
				delayed = true
			}
		}
		if delayed {
			break
		}
	}
	if !delayed {
		t.Fatalf("penalized return was not delayed")
	}
}

func TestPenaltyClearsAfterReturnHolds(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	te.sel.penalty["a1"] = 4
	te.sel.returnedTo = "a1"
	te.sel.returnedAt = at(0)
	te.tick(at(4 * time.Minute))
	if te.sel.penaltyFor("a1") != 4 {
		t.Fatalf("penalty cleared before the window")
	}
	te.tick(at(6 * time.Minute))
	if te.sel.penaltyFor("a1") != 1 {
		t.Fatalf("penalty %v after the return held, want 1", te.sel.penaltyFor("a1"))
	}
}

func TestReturnToCheaperTargetLeavingCandidatesIsDropped(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	te.run(t, at(6*time.Minute), nil)
	if te.sel.returnToCheaperTarget != "a1" {
		t.Fatalf("setup: returnToCheaper target %q", te.sel.returnToCheaperTarget)
	}
	te.updateCandidates(te.sel, []string{"b1"}, at(6*time.Minute+time.Second))
	for now := at(6 * time.Minute); now.Before(at(8 * time.Minute)); now = now.Add(5 * time.Second) {
		te.run(t, now, nil)
	}
	if te.sel.current() != "b1" {
		t.Fatalf("returned to %q, which is no longer a candidate", te.sel.current())
	}
}

func TestNetworkChangeForgetsAndRemembersPerNetwork(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onNetworkChange("home", at(0))
	te.run(t, at(time.Second), nil)
	te.onSuccess("a1", at(2*time.Second))
	te.onNetworkChange("lte", at(time.Minute))
	te.run(t, at(time.Minute), nil)
	te.failN(3, "a1", failDial, at(2*time.Minute))
	te.run(t, at(2*time.Minute+time.Second), map[string]bool{"a1": false})
	if te.sel.current() != "b1" {
		t.Fatalf("failover went to %q", te.sel.current())
	}
	te.onSuccess("b1", at(3*time.Minute))
	if te.path("a1").status != statusDown {
		t.Fatalf("a1 should be down on lte")
	}
	// Back home: a1 is remembered for this network and tried at once.
	te.onNetworkChange("home", at(10*time.Minute))
	if te.path("a1").status != statusUnknown {
		t.Fatalf("network change did not forget a1 state")
	}
	te.noteDemand(at(10 * time.Minute))
	jobs := te.run(t, at(10*time.Minute), map[string]bool{"a1": true, "b1": true})
	if len(jobs) != 1 || jobs[0].reason != reasonRemembered || jobs[0].tag != "a1" {
		t.Fatalf("want one remembered probe of a1, got %+v", jobs)
	}
	if te.sel.current() != "a1" {
		t.Fatalf("active %q, want a1", te.sel.current())
	}
}

func TestUnknownNetworkSchedulesReturnToCheaperKnownDoesNot(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), MinHoldTime: v2Int64(int64(time.Second)),
		ReturnToCheaper: &ReturnToCheaperConfig{ProbesToReturn: 1}}
	te := newTestEngine(t, nil, sc, "a1", "b1")
	failoverToB(t, te, at(0))
	soon := func(now time.Time) bool {
		return !te.sel.returnToCheaperAt.IsZero() && !te.sel.returnToCheaperAt.After(now.Add(time.Second))
	}
	te.onNetworkChange("cafe", at(time.Minute))
	if !soon(at(time.Minute)) {
		t.Fatalf("unknown network did not schedule an returnToCheaper at once")
	}
	// b1 works in the cafe, so the cafe becomes a known network.
	te.onSuccess("b1", at(70*time.Second))
	te.onNetworkChange("office", at(2*time.Minute))
	te.onNetworkChange("cafe", at(3*time.Minute))
	if soon(at(3 * time.Minute)) {
		t.Fatalf("known network scheduled an returnToCheaper at once")
	}
	if te.sel.returnToCheaperAt.IsZero() {
		t.Fatalf("known network lost the regular returnToCheaper timer")
	}
	sc.ReturnToCheaper.TryOnAllNetworks = true
	sc.Paths = pathSpecs("a1", 1, "b1", 3)
	te.addSelector("auto", legacyScenarioSelector(sc, nil))
	te.onNetworkChange("cafe", at(4*time.Minute))
	if !soon(at(4 * time.Minute)) {
		t.Fatalf("with allNetworks a known network did not schedule an returnToCheaper at once")
	}
}

func TestIgnoreNetworkChangeKeepsState(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), NetworkChangeAction: NetworkChangeMode_IGNORE}, "a1", "b1")
	te.failN(3, "b1", failDial, at(0))
	te.onNetworkChange("x", at(time.Second))
	if te.path("b1").status != statusDown {
		t.Fatalf("ignored network change reset state")
	}
}

func TestIgnoreNetworkChangeKeepsProbesInFlight(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), NetworkChangeAction: NetworkChangeMode_IGNORE}, "a1", "b1")
	suspectWeak(te, "a1", 0)
	jobs, _ := te.tick(at(3 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("want a confirming probe, got %+v", jobs)
	}
	te.onNetworkChange("other", at(4*time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(5*time.Second))
	if te.path("a1").status != statusDown {
		t.Fatalf("the probe result was dropped: a1 %v", te.path("a1").status)
	}
}

func TestProbeBudget(t *testing.T) {
	cfg := &Config{Schedule: &ScheduleConfig{MinProbeInterval: int64(2 * time.Second), ProbeJitterFraction: 0.0001, ProbesPerServerNamePerMinute: 3, ProbesPerMinute: 100}}
	tags := []string{"a1", "a2", "a3", "a4", "a5"}
	te := newTestEngine(t, cfg, &SelectorConfig{}, tags...)
	// Five addresses behind one server name, as several IPs of one variant.
	te.serverNameFor = func(string) string { return "same.example" }
	for _, tag := range tags {
		te.enqueue(tag, reasonRecheck, "auto", t0)
	}
	if len(te.queue) != len(tags) {
		t.Fatalf("queued %d probes, want %d", len(te.queue), len(tags))
	}
	var starts []time.Time
	for now := t0; now.Before(t0.Add(70 * time.Second)); now = now.Add(100 * time.Millisecond) {
		jobs, _ := te.tick(now)
		if len(jobs) > 1 {
			t.Fatalf("%d probes at once", len(jobs))
		}
		for _, j := range jobs {
			starts = append(starts, now)
			te.onProbeResult(j, probeResult{ok: true}, now)
		}
	}
	for i := 1; i < len(starts); i++ {
		if starts[i].Sub(starts[i-1]) < 1900*time.Millisecond {
			t.Fatalf("probes %v apart", starts[i].Sub(starts[i-1]))
		}
	}
	inFirstMinute := 0
	for _, s := range starts {
		if s.Before(t0.Add(time.Minute)) {
			inFirstMinute++
		}
	}
	if inFirstMinute != 3 {
		t.Fatalf("%d probes to one server name in the first minute, want 3", inFirstMinute)
	}
	if len(starts) != len(tags) {
		t.Fatalf("%d probes ran, want %d", len(starts), len(tags))
	}
}

func TestPinnedStaysUntilRelease(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), StartPinnedOutbound: "a1"}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	te.run(t, at(time.Second), map[string]bool{"a1": false})
	if te.sel.current() != "a1" {
		t.Fatalf("pinned balancer moved to %q", te.sel.current())
	}
	te.release("auto", at(2*time.Second))
	jobs, wake := te.tick(at(3 * time.Second))
	if len(te.spacedUntil) != 1 || len(jobs) != 0 || !wake.After(at(3*time.Second)) || !wake.Equal(te.spacedUntil[0]) || te.sel.current() != "a1" {
		t.Fatalf("audit budget: jobs %+v, wake %v, slots at %v, active %q", jobs, wake.Sub(t0), te.spacedUntil, te.sel.current())
	}
	jobs = te.run(t, wake, map[string]bool{"a1": false})
	if !slices.Equal(reasons(jobs), []string{"failover:b1"}) {
		t.Fatalf("after the audit budget: probes %v, want failover:b1", reasons(jobs))
	}
	if te.sel.current() != "b1" {
		t.Fatalf("after release active %q, want b1", te.sel.current())
	}
}

func TestStartPinReleasesItselfAfterItsTime(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), StartPinnedOutbound: "a1", StartPinTimeout: int64(30 * time.Second)}
	te := newTestEngine(t, nil, sc, "a1", "b1")
	te.failN(3, "a1", failDial, at(time.Second))
	te.run(t, at(10*time.Second), nil)
	if te.sel.current() != "a1" || te.sel.pinned != "a1" {
		t.Fatalf("before its time the pin moved to %q (pinned %q)", te.sel.current(), te.sel.pinned)
	}
	if _, wake := te.tick(at(11 * time.Second)); wake.IsZero() || wake.After(at(30*time.Second)) {
		t.Fatalf("no wake-up planned for the end of the pin: %v", wake.Sub(t0))
	}
	te.run(t, at(31*time.Second), nil)
	if te.sel.pinned != "" {
		t.Fatal("the start pin outlived its time")
	}
	te.failN(3, "a1", failDial, at(32*time.Second))
	te.run(t, at(33*time.Second), nil)
	if te.sel.current() != "b1" {
		t.Fatalf("after the pin ended a dead a1 kept the balancer: %q", te.sel.current())
	}
}

func connectivityConfig() *Config {
	return &Config{Connectivity: &ConnectivityConfig{Outbound: "direct", Destinations: []string{"http://x/204"}}}
}

func TestConnectivityFailurePausesDecisions(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDialTimeout, at(0))
	jobs, _ := te.tick(at(time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConnectivity {
		t.Fatalf("want connectivity check first, got %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false, err: "down"}, at(2*time.Second))
	if !te.networkDown || te.sel.current() != "a1" || te.path("a1").status == statusDown {
		t.Fatalf("network down still changed the path: active %q a1 %v", te.sel.current(), te.path("a1").status)
	}
	// More failures while the network is down are ignored.
	te.failN(5, "a1", failDial, at(3*time.Second))
	if p := te.path("a1"); len(p.failures) != 0 || p.status == statusSuspect || p.status == statusDown {
		t.Fatalf("failures counted while the network was down: %d, a1 %v", len(p.failures), p.status)
	}
}

func TestUnreachableConnectivityCheckIsIgnoredAfterRepeatedFailures(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	now := at(time.Second)
	for i := 0; i < 10 && te.sel.current() == "a1"; i++ {
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			ok := j.reason != reasonConnectivity
			te.onProbeResult(j, probeResult{ok: ok}, now)
		}
		if !te.networkDown && te.path("a1").status != statusDown {
			te.failN(3, "a1", failDial, now)
		}
		now = now.Add(40 * time.Second)
	}
	if te.sel.current() != "b1" {
		t.Fatalf("an unreachable connectivity check kept the balancer on %q", te.sel.current())
	}
}

func TestTrafficSuccessEndsNetworkDown(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(2*time.Second))
	te.onSuccess("a1", at(3*time.Second))
	if te.networkDown {
		t.Fatalf("a working connection did not end the network outage")
	}
}

func TestNetworkProbeSuccessIsNotAFalseAlarm(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	for i := 0; i < 5; i++ {
		base := time.Duration(i) * 10 * time.Minute
		te.failN(3, "a1", failDial, at(base))
		jobs, _ := te.tick(at(base + time.Second))
		for _, j := range jobs {
			te.onProbeResult(j, probeResult{ok: false}, at(base+time.Second))
		}
		jobs, _ = te.tick(at(base + 5*time.Second))
		for _, j := range jobs {
			te.onProbeResult(j, probeResult{ok: true}, at(base+5*time.Second))
		}
		te.run(t, at(base+6*time.Second), nil)
	}
	if fa := te.path("a1").falseAlarms; fa != 0 {
		t.Fatalf("network recoveries counted as %d false alarms", fa)
	}
}

// second plays one second of a user who keeps trying: the network check
// passes only with network, probes pass for outbounds in works, and the
// chosen outbound fails a dial unless it works. It returns the path probes
// that started.
func (te *testEngine) second(now time.Time, network bool, works map[string]bool) []*probeJob {
	te.noteDemand(now)
	var started []*probeJob
	jobs, _ := te.tick(now)
	for _, j := range jobs {
		ok := network
		if j.reason != reasonConnectivity {
			ok = works[j.tag]
			started = append(started, j)
		}
		te.onProbeResult(j, probeResult{ok: ok}, now)
	}
	if cur := te.sel.current(); cur != "" {
		if works[cur] {
			te.onSuccess(cur, now)
		} else {
			te.fail(cur, failDialTimeout, "", now)
		}
	}
	return started
}

func countEvents(types []string, typ string) int {
	n := 0
	for _, t := range types {
		if t == typ {
			n++
		}
	}
	return n
}

func TestShortOutageKeepsThePath(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	var probes []*probeJob
	for s := 0; s < 40; s++ {
		probes = append(probes, te.second(at(time.Duration(s)*time.Second), false, nil)...)
	}
	all := map[string]bool{"a1": true, "a2": true, "b1": true}
	for s := 40; s < 50; s++ {
		te.second(at(time.Duration(s)*time.Second), true, all)
	}
	if len(probes) != 0 {
		t.Fatalf("%d outbounds probed while there was no network", len(probes))
	}
	if te.sel.current() != "a1" || te.path("a1").status == statusDown {
		t.Fatalf("after a 40 s outage active %q a1 %v, want a1", te.sel.current(), te.path("a1").status)
	}
	if n := countEvents(te.eventTypes(), "switch"); n != 1 {
		t.Fatalf("%d switches, want only the start", n)
	}
	if te.connectivityState() != "ok" {
		t.Fatalf("connectivity %q after the network came back", te.connectivityState())
	}
}

func TestLongOutageRulesOutboundsOutProvisionally(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	var probes []*probeJob
	for s := 0; s < 180; s++ {
		probes = append(probes, te.second(at(time.Duration(s)*time.Second), false, nil)...)
	}
	evs := te.eventTypes()
	if countEvents(evs, "connectivity_doubt") != 1 || !te.sel.exhausted {
		t.Fatalf("after 3 minutes without network: doubt events %d, exhausted %v", countEvents(evs, "connectivity_doubt"), te.sel.exhausted)
	}
	if p := te.path("a1"); !p.provisional || p.quarantineLevel != 0 {
		t.Fatalf("a1 ruled out during an outage: provisional %v, quarantine level %d", p.provisional, p.quarantineLevel)
	}
	if len(probes) > 6 {
		t.Fatalf("%d probes during the outage", len(probes))
	}
	all := map[string]bool{"a1": true, "a2": true, "b1": true}
	back := -1
	for s := 180; s < 240 && back < 0; s++ {
		te.second(at(time.Duration(s)*time.Second), true, all)
		if te.sel.current() == "a1" {
			back = s - 180
		}
	}
	if back < 0 || back > 31 {
		t.Fatalf("a1 chosen %d s after the network came back, want within the 30 s recheck", back)
	}
	for _, tag := range []string{"a1", "a2", "b1"} {
		if p := te.path(tag); p.provisional || p.status == statusDown {
			t.Fatalf("%s still ruled out after the check passed: %v provisional %v", tag, p.status, p.provisional)
		}
	}
}

func TestWorkingOutboundProvesTheCheckWrong(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	works := map[string]bool{"b1": true}
	for s := 0; s < 120 && te.connectivityState() != "ignored"; s++ {
		te.second(at(time.Duration(s)*time.Second), false, works)
	}
	if te.connectivityState() != "ignored" || te.sel.current() != "b1" {
		t.Fatalf("connectivity %q active %q, want ignored and b1", te.connectivityState(), te.sel.current())
	}
	if p := te.path("a1"); p.status != statusDown || p.provisional || p.quarantineLevel != 1 {
		t.Fatalf("a1 %v provisional %v level %d, want down for real", p.status, p.provisional, p.quarantineLevel)
	}
}

func TestNoPauseStillRulesOutADeadOutbound(t *testing.T) {
	cfg := connectivityConfig()
	cfg.Connectivity.MaxPause = 1
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	works := map[string]bool{"b1": true}
	for s := 0; s < 30 && te.sel.current() != "b1"; s++ {
		te.second(at(time.Duration(s)*time.Second), false, works)
	}
	if te.sel.current() != "b1" {
		t.Fatalf("without a pause the dead a1 stayed chosen: %q", te.sel.current())
	}
}

func TestWitnessTrafficProvesTheCheckWrong(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	works := map[string]bool{"b1": true}
	switched := -1
	for s := 0; s < 60 && switched < 0; s++ {
		now := at(time.Duration(s) * time.Second)
		te.sawSuccess("witness", now)
		te.second(now, false, works)
		if te.sel.current() == "b1" {
			switched = s
		}
	}
	if switched < 0 || switched > 15 {
		t.Fatalf("switched to b1 after %d s with direct traffic working, want within 15 s", switched)
	}
	if te.connectivityState() != "ignored" {
		t.Fatalf("connectivity %q, want ignored", te.connectivityState())
	}
}

func TestStaleProbeResultIsIgnored(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	suspectWeak(te, "a1", 0)
	jobs, _ := te.tick(at(3 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("want a confirming probe, got %+v", jobs)
	}
	// The probe fails, but it was made in the previous network.
	te.onNetworkChange("other", at(4*time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(5*time.Second))
	if te.path("a1").status == statusDown || te.sel.current() != "a1" {
		t.Fatalf("result from the previous network took a1 down")
	}
}

func TestGCForgetsOutboundsNoLongerOffered(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1", "a2")
	te.updateCandidates(te.sel, []string{"a1"}, at(time.Second))
	te.tick(at(time.Hour))
	if _, ok := te.paths["a2"]; ok {
		t.Fatalf("a2 is no longer offered but kept")
	}
	if _, ok := te.paths["a1"]; !ok {
		t.Fatalf("a1 is still offered but forgotten")
	}
}

func TestEventLogLimits(t *testing.T) {
	l := newEventLog(3, 1<<20)
	for i := 0; i < 5; i++ {
		l.add(t0, "x", map[string]any{"i": i})
	}
	raw, dropped := l.drain()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 || dropped != 2 || evs[0].Seq != 3 {
		t.Fatalf("kept %d dropped %d first seq %d", len(evs), dropped, evs[0].Seq)
	}
	if raw, _ := l.drain(); string(raw) != "[]" {
		t.Fatalf("drain did not empty the log: %s", raw)
	}
}

func TestEventLogByteLimitAndDefaults(t *testing.T) {
	l := newEventLog(1000, 300)
	for i := 0; i < 10; i++ {
		l.add(t0, "x", map[string]any{"i": i})
	}
	raw, dropped := l.drain()
	if len(raw) > 300+2+10 || dropped == 0 {
		t.Fatalf("%d bytes kept, %d dropped, want at most about 300 bytes", len(raw), dropped)
	}
	s := newSettings(&Config{Features: &FeatureConfig{Events: v2Bool(true)}})
	if s.maxEvents != 1000 || s.maxBytes != 64*1024 {
		t.Fatalf("event limits by default %d events, %d bytes", s.maxEvents, s.maxBytes)
	}
}

func TestQuarantineGrowsToItsCeiling(t *testing.T) {
	q := newSettings(&Config{}).schedule.quarantine
	var got []time.Duration
	for i := 0; i < 6; i++ {
		got = append(got, q.delay(i))
	}
	want := []time.Duration{2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 30 * time.Minute, 30 * time.Minute}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("quarantine by attempt %v, want %v", got, want)
		}
	}
	if q.jitter == 0 {
		t.Fatal("quarantine has no jitter: every client would recheck at the same moment")
	}
}

func TestHistogramQuantiles(t *testing.T) {
	var h histogram
	if h.quantile(0.5) != -1 {
		t.Fatal("empty histogram")
	}
	for i := 0; i < 90; i++ {
		h.add(80 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		h.add(4 * time.Second)
	}
	if h.quantile(0.5) != 100*time.Millisecond || h.quantile(0.95) != 5*time.Second {
		t.Fatalf("p50 %v p95 %v", h.quantile(0.5), h.quantile(0.95))
	}
}

func TestStatusIsJSON(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	if _, err := json.Marshal(te.status(at(time.Second))); err != nil {
		t.Fatal(err)
	}
}

func TestProtoZeroValuesMatchJSONDefaults(t *testing.T) {
	s := newSelectorSettings(&SelectorConfig{})
	if !s.switchAfterProbe || !s.returnToCheaper.randomPhase || s.returnToCheaper.onNetworkChange || !s.returnToCheaper.unknownNetworksOnly || s.returnToCheaper.interval != 0 {
		t.Fatalf("zero config: %+v", s)
	}
	if newSettings(&Config{}).remember {
		t.Fatalf("zero config remembers networks without its flag")
	}
}

// The recheck of an exhausted balancer ranks its outbounds as if every
// quarantine were over; an outbound it is the first to look at still dates
// from now.
func TestRecheckExhaustedNotesTheRealTime(t *testing.T) {
	for _, tc := range []struct {
		name string
		sc   *SelectorConfig
	}{
		{"plain", &SelectorConfig{}},
		{"groups", groupConfig()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := newTestEngine(t, nil, tc.sc, "a1", "a2", "b1")
			te.sel.exhausted = true
			te.path("b1").created = time.Time{}
			now := at(time.Minute)
			te.recheckExhausted(te.sel, now)
			if got := te.path("b1").created; !got.Equal(now) {
				t.Fatalf("created %v, want %v", got, now)
			}
		})
	}
}
