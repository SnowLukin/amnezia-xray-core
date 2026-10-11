package health

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// parallelConfig allows n probes at a time; an outbound found down rests a minute.
func parallelConfig(n int) *Config {
	return &Config{Schedule: &ScheduleConfig{MaxConcurrentProbes: int32(n), Quarantine: &Backoff{InitialDelay: int64(time.Minute)},
		IdleAfter: int64(time.Hour)}}
}

func (te *testEngine) serverNames(names map[string]string) {
	te.serverNameFor = func(tag string) string {
		if n, ok := names[tag]; ok {
			return n
		}
		return tag
	}
}

func jobTags(jobs []*probeJob) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.tag
	}
	return out
}

func jobOf(t *testing.T, jobs []*probeJob, tag string) *probeJob {
	t.Helper()
	for _, j := range jobs {
		if j.tag == tag {
			return j
		}
	}
	t.Fatalf("no probe of %s among %v", tag, jobTags(jobs))
	return nil
}

// a1 is chosen and dies; a2 is as cheap, b1 dearer, each behind a server name of its own.
func failoverOfThree(t *testing.T, concurrent int) (*testEngine, []*probeJob) {
	t.Helper()
	te := newTestEngine(t, parallelConfig(concurrent), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.serverNames(map[string]string{"a1": "a1.example", "a2": "a2.example", "b1": "b1.example"})
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	return te, jobs
}

func TestFailoverProbesCandidatesOfDifferentServerNamesTogether(t *testing.T) {
	te := newTestEngine(t, parallelConfig(2), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3, "c", 5)}, "a1", "a2", "b1", "c1")
	te.serverNames(map[string]string{"a1": "a1.example", "a2": "a2.example", "b1": "b1.example", "c1": "c1.example"})
	te.failN(3, "a1", failDial, at(0))
	te.events.drain()
	jobs, _ := te.tick(at(time.Second))
	if got := jobTags(jobs); !slices.Equal(got, []string{"a2", "b1"}) {
		t.Fatalf("probes started together %v, want the two cheapest a2 and b1", got)
	}
	raw, _ := te.events.drain()
	var evs []Event
	_ = json.Unmarshal(raw, &evs)
	var running []any
	for _, ev := range evs {
		if ev.Type == "probe_start" {
			running = append(running, ev.Fields["running"])
		}
	}
	if len(running) != 2 || running[0] != float64(1) || running[1] != float64(2) {
		t.Fatalf("probe_start running %v, want 1 then 2", running)
	}
}

// A failover candidate behind the server name of a probe still running waits
// for it, here a confirming probe of the dead outbound itself.
func TestOneServerNameIsNeverProbedTwiceAtOnce(t *testing.T) {
	cfg := parallelConfig(2)
	// Room in the budget of the server name for the probes of the host too.
	cfg.Schedule.ProbesPerServerNamePerMinute = 10
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "a3", "b1")
	te.serverNames(map[string]string{"a1": "r.example", "a2": "r.example", "a3": "r.example", "b1": "b.example"})
	suspectWeak(te, "a1", 0)
	jobs, _ := te.tick(at(3 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("setup: want a confirming probe, got %+v", jobs)
	}
	type pending struct {
		j  *probeJob
		at time.Time
	}
	// The confirming probe hangs until 9 s; the host asks for probes of the
	// balancer, and then strong evidence rules a1 out.
	running := []pending{{jobs[0], at(9 * time.Second)}}
	var probed []string
	for now := at(3500 * time.Millisecond); now.Before(at(time.Minute)); now = now.Add(100 * time.Millisecond) {
		if now.Equal(at(3500 * time.Millisecond)) {
			if _, _, err := te.requestProbe("auto", nil, now); err != nil {
				t.Fatal(err)
			}
		}
		if now.Equal(at(4 * time.Second)) {
			te.failN(3, "a1", failDial, now)
		}
		kept := running[:0]
		for _, p := range running {
			if now.Before(p.at) {
				kept = append(kept, p)
				continue
			}
			te.onProbeResult(p.j, probeResult{ok: p.j.tag == "b1"}, now)
		}
		running = kept
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			for _, p := range running {
				if te.serverName(p.j.tag) == te.serverName(j.tag) {
					t.Fatalf("%s started at %v while %s (%s) of the same server name was probed", j.tag, now.Sub(t0), p.j.tag, p.j.reason)
				}
			}
			running = append(running, pending{j, now.Add(time.Second)})
			probed = append(probed, j.reason.String()+":"+j.tag)
		}
	}
	if te.sel.current() != "b1" || !slices.Contains(probed, "failover:a2") || !slices.Contains(probed, "failover:a3") ||
		!slices.ContainsFunc(probed, func(p string) bool { return strings.HasPrefix(p, "host:") }) {
		t.Fatalf("active %q after probes %v, want b1 once a2 and a3 failed, and a probe for the host", te.sel.current(), probed)
	}
}

func TestCheaperCandidatePassingFirstSwitchesAtOnce(t *testing.T) {
	te, jobs := failoverOfThree(t, 2)
	a2, b1 := jobOf(t, jobs, "a2"), jobOf(t, jobs, "b1")
	te.onProbeResult(a2, probeResult{ok: true}, at(1100*time.Millisecond))
	if te.sel.current() != "a2" {
		t.Fatalf("a2 is the cheapest and passed, active %q", te.sel.current())
	}
	te.events.drain()
	te.onProbeResult(b1, probeResult{ok: false}, at(4*time.Second))
	if n := countEvents(te.eventTypes(), "switch"); te.sel.current() != "a2" || n != 0 {
		t.Fatalf("the late result of b1 moved the balancer to %q (%d switches)", te.sel.current(), n)
	}
	if te.path("b1").status != statusDown {
		t.Fatalf("b1 failed its probe, yet it is %v", te.path("b1").status)
	}
}

func TestDearerCandidatePassingFirstWaitsForTheCheaper(t *testing.T) {
	te, jobs := failoverOfThree(t, 2)
	a2, b1 := jobOf(t, jobs, "a2"), jobOf(t, jobs, "b1")
	te.onProbeResult(b1, probeResult{ok: true}, at(1100*time.Millisecond))
	if te.sel.current() != "a1" {
		t.Fatalf("moved to %q while the cheaper a2 was still probed", te.sel.current())
	}
	te.onProbeResult(a2, probeResult{ok: true}, at(2*time.Second))
	if te.sel.current() != "a2" {
		t.Fatalf("a2 passed too and is cheaper, active %q", te.sel.current())
	}
}

func TestCheaperCandidateFailingTakesTheDearerThatPassed(t *testing.T) {
	te, jobs := failoverOfThree(t, 2)
	a2, b1 := jobOf(t, jobs, "a2"), jobOf(t, jobs, "b1")
	te.onProbeResult(b1, probeResult{ok: true}, at(1100*time.Millisecond))
	te.onProbeResult(a2, probeResult{ok: false}, at(4*time.Second))
	if te.sel.current() != "b1" || !te.sel.activeSince.Equal(at(4*time.Second)) {
		t.Fatalf("active %q since %v, want b1 when a2 failed at 4s", te.sel.current(), te.sel.activeSince.Sub(t0))
	}
	if jobs, _ := te.tick(at(5 * time.Second)); !slices.Equal(reasons(jobs), []string{"audit:a1"}) {
		t.Fatalf("probed %v after the switch, want audit:a1", reasons(jobs))
	} else {
		te.onProbeResult(jobs[0], probeResult{ok: false}, at(5*time.Second))
	}
}

// One probe at a time keeps the failover as it was: candidates one by one in
// order of cost, each after the previous one answered.
func TestOneProbeAtATimeTriesCandidatesInTurn(t *testing.T) {
	te, jobs := failoverOfThree(t, 1)
	if got := jobTags(jobs); !slices.Equal(got, []string{"a2"}) {
		t.Fatalf("first probes %v, want a2 alone", got)
	}
	starts := []string{"failover:a2@1s"}
	answers := map[*probeJob]time.Time{jobs[0]: at(4 * time.Second)}
	for now := at(1100 * time.Millisecond); now.Before(at(20 * time.Second)); now = now.Add(100 * time.Millisecond) {
		for j, due := range answers {
			if !now.Before(due) {
				delete(answers, j)
				te.onProbeResult(j, probeResult{ok: j.tag == "b1"}, now)
			}
		}
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			starts = append(starts, j.reason.String()+":"+j.tag+"@"+now.Sub(t0).String())
			answers[j] = now.Add(3 * time.Second)
		}
	}
	if !slices.Equal(starts, []string{"failover:a2@1s", "failover:b1@4s", "audit:a1@7s"}) || te.sel.current() != "b1" {
		t.Fatalf("probes %v, active %q; want a2, then b1 once a2 failed", starts, te.sel.current())
	}
}

// With one probe at a time a candidate that waits for the budget of its server
// name is not passed by a dearer one.
func TestOneProbeAtATimeKeepsTheTurnOfACandidateWaitingForItsBudget(t *testing.T) {
	cfg := parallelConfig(1)
	cfg.Schedule.ProbesPerServerNamePerMinute = 1
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.serverNames(map[string]string{"a1": "r.example", "a2": "r.example", "b1": "b1.example"})
	suspectWeak(te, "a1", 0)
	jobs, _ := te.tick(at(3 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("setup: want a confirming probe, got %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(4*time.Second))
	var starts []string
	for now := at(4 * time.Second); now.Before(at(70*time.Second)) && te.sel.current() == "a1"; now = now.Add(time.Second) {
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			starts = append(starts, j.tag+"@"+now.Sub(t0).String())
			te.onProbeResult(j, probeResult{ok: true}, now)
		}
	}
	if !slices.Equal(starts, []string{"a2@1m3s"}) || te.sel.current() != "a2" {
		t.Fatalf("probes %v, active %q; want a2 once its server name may be probed again", starts, te.sel.current())
	}
}

func TestLazyCandidateWaitsForEverythingCheaperInParallel(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3, "c", 50)}
	sc.Paths[2].IsLazy = true
	te := newTestEngine(t, parallelConfig(3), sc, "a1", "a2", "b1", "c1")
	te.serverNames(map[string]string{"a1": "a1.example", "a2": "a2.example", "b1": "b1.example", "c1": "c1.example"})
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	if got := reasons(jobs); !slices.Equal(got, []string{"failover:a2", "failover:b1", "audit:a1"}) {
		t.Fatalf("probes %v, want a2 and b1 without the lazy c1", got)
	}
	te.onProbeResult(jobs[2], probeResult{ok: false}, at(2*time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(4*time.Second))
	if more, _ := te.tick(at(4 * time.Second)); len(more) != 0 {
		t.Fatalf("probed %v while b1, cheaper than the lazy c1, was still probed", jobTags(more))
	}
	te.onProbeResult(jobs[1], probeResult{ok: false}, at(4*time.Second))
	if more, _ := te.tick(at(4 * time.Second)); !slices.Equal(jobTags(more), []string{"c1"}) {
		t.Fatalf("probes %v once everything cheaper failed, want c1", jobTags(more))
	}
}

// The host pins the outbound the balancer is leaving while a candidate is
// probed: the answer of that probe does not move the balancer off the pin.
func TestPinDuringFailoverProbeHolds(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	if len(jobs) != 1 || jobs[0].tag != "b1" {
		t.Fatalf("setup: %+v", jobs)
	}
	te.pin("auto", "a1", at(2*time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(3*time.Second))
	if te.sel.current() != "a1" || te.sel.pinned != "a1" {
		t.Fatalf("pinned to a1, yet the balancer is on %q (pinned %q)", te.sel.current(), te.sel.pinned)
	}
}

// The host pins the outbound the balancer is on while a return to a cheaper
// one is being probed: the probes that pass do not move it off the pin.
func TestPinDuringReturnProbeHolds(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	jobs, _ := te.tick(at(6 * time.Minute))
	if len(jobs) != 1 || jobs[0].reason != reasonReturnToCheaper || jobs[0].tag != "a1" {
		t.Fatalf("setup: probes %+v, want the return to a1", jobs)
	}
	te.pin("auto", "b1", at(6*time.Minute+time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(6*time.Minute+2*time.Second))
	for now := at(6 * time.Minute); now.Before(at(10 * time.Minute)); now = now.Add(5 * time.Second) {
		te.run(t, now, nil)
	}
	if te.sel.current() != "b1" || te.sel.pinned != "b1" {
		t.Fatalf("pinned to b1, yet the balancer is on %q (pinned %q)", te.sel.current(), te.sel.pinned)
	}
}

// A pin drops the failover probes still waiting for their turn.
func TestPinDropsWaitingFailoverProbes(t *testing.T) {
	cfg := parallelConfig(2)
	cfg.Schedule.ProbesPerMinute = 1
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.serverNames(map[string]string{"a1": "a1.example", "a2": "a2.example", "b1": "b1.example"})
	te.failN(3, "a1", failDial, at(0))
	if jobs, _ := te.tick(at(time.Second)); !slices.Equal(jobTags(jobs), []string{"a2"}) {
		t.Fatalf("setup: probes %v, want a2 alone within one probe a minute", jobTags(jobs))
	}
	te.pin("auto", "a1", at(2*time.Second))
	if later, _ := te.tick(at(2 * time.Minute)); len(later) != 0 {
		t.Fatalf("probed %v for a failover after the pin", jobTags(later))
	}
}

// A candidate still waiting for the probe budget when the balancer has moved
// is not probed any more.
func TestWaitingCandidateIsDroppedAfterTheSwitch(t *testing.T) {
	cfg := parallelConfig(2)
	cfg.Schedule.ProbesPerMinute = 1
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.serverNames(map[string]string{"a1": "a1.example", "a2": "a2.example", "b1": "b1.example"})
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	if got := jobTags(jobs); !slices.Equal(got, []string{"a2"}) {
		t.Fatalf("setup: probes %v, want a2 alone within one probe a minute", got)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(1100*time.Millisecond))
	if te.sel.current() != "a2" {
		t.Fatalf("setup: active %q", te.sel.current())
	}
	if later, _ := te.tick(at(2 * time.Minute)); len(later) != 0 {
		t.Fatalf("probed %v after the balancer moved to a2", jobTags(later))
	}
}

// confirmRunning brings a1 under weak suspicion as a phone on Wi-Fi under
// load saw it: a dial that timed out and two requests without an answer, at
// two destinations. The network check, if any, passes, and the confirming probe
// of a1 starts; it is returned still running.
func confirmRunning(t *testing.T, te *testEngine) *probeJob {
	t.Helper()
	te.fail("a1", failDialTimeout, "", at(0))
	te.fail("a1", failNoResponse, "x.test", at(time.Second))
	te.fail("a1", failNoResponse, "y.test", at(2*time.Second))
	if te.path("a1").status != statusSuspect {
		t.Fatalf("setup: a1 is %v, want suspect", te.path("a1").status)
	}
	for now := at(2100 * time.Millisecond); now.Before(at(5 * time.Second)); now = now.Add(100 * time.Millisecond) {
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			switch {
			case j.reason == reasonConnectivity:
				te.onProbeResult(j, probeResult{ok: true}, now)
			case j.reason == reasonConfirm && j.tag == "a1":
				return j
			default:
				t.Fatalf("setup: %s probe of %s started", j.reason, j.tag)
			}
		}
	}
	t.Fatal("setup: no confirming probe of a1 started")
	return nil
}

// strongDown fails two more dials of a1 at now: with the one before, failures
// about the path that rule it out without waiting for the probe.
func strongDown(t *testing.T, te *testEngine, now time.Time) {
	t.Helper()
	te.fail("a1", failDialTimeout, "", now)
	te.fail("a1", failDialTimeout, "", now)
	if p := te.path("a1"); p.status != statusDown || !p.quarantined(now) {
		t.Fatalf("setup: a1 is %v until %v, want down in quarantine", p.status, p.quarantineUntil.Sub(now))
	}
}

// brief renders the events that tell the story of a path.
func brief(t *testing.T, te *testEngine) []string {
	t.Helper()
	raw, _ := te.events.drain()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, ev := range evs {
		f := ev.Fields
		switch ev.Type {
		case "path_state":
			out = append(out, fmt.Sprintf("%v %v %v", f["outbound"], f["state"], f["reason"]))
		case "probe_start":
			out = append(out, fmt.Sprintf("start %v %v", f["reason"], f["outbound"]))
		case "probe_result":
			out = append(out, fmt.Sprintf("result %v %v %v", f["reason"], f["outbound"], f["is_ok"]))
		case "connectivity":
			out = append(out, fmt.Sprintf("connectivity %v", f["is_ok"]))
		case "switch":
			out = append(out, fmt.Sprintf("switch %v>%v %v", f["from"], f["to"], f["reason"]))
		case "exhausted", "pinned_down", "return_penalty", "return_delayed", "return_start":
			out = append(out, ev.Type)
		}
	}
	return out
}

// The sequence of the report: weak suspicion, the network check passes and
// the confirming probe starts, a second later failures about the path rule
// a1 out, and the probe passes 0.4 s after that. The down is undone: a1 is
// up with reason rescued, out of quarantine at the level it had, and its
// silent connections stay. The failover started by the down still moves
// the balancer once its candidate passes, and the return to a1 comes soon
// after minHoldTime.
func TestConfirmingProbeThatPassesAfterTheDownRescuesThePath(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	confirm := confirmRunning(t, te)
	decided := te.path("a1").lastProbe.Add(time.Second)
	strongDown(t, te, decided)
	if downs := te.takeDowns(); len(downs) != 1 || !downs[0].pending || !downs[0].watch {
		t.Fatalf("setup: down marks %+v, want the pending and silent marks of a strong decision", downs)
	}
	if jobs, _ := te.tick(decided.Add(200 * time.Millisecond)); len(jobs) != 0 {
		t.Fatalf("probed %v while the confirming probe ran with one probe at a time", jobTags(jobs))
	}
	te.onProbeResult(confirm, probeResult{ok: true}, decided.Add(400*time.Millisecond))
	p := te.path("a1")
	if p.status != statusUp || p.reason != "rescued" || !p.quarantineUntil.IsZero() || p.quarantineLevel != 0 ||
		p.rescue != nil {
		t.Fatalf("a1 %v (%s) until %v at level %d, want up, rescued, no quarantine, level 0",
			p.status, p.reason, p.quarantineUntil.Sub(t0), p.quarantineLevel)
	}
	if len(te.silences) != 0 {
		t.Fatalf("silence checks left after the rescue: %+v", te.silences)
	}
	if te.sel.current() != "a1" {
		t.Fatalf("the rescue moved the balancer to %q while its failover was under way", te.sel.current())
	}
	te.run(t, decided.Add(3*time.Second), nil)
	switched := te.sel.activeSince
	if te.sel.current() != "b1" {
		t.Fatalf("the failover the down started ended on %q, want b1", te.sel.current())
	}
	want := []string{
		"a1 suspect traffic", "connectivity true", "start confirm a1",
		"a1 down traffic", "result confirm a1 true", "a1 up rescued",
		"start failover b1", "result failover b1 true", "b1 up probe", "switch a1>b1 failover",
	}
	if got := brief(t, te); !slices.Equal(got[1:], want) {
		t.Fatalf("events\n%v\nwant after the start\n%v", got, want)
	}
	// Leaving a1 ends nothing: it is no longer down, and the check of its
	// silent connections is gone.
	for now := switched; now.Before(decided.Add(time.Minute)); now = now.Add(time.Second) {
		te.step(now)
		if downs := te.takeDowns(); len(downs) != 0 {
			t.Fatalf("at %v connections of a1 were marked to end: %+v", now.Sub(t0), downs)
		}
	}
	hold, spread := te.sel.cfg.minHoldTime, min(te.sel.cfg.returnToCheaper.interval, returnSpread)
	if wait := te.sel.returnToCheaperAt.Sub(switched); wait < hold || wait > hold+spread {
		t.Fatalf("return %v after the failover, want within %v after minHoldTime %v", wait, spread, hold)
	}
}

// A confirming probe that was not running when the outbound went down is no
// proof against the down, even when it passes: the one waiting for its turn
// is dropped, and one that starts later changes nothing.
func TestProbeStartedAfterTheDownRescuesNothing(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	suspectWeak(te, "a1", 0)
	if !te.queued("a1", reasonConfirm) {
		t.Fatal("setup: no confirming probe queued")
	}
	te.failN(3, "a1", failDial, at(2500*time.Millisecond))
	if p := te.path("a1"); p.status != statusDown || p.rescue == nil || p.rescue.by != "audit" || te.queued("a1", reasonConfirm) {
		t.Fatalf("a1 %v, mark %+v, confirming probe still queued %v", p.status, p.rescue, te.queued("a1", reasonConfirm))
	}
	late := te.enqueue("a1", reasonConfirm, "", at(3*time.Second))
	jobs, _ := te.tick(at(3 * time.Second))
	if !slices.Contains(jobs, late) {
		t.Fatalf("setup: the late probe did not start, %v did", jobTags(jobs))
	}
	te.events.drain()
	te.onProbeResult(late, probeResult{ok: true}, at(4*time.Second))
	if p := te.path("a1"); p.status != statusDown || !p.quarantined(at(4*time.Second)) || p.quarantineLevel != 1 {
		t.Fatalf("a1 %v at level %d after a probe that started after the down, want down at level 1", p.status, p.quarantineLevel)
	}
	if got := brief(t, te); slices.Contains(got, "a1 up rescued") {
		t.Fatalf("events %v", got)
	}
}

// A confirming probe that fails after the down only agrees with it.
func TestFailedConfirmingProbeAfterTheDownRescuesNothing(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	confirm := confirmRunning(t, te)
	strongDown(t, te, at(3*time.Second))
	until := te.path("a1").quarantineUntil
	te.onProbeResult(confirm, probeResult{ok: false, err: "timeout", class: "timeout"}, at(4*time.Second))
	p := te.path("a1")
	if p.status != statusDown || p.reason != "traffic" || !p.quarantineUntil.Equal(until) || p.quarantineLevel != 1 ||
		p.rescue != nil {
		t.Fatalf("a1 %v (%s) until %v at level %d, want the down of traffic as it was", p.status, p.reason,
			p.quarantineUntil.Sub(t0), p.quarantineLevel)
	}
	if len(te.silences) != 1 {
		t.Fatalf("silence checks %+v, want the one of the decision", te.silences)
	}
}

// A confirming probe made in the previous network proves nothing about this
// one, even when it passes after the outbound went down there.
func TestConfirmingProbeOfThePreviousNetworkRescuesNothing(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	confirm := confirmRunning(t, te)
	strongDown(t, te, at(3*time.Second))
	te.onNetworkChange("lte", at(3500*time.Millisecond))
	te.events.drain()
	te.onProbeResult(confirm, probeResult{ok: true}, at(4*time.Second))
	if p := te.path("a1"); p.status != statusUnknown || p.rescue != nil {
		t.Fatalf("a1 %v (%s) after a stale pass, want unknown as the network change left it", p.status, p.reason)
	}
	if got := brief(t, te); !slices.Equal(got, []string{"result confirm a1 true"}) {
		t.Fatalf("events %v, want only the stale result", got)
	}
}

// A false down does not grow the next quarantine: the rescue puts the level
// back where it was, and the next real down rests as long as this one did.
func TestRescueRestoresTheQuarantineLevel(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	// a1 rested twice before and was chosen again; nothing proved it since.
	te.path("a1").quarantineLevel = 2
	confirm := confirmRunning(t, te)
	strongDown(t, te, at(3*time.Second))
	if l := te.path("a1").quarantineLevel; l != 3 {
		t.Fatalf("setup: level %d after the down, want 3", l)
	}
	te.onProbeResult(confirm, probeResult{ok: true}, at(4*time.Second))
	if l := te.path("a1").quarantineLevel; l != 2 {
		t.Fatalf("level %d after the rescue, want the 2 it had before the down", l)
	}
	te.failN(3, "a1", failDial, at(10*time.Second))
	rest := te.path("a1").quarantineUntil.Sub(at(10 * time.Second))
	q := te.s.schedule.quarantine
	if lo, hi := q.delay(2)*7/10, q.delay(2)*13/10; rest < lo || rest > hi || te.path("a1").quarantineLevel != 3 {
		t.Fatalf("the next down rests %v at level %d, want %v +-30%% at level 3", rest, te.path("a1").quarantineLevel, q.delay(2))
	}
}

// A balancer that moved away on the false down stays where it moved. It
// returns as any return does: after minHoldTime, at once with a fixed phase,
// and through the returnToCheaper probes, without the penalty the down gave the
// return it had just made to a1.
func TestRescueLeavesTheBalancerWhereItMoved(t *testing.T) {
	sc := returnToCheaperConfig()
	sc.SkipProbeBeforeSwitch = true
	te := newTestEngine(t, nil, sc, "a1", "b1")
	// The balancer has just returned to a1.
	te.sel.returnedTo, te.sel.returnedAt = "a1", t0
	confirm := confirmRunning(t, te)
	strongDown(t, te, at(3*time.Second))
	if te.sel.current() != "b1" || te.sel.penaltyFor("a1") != 2 {
		t.Fatalf("setup: on %q, penalty of a1 %v; want b1 and the penalty of a failed return", te.sel.current(),
			te.sel.penaltyFor("a1"))
	}
	moved := te.sel.activeSince
	te.events.drain()
	te.onProbeResult(confirm, probeResult{ok: true}, at(3400*time.Millisecond))
	if te.sel.current() != "b1" {
		t.Fatalf("the rescue moved the balancer back to %q at once", te.sel.current())
	}
	if te.sel.penaltyFor("a1") != 1 || te.sel.returnedTo != "a1" || !te.sel.returnedAt.Equal(t0) {
		t.Fatalf("penalty of a1 %v, return %q at %v; want none, as before the down", te.sel.penaltyFor("a1"),
			te.sel.returnedTo, te.sel.returnedAt.Sub(t0))
	}
	// The failover planned the return for returnToCheaper.interval later; a1 did
	// not fail, so it waits only for minHoldTime.
	hold, spacing := time.Duration(sc.MinHoldTime), time.Duration(sc.ReturnToCheaper.ProbeInterval)
	var back time.Time
	probes := 0
	for now := at(4 * time.Second); now.Before(at(5*time.Minute)) && back.IsZero(); now = now.Add(time.Second) {
		for _, j := range te.run(t, now, nil) {
			if j.reason != reasonReturnToCheaper {
				continue
			}
			if now.Before(moved.Add(hold)) {
				t.Fatalf("a return probe %v after the move, inside minHoldTime", now.Sub(moved))
			}
			probes++
		}
		if te.sel.current() == "a1" {
			back = now
		}
	}
	got := brief(t, te)
	if back.IsZero() || probes != int(sc.ReturnToCheaper.ProbesToReturn) || slices.Contains(got, "return_delayed") {
		t.Fatalf("back on a1 %v after the move, %d return probes; events %v", back.Sub(moved), probes, got)
	}
	if took := back.Sub(moved); took > hold+3*spacing+10*time.Second {
		t.Fatalf("back on a1 %v after the move, want minHoldTime %v and the probes %v apart", took, hold, spacing)
	}
}

// A balancer the down left with nothing takes the rescued outbound back at
// once, as after a recheck; also while memory is low, when finding a
// working outbound goes on.
func TestRescueBringsAnExhaustedBalancerBack(t *testing.T) {
	for _, low := range []bool{false, true} {
		t.Run(fmt.Sprintf("memory low %v", low), func(t *testing.T) {
			te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
			te.setMemoryLow(low, t0)
			confirm := confirmRunning(t, te)
			strongDown(t, te, at(3*time.Second))
			if !te.sel.exhausted {
				t.Fatalf("setup: the balancer is on %q, want exhausted", te.sel.current())
			}
			te.events.drain()
			te.onProbeResult(confirm, probeResult{ok: true}, at(4*time.Second))
			if te.sel.exhausted || te.sel.current() != "a1" {
				t.Fatalf("exhausted %v on %q after the rescue, want a1", te.sel.exhausted, te.sel.current())
			}
			if got := brief(t, te); !slices.Equal(got, []string{"result confirm a1 true", "a1 up rescued", "switch >a1 recovered"}) {
				t.Fatalf("events %v", got)
			}
		})
	}
}

// What else may hold the outbound when the probe passes: a pin keeps the
// balancer on it, a provisional down is undone like any other, and an
// outbound that left its provider set comes up but is taken by no balancer.
func TestRescueAndWhatHoldsTheOutbound(t *testing.T) {
	t.Run("pinned", func(t *testing.T) {
		te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
		te.pin("auto", "a1", t0)
		confirm := confirmRunning(t, te)
		strongDown(t, te, at(3*time.Second))
		te.onProbeResult(confirm, probeResult{ok: true}, at(4*time.Second))
		if te.path("a1").status != statusUp || te.sel.current() != "a1" || te.sel.pinned != "a1" {
			t.Fatalf("a1 %v, balancer on %q pinned %q", te.path("a1").status, te.sel.current(), te.sel.pinned)
		}
		if got := brief(t, te); !slices.Contains(got, "pinned_down") || countEvents(got, "switch a1>b1 failover") != 0 {
			t.Fatalf("events %v", got)
		}
	})
	t.Run("provisional", func(t *testing.T) {
		te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
		te.doubt = true
		confirm := confirmRunning(t, te)
		strongDown(t, te, at(3*time.Second))
		if p := te.path("a1"); !p.provisional || p.quarantineLevel != 0 {
			t.Fatalf("setup: provisional %v at level %d", p.provisional, p.quarantineLevel)
		}
		te.onProbeResult(confirm, probeResult{ok: true}, at(4*time.Second))
		if p := te.path("a1"); p.status != statusUp || p.provisional || p.quarantineLevel != 0 {
			t.Fatalf("a1 %v provisional %v at level %d, want up for good at level 0", p.status, p.provisional, p.quarantineLevel)
		}
	})
	t.Run("retired", func(t *testing.T) {
		te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
		confirm := confirmRunning(t, te)
		strongDown(t, te, at(3*time.Second))
		if !te.sel.exhausted {
			t.Fatalf("setup: the balancer is on %q, want exhausted", te.sel.current())
		}
		te.retiring["a1"] = true
		te.refreshCandidates(at(3500 * time.Millisecond))
		te.onProbeResult(confirm, probeResult{ok: true}, at(4*time.Second))
		if te.path("a1").status != statusUp || !te.sel.exhausted || te.sel.current() != "" {
			t.Fatalf("a1 %v, balancer on %q exhausted %v; want a1 up and taken by no balancer",
				te.path("a1").status, te.sel.current(), te.sel.exhausted)
		}
	})
}

// Every device that saw one server hiccup rescues it at about the same
// moment, so their returns to it spread after minHoldTime by the random choices
// of each, as timed returns do: when the balancer had moved before the
// rescue and when its failover ends after it. None comes before minHoldTime,
// and with a fixed phase each comes at minHoldTime.
func TestReturnAfterARescueIsSpread(t *testing.T) {
	for _, tc := range []struct {
		name  string
		moved bool
		fixed bool
	}{
		{name: "moved before the rescue", moved: true},
		{name: "failover ended after it"},
		{name: "fixed phase", moved: true, fixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			waits := map[time.Duration]bool{}
			const seeds = 8
			for seed := int64(1); seed <= seeds; seed++ {
				sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), SkipProbeBeforeSwitch: tc.moved,
					ReturnToCheaper: &ReturnToCheaperConfig{SkipRandomPhase: tc.fixed}}
				te := newSeededTestEngine(t, seed, nil, sc, "a1", "b1")
				confirm := confirmRunning(t, te)
				strongDown(t, te, at(3*time.Second))
				te.onProbeResult(confirm, probeResult{ok: true}, at(3400*time.Millisecond))
				if !tc.moved {
					te.run(t, at(6*time.Second), nil)
				}
				if te.sel.current() != "b1" {
					t.Fatalf("seed %d: setup: the balancer is on %q, want b1", seed, te.sel.current())
				}
				hold, spread := te.sel.cfg.minHoldTime, min(te.sel.cfg.returnToCheaper.interval, returnSpread)
				wait := te.sel.returnToCheaperAt.Sub(te.sel.activeSince)
				if wait < hold || wait > hold+spread || tc.fixed && wait != hold {
					t.Fatalf("seed %d: return %v after the move, want within %v after minHoldTime %v", seed, wait, spread, hold)
				}
				waits[wait] = true
			}
			if want := map[bool]int{false: seeds, true: 1}[tc.fixed]; len(waits) != want {
				t.Fatalf("returns of %d seeds at %d distinct times %v, want %d", seeds, len(waits), waits, want)
			}
		})
	}
}
