package health

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"
)

// parallelConfig allows n probes at a time; an outbound found down rests a minute.
func parallelConfig(n int) *Config {
	return &Config{Schedule: &ScheduleConfig{Concurrent: int32(n), Quarantine: &Backoff{Initial: int64(time.Minute)},
		PauseWhenIdle: int64(time.Hour)}}
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
	te := newTestEngine(t, parallelConfig(concurrent), &SelectorConfig{Costs: costs("a", 1, "b", 3)}, "a1", "a2", "b1")
	te.serverNames(map[string]string{"a1": "a1.example", "a2": "a2.example", "b1": "b1.example"})
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	return te, jobs
}

func TestFailoverProbesCandidatesOfDifferentServerNamesTogether(t *testing.T) {
	te := newTestEngine(t, parallelConfig(2), &SelectorConfig{Costs: costs("a", 1, "b", 3, "c", 5)}, "a1", "a2", "b1", "c1")
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
	cfg.Schedule.PerServerNamePerMinute = 10
	te := newTestEngine(t, cfg, &SelectorConfig{Costs: costs("a", 1, "b", 3)}, "a1", "a2", "a3", "b1")
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
	if jobs, _ := te.tick(at(5 * time.Second)); len(jobs) != 0 {
		t.Fatalf("probed %v after the switch", jobTags(jobs))
	}
}

// One probe at a time keeps the failover as it was: candidates one by one in
// order of cost, each after the previous one answered.
func TestOneProbeAtATimeTriesCandidatesInTurn(t *testing.T) {
	te, jobs := failoverOfThree(t, 1)
	if got := jobTags(jobs); !slices.Equal(got, []string{"a2"}) {
		t.Fatalf("first probes %v, want a2 alone", got)
	}
	starts := []string{"a2@1s"}
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
			starts = append(starts, j.tag+"@"+now.Sub(t0).String())
			answers[j] = now.Add(3 * time.Second)
		}
	}
	if !slices.Equal(starts, []string{"a2@1s", "b1@4s"}) || te.sel.current() != "b1" {
		t.Fatalf("probes %v, active %q; want a2, then b1 once a2 failed", starts, te.sel.current())
	}
}

// With one probe at a time a candidate that waits for the budget of its server
// name is not passed by a dearer one.
func TestOneProbeAtATimeKeepsTheTurnOfACandidateWaitingForItsBudget(t *testing.T) {
	cfg := parallelConfig(1)
	cfg.Schedule.PerServerNamePerMinute = 1
	te := newTestEngine(t, cfg, &SelectorConfig{Costs: costs("a", 1, "b", 3)}, "a1", "a2", "b1")
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
	sc := &SelectorConfig{Costs: costs("a", 1, "b", 3, "c", 50), Lazy: []string{"c"}}
	te := newTestEngine(t, parallelConfig(3), sc, "a1", "a2", "b1", "c1")
	te.serverNames(map[string]string{"a1": "a1.example", "a2": "a2.example", "b1": "b1.example", "c1": "c1.example"})
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	if got := jobTags(jobs); !slices.Equal(got, []string{"a2", "b1"}) {
		t.Fatalf("probes %v, want a2 and b1 without the lazy c1", got)
	}
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
	te := newTestEngine(t, nil, &SelectorConfig{Costs: costs("a", 1, "b", 3)}, "a1", "b1")
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
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, upgradeConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	jobs, _ := te.tick(at(6 * time.Minute))
	if len(jobs) != 1 || jobs[0].reason != reasonUpgrade || jobs[0].tag != "a1" {
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
	cfg.Schedule.PerMinute = 1
	te := newTestEngine(t, cfg, &SelectorConfig{Costs: costs("a", 1, "b", 3)}, "a1", "a2", "b1")
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
	cfg.Schedule.PerMinute = 1
	te := newTestEngine(t, cfg, &SelectorConfig{Costs: costs("a", 1, "b", 3)}, "a1", "a2", "b1")
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
