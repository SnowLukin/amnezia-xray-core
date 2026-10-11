package health

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"
)

func (te *testEngine) ask(t *testing.T, now time.Time, tags ...string) *probeRequest {
	t.Helper()
	id, _, err := te.requestProbe("auto", tags, now)
	if err != nil {
		t.Fatal(err)
	}
	return te.request(t, id)
}

func (te *testEngine) request(t *testing.T, id uint64) *probeRequest {
	t.Helper()
	for _, r := range te.requests {
		if r.id == id {
			return r
		}
	}
	t.Fatalf("request %d is not kept", id)
	return nil
}

// answers lists result/source per outbound of a request, in order.
func answers(r *probeRequest) []string {
	out := []string{}
	for _, it := range r.items {
		out = append(out, it.tag+" "+it.result+"/"+it.source)
	}
	return out
}

func queued(te *testEngine) []string {
	return reasons(te.queue)
}

func drainEvents(te *testEngine) []Event {
	raw, _ := te.events.drain()
	var evs []Event
	_ = json.Unmarshal(raw, &evs)
	return evs
}

func eventsOf(evs []Event, typ string) []Event {
	var out []Event
	for _, e := range evs {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

func hostConfig(hp *HostProbesConfig) *Config {
	return &Config{Schedule: &ScheduleConfig{IdleAfter: int64(time.Hour), Quarantine: &Backoff{InitialDelay: int64(time.Minute)},
		HostProbes: hp}}
}

// A request without outbounds covers the chosen outbound, the usable
// candidates from the cheapest and then the ones in quarantine; lazy ones
// stay out. Quarantine and fresh proof answer without a probe.
func TestHostProbePlan(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3, "c", 5)}
	sc.Paths[2].IsLazy = true
	te := newTestEngine(t, hostConfig(nil), sc, "a1", "a2", "b1", "b2", "c1")
	te.markDown(te.path("a2"), "probe", at(0))
	te.onSuccess("b2", at(0))
	r := te.ask(t, at(time.Second))
	want := []string{"a1 pending/", "b1 pending/", "b2 ok/recent", "a2 down/quarantine"}
	if got := answers(r); !slices.Equal(got, want) {
		t.Fatalf("plan %v, want %v", got, want)
	}
	if got := queued(te); !slices.Equal(got, []string{"host:a1", "host:b1"}) {
		t.Fatalf("queued %v, want host probes of a1 and b1", got)
	}

	te = newTestEngine(t, hostConfig(&HostProbesConfig{MaxOutbounds: 2}), sc, "a1", "a2", "b1", "b2", "c1")
	if got := answers(te.ask(t, at(0))); len(got) != 2 {
		t.Fatalf("plan %v, want two outbounds at most", got)
	}
}

func TestHostProbeRequestChecks(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3, "c", 5)}
	sc.Paths[2].IsLazy = true
	te := newTestEngine(t, hostConfig(&HostProbesConfig{MaxOutbounds: 2}), sc, "a1", "b1", "c1")
	te.addSelector("other", newSelectorSettings(&SelectorConfig{}))
	for _, tc := range []struct {
		balancer string
		tags     []string
		err      string
	}{
		{"nope", nil, "no such balancer"},
		{"other", nil, "no candidates yet"},
		{"auto", []string{"a1", "x1"}, "not a candidate: x1"},
		{"auto", []string{"a1", "b1", "c1"}, "too many outbounds"},
	} {
		if _, _, err := te.requestProbe(tc.balancer, tc.tags, at(0)); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("request %s %v: error %v, want %q", tc.balancer, tc.tags, err, tc.err)
		}
	}
	if len(te.requests) != 0 {
		t.Fatalf("a refused request was kept: %d", len(te.requests))
	}
	// A lazy outbound the host names is probed.
	te.ask(t, at(0), "c1")
	if jobs, _ := te.tick(at(0)); !slices.Equal(reasons(jobs), []string{"host:c1"}) {
		t.Fatalf("probes %v, want the lazy c1 the host asked for", reasons(jobs))
	}
}

// An outbound that proved to work within reuse is answered at once; later,
// or while suspected, it is probed.
func TestHostProbeReusesFreshProof(t *testing.T) {
	te := newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onSuccess("a1", at(0))
	te.events.drain()
	r := te.ask(t, at(10*time.Second), "a1")
	if got := answers(r); !slices.Equal(got, []string{"a1 ok/recent"}) || r.open() || r.items[0].age != 10*time.Second {
		t.Fatalf("answers %v, open %v, age %v", got, r.open(), r.items[0].age)
	}
	if len(te.queue) != 0 || len(eventsOf(drainEvents(te), "probe_request")) != 0 {
		t.Fatalf("a request answered at once queued %v or wrote an event", queued(te))
	}
	r2 := te.ask(t, at(20*time.Second), "a1")
	if r2.id == r.id || !r2.open() || !slices.Equal(queued(te), []string{"host:a1"}) {
		t.Fatalf("request %d after reuse: open %v, queued %v", r2.id, r2.open(), queued(te))
	}
}

// A confirming probe that may start now answers the request: the host
// gets no probe of its own.
func TestHostProbeWaitsForAProbeOfTheObserver(t *testing.T) {
	te := newTestEngine(t, parallelConfig(2), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	suspectWeak(te, "a1", 0)
	r := te.ask(t, at(3*time.Second), "a1")
	jobs := te.run(t, at(3*time.Second), nil)
	if !slices.Equal(reasons(jobs), []string{"confirm:a1"}) || len(te.queue) != 0 || r.open() ||
		r.items[0].reason != reasonConfirm || r.items[0].result != probeOK {
		t.Fatalf("probes %v, queued %v, request open %v, answers %v", reasons(jobs), queued(te), r.open(), answers(r))
	}
}

// A probe of the observer queued later takes over the host probe that has
// not started yet.
func TestHostProbeGivesWayToTheObserver(t *testing.T) {
	te := newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	r := te.ask(t, at(0), "b1")
	te.failN(3, "a1", failDial, at(time.Second))
	jobs := te.run(t, at(2*time.Second), map[string]bool{"a1": false})
	if !slices.Equal(reasons(jobs), []string{"failover:b1", "audit:a1"}) || te.sel.current() != "b1" {
		t.Fatalf("probes %v, active %q, want failover:b1 followed by audit:a1", reasons(jobs), te.sel.current())
	}
	if r.open() || r.items[0].result != probeOK || r.items[0].reason != reasonFailover {
		t.Fatalf("request open %v, answers %v", r.open(), answers(r))
	}
}

// A second request for the balancer joins the open one.
func TestHostProbeRequestsJoin(t *testing.T) {
	te := newTestEngine(t, hostConfig(&HostProbesConfig{MaxOutbounds: 2}), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3, "c", 5)},
		"a1", "b1", "c1")
	r := te.ask(t, at(0), "a1")
	id, tags, err := te.requestProbe("auto", []string{"b1", "a1"}, at(time.Second))
	if err != nil || id != r.id || !slices.Equal(tags, []string{"a1", "b1"}) {
		t.Fatalf("second request %d %v %v, want %d [a1 b1]", id, tags, err, r.id)
	}
	// The joined request stays within maxOutbounds.
	if id, tags, _ := te.requestProbe("auto", []string{"c1"}, at(2*time.Second)); id != r.id || len(tags) != 2 {
		t.Fatalf("third request %d %v", id, tags)
	}
}

// A host probe does not start while a probe of the observer waits for the
// budget, even when the budget would let the host probe through.
func TestHostProbeWaitsBehindTheObserver(t *testing.T) {
	cfg := parallelConfig(2)
	cfg.Schedule.ProbesPerServerNamePerMinute = 1
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "c", 5)}, "a1", "a2", "c1")
	te.serverNames(map[string]string{"a1": "r.example", "a2": "r.example", "c1": "c.example"})
	te.startsByName["r.example"] = []time.Time{at(0)}
	te.enqueue("a2", reasonNetwork, "auto", at(time.Second))
	te.ask(t, at(time.Second), "c1")
	if jobs, _ := te.tick(at(time.Second)); len(jobs) != 0 {
		t.Fatalf("probes %v while the probe of a2 waits for its server name", reasons(jobs))
	}
	if jobs, _ := te.tick(at(61 * time.Second)); !slices.Equal(reasons(jobs), []string{"network_change:a2", "host:c1"}) {
		t.Fatalf("probes %v once the budget allows", reasons(jobs))
	}
}

// Host probes start at most hostProbes.perMinute a minute, and at most half
// of the probes of one server name.
func TestHostProbeBudget(t *testing.T) {
	te := newTestEngine(t, hostConfig(&HostProbesConfig{ProbesPerMinute: 2}), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 1, "c", 1, "d", 1)},
		"a1", "b1", "c1", "d1")
	te.ask(t, at(0), "a1", "b1", "c1", "d1")
	var started []time.Duration
	for now := at(0); now.Before(at(2 * time.Minute)); now = now.Add(time.Second) {
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			started = append(started, now.Sub(t0))
			te.onProbeResult(j, probeResult{ok: true}, now)
		}
	}
	if len(started) != 4 || started[1] >= time.Minute || started[2] < time.Minute {
		t.Fatalf("host probes started at %v, want two in the first minute and the rest after it", started)
	}

	te = newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1", "a2")
	te.serverNames(map[string]string{"a1": "r.example", "a2": "r.example"})
	te.ask(t, at(0), "a1", "a2")
	started = nil
	for now := at(0); now.Before(at(2 * time.Minute)); now = now.Add(time.Second) {
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			started = append(started, now.Sub(t0))
			te.onProbeResult(j, probeResult{ok: true}, now)
		}
	}
	// probesPerServerNamePerMinute is 3: the host gets one of them.
	if len(started) != 2 || started[1] < time.Minute {
		t.Fatalf("host probes of one server name started at %v, want a minute apart", started)
	}
}

// Without user traffic host probes still run; without a network they wait.
func TestHostProbeIgnoresIdleButNotTheNetwork(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	now := at(10 * time.Minute)
	if !te.idle(now) {
		t.Fatal("setup: not idle")
	}
	te.ask(t, now, "b1")
	jobs, _ := te.tick(now)
	if !slices.Equal(reasons(jobs), []string{"host:b1"}) {
		t.Fatalf("probes %v without user traffic, want the host probe", reasons(jobs))
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, now)
	te.onNetworkLost(now)
	te.ask(t, now.Add(time.Minute), "a1")
	if jobs, _ := te.tick(now.Add(time.Minute)); len(jobs) != 0 {
		t.Fatalf("probes %v without a network", reasons(jobs))
	}
}

// A failed host probe of the chosen outbound is evidence: the outbound is
// ruled out and the balancer moves away.
func TestHostProbeFailureFailsOver(t *testing.T) {
	te := newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	r := te.ask(t, at(0))
	te.run(t, at(time.Second), map[string]bool{"a1": false})
	if te.sel.current() != "b1" {
		t.Fatalf("active %q after the host probe of a1 failed, want b1", te.sel.current())
	}
	want := []string{"a1 failed/probe", "b1 ok/probe"}
	if got := answers(r); !slices.Equal(got, want) || r.open() || r.end != "done" || r.items[1].reason != reasonFailover {
		t.Fatalf("answers %v, open %v, end %q", got, r.open(), r.end)
	}
	evs := drainEvents(te)
	down := false
	for _, e := range eventsOf(evs, "path_state") {
		down = down || (e.Fields["outbound"] == "a1" && e.Fields["state"] == "down" && e.Fields["reason"] == "host_probe")
	}
	var hostResults []map[string]any
	for _, e := range eventsOf(evs, "probe_result") {
		if e.Fields["reason"] == "host" {
			hostResults = append(hostResults, e.Fields)
		}
	}
	reqs := eventsOf(evs, "probe_request")
	if !down || len(hostResults) != 1 || hostResults[0]["request"] != float64(r.id) || len(reqs) != 1 {
		t.Fatalf("a1 down by host_probe %v, host probe results %v, probe_request events %d", down, hostResults, len(reqs))
	}
	f := reqs[0].Fields
	if f["request"] != float64(r.id) || f["end"] != "done" || f["outbounds"] != 2.0 || f["probed"] != 2.0 ||
		f["ok"] != 1.0 || f["failed"] != 1.0 || f["reused"] != 0.0 || f["expired"] != 0.0 {
		t.Fatalf("probe_request %v", f)
	}
}

// A pinned balancer stays on its outbound.
func TestHostProbeFailureKeepsThePin(t *testing.T) {
	te := newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.pin("auto", "a1", at(0))
	te.ask(t, at(0), "a1")
	te.run(t, at(time.Second), map[string]bool{"a1": false})
	if te.sel.current() != "a1" || countEvents(te.eventTypes(), "pinned_down") != 1 {
		t.Fatalf("active %q, want a1 with pinned_down", te.sel.current())
	}
}

// A host probe that passes while the outbound is suspected is a false alarm.
func TestHostProbeClearsASuspicion(t *testing.T) {
	cfg := hostConfig(nil)
	cfg.Schedule.SuspectCooldown = int64(time.Minute)
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.ask(t, at(0), "a1")
	jobs, _ := te.tick(at(0))
	suspectWeak(te, "a1", time.Second)
	if !slices.Equal(queued(te), []string{"confirm:a1"}) {
		t.Fatalf("setup: queued %v", queued(te))
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(4*time.Second))
	p := te.path("a1")
	if p.status != statusUp || p.reason != "host_probe" || len(te.queue) != 0 || p.falseAlarms != 1 ||
		!p.cooldownUntil.Equal(at(time.Minute+4*time.Second)) {
		t.Fatalf("a1 %v by %q, queued %v, false alarms %d, cooldown until %v", p.status, p.reason, queued(te),
			p.falseAlarms, p.cooldownUntil.Sub(t0))
	}
}

// An exhausted balancer comes back once a host probe finds a candidate
// working, here while no user traffic brings a recheck.
func TestHostProbeRecoversAnExhaustedBalancer(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: &ScheduleConfig{Quarantine: &Backoff{InitialDelay: int64(time.Minute)}}},
		&SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	te.run(t, at(time.Second), map[string]bool{"a1": false, "b1": false})
	if !te.sel.exhausted {
		t.Fatal("setup: not exhausted")
	}
	te.events.drain()
	te.ask(t, at(5*time.Minute))
	te.run(t, at(5*time.Minute), map[string]bool{"a1": true, "b1": true})
	sw := eventsOf(drainEvents(te), "switch")
	if len(sw) != 1 || sw[0].Fields["to"] != "a1" || sw[0].Fields["reason"] != "recovered" {
		t.Fatalf("switches %v, want one to a1 for recovered", sw)
	}
}

// A passed host probe of a cheaper outbound neither moves the balancer nor
// brings the return closer.
func TestHostProbeOfACheaperOutboundDoesNotReturn(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	te.failN(3, "a1", failDial, at(0))
	te.run(t, at(time.Second), map[string]bool{"a1": false})
	if te.sel.current() != "b1" {
		t.Fatalf("failover went to %q", te.sel.current())
	}
	returnToCheaperAt := te.sel.returnToCheaperAt
	te.ask(t, at(2*time.Minute), "a1")
	jobs := te.run(t, at(2*time.Minute), map[string]bool{"a1": true})
	if !slices.Equal(reasons(jobs), []string{"host:a1"}) || te.path("a1").status != statusUp {
		t.Fatalf("probes %v, a1 %v", reasons(jobs), te.path("a1").status)
	}
	if te.sel.current() != "b1" || !te.sel.returnToCheaperAt.Equal(returnToCheaperAt) || te.sel.returnToCheaperTarget != "" {
		t.Fatalf("active %q, return at %v (was %v), target %q", te.sel.current(), te.sel.returnToCheaperAt.Sub(t0),
			returnToCheaperAt.Sub(t0), te.sel.returnToCheaperTarget)
	}
}

// returnConfig lets host probes of an outbound run right after the probes of
// a return to it.
func returnConfig() *Config {
	cfg := hostConfig(&HostProbesConfig{ReuseWindow: int64(time.Second)})
	cfg.Schedule.Quarantine = &Backoff{InitialDelay: int64(30 * time.Second)}
	cfg.Schedule.ProbesPerServerNamePerMinute = 20
	cfg.Schedule.ProbesPerMinute = 60
	return cfg
}

// A failed host probe of the outbound a return is under way to ends the
// return.
func TestHostProbeFailureEndsAReturn(t *testing.T) {
	te := newTestEngine(t, returnConfig(), returnToCheaperConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	jobs, _ := te.tick(at(6 * time.Minute))
	if !slices.Equal(reasons(jobs), []string{"return:a1"}) {
		t.Fatalf("setup: probes %v", reasons(jobs))
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(6*time.Minute))
	te.events.drain()
	te.ask(t, at(6*time.Minute+3*time.Second), "a1")
	jobs, _ = te.tick(at(6*time.Minute + 3*time.Second))
	if !slices.Equal(reasons(jobs), []string{"host:a1"}) {
		t.Fatalf("probes %v, want the host probe between the return probes", reasons(jobs))
	}
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(6*time.Minute+4*time.Second))
	if te.sel.returnToCheaperTarget != "" || len(te.queue) != 0 || countEvents(te.eventTypes(), "return_failed") != 1 {
		t.Fatalf("return target %q, queued %v", te.sel.returnToCheaperTarget, queued(te))
	}
}

// A failed host probe right after a return is a failed return.
func TestHostProbeFailureAfterAReturnIsPenalized(t *testing.T) {
	te := newTestEngine(t, returnConfig(), returnToCheaperConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	now := at(6 * time.Minute)
	for ; te.sel.current() != "a1" && now.Before(at(10*time.Minute)); now = now.Add(5 * time.Second) {
		te.run(t, now, nil)
	}
	if te.sel.current() != "a1" {
		t.Fatal("setup: no return to a1")
	}
	te.events.drain()
	te.ask(t, now.Add(5*time.Second), "a1")
	te.run(t, now.Add(5*time.Second), map[string]bool{"a1": false})
	if types := te.eventTypes(); countEvents(types, "return_penalty") != 1 || te.sel.current() != "b1" {
		t.Fatalf("events %v, active %q, want a penalty and b1", types, te.sel.current())
	}
}

// While the network check decides whether the network failed, a failed
// host probe changes nothing.
func TestHostProbeFailureWaitsForTheNetworkCheck(t *testing.T) {
	cfg := connectivityConfig()
	cfg.Schedule = &ScheduleConfig{MaxConcurrentProbes: 2, IdleAfter: int64(time.Hour)}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	suspectWeak(te, "a1", 0)
	te.ask(t, at(3*time.Second), "a1")
	jobs, _ := te.tick(at(3 * time.Second))
	if !slices.Equal(reasons(jobs), []string{"connectivity:", "host:a1"}) {
		t.Fatalf("setup: probes %v", reasons(jobs))
	}
	te.onProbeResult(jobs[1], probeResult{ok: false}, at(4*time.Second))
	if _, pending := te.pendingSuspicion["a1"]; te.path("a1").status != statusSuspect || !pending {
		t.Fatalf("a1 %v, waits for the network check %v", te.path("a1").status, pending)
	}
}

// A request ends after its timeout; probes it waited for that have not
// started are dropped.
func TestHostProbeRequestTimesOut(t *testing.T) {
	te := newTestEngine(t, hostConfig(&HostProbesConfig{ProbesPerMinute: 1, Timeout: int64(30 * time.Second)}),
		&SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	r := te.ask(t, at(0), "a1", "b1")
	jobs, _ := te.tick(at(0))
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(time.Second))
	if jobs, _ := te.tick(at(29 * time.Second)); len(jobs) != 0 || !r.open() {
		t.Fatalf("probes %v, open %v before the timeout", reasons(jobs), r.open())
	}
	te.events.drain()
	te.tick(at(30 * time.Second))
	if got := answers(r); r.open() || r.end != "timeout" || !slices.Equal(got, []string{"a1 ok/probe", "b1 expired/timeout"}) {
		t.Fatalf("open %v, end %q, answers %v", r.open(), r.end, got)
	}
	if len(te.queue) != 0 {
		t.Fatalf("queued %v after the timeout", queued(te))
	}
	if reqs := eventsOf(drainEvents(te), "probe_request"); len(reqs) != 1 || reqs[0].Fields["end"] != "timeout" ||
		reqs[0].Fields["expired"] != 1.0 {
		t.Fatalf("probe_request events %v", reqs)
	}
}

// A network change ends the request; the late result of its probe tells
// nothing about the new network.
func TestHostProbeRequestEndsWithTheNetwork(t *testing.T) {
	te := newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	r := te.ask(t, at(0), "b1")
	jobs, _ := te.tick(at(0))
	te.onNetworkChange("cafe", at(time.Second))
	te.tick(at(time.Second))
	if got := answers(r); r.open() || r.end != "network_change" || !slices.Equal(got, []string{"b1 expired/network_change"}) {
		t.Fatalf("open %v, end %q, answers %v", r.open(), r.end, got)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(2*time.Second))
	if te.path("b1").status == statusDown {
		t.Fatal("a probe of the previous network ruled b1 out")
	}

	// A probe of the new network does not answer a request of the previous
	// one, even before the request is ended.
	r = te.ask(t, at(3*time.Second), "b1")
	te.onNetworkChange("home", at(4*time.Second))
	j := te.enqueue("b1", reasonNetwork, "auto", at(4*time.Second))
	te.dropQueued("b1", reasonNetwork)
	te.running[j.id] = j
	te.onProbeResult(j, probeResult{ok: true}, at(5*time.Second))
	te.serveRequests(at(5 * time.Second))
	if got := answers(r); !slices.Equal(got, []string{"b1 expired/network_change"}) {
		t.Fatalf("answers %v of a request of the previous network", got)
	}
}

// An outbound the balancer no longer offers is gone; so is a request of a
// balancer the routing no longer has.
func TestHostProbeRequestOfWhatIsGone(t *testing.T) {
	te := newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	r := te.ask(t, at(0), "a1", "b1")
	te.dropProviderOutbound("b1", at(time.Second))
	te.serveRequests(at(time.Second))
	if got := answers(r); !slices.Equal(got, []string{"a1 pending/", "b1 gone/"}) || slices.Contains(queued(te), "host:b1") {
		t.Fatalf("answers %v, queued %v", got, queued(te))
	}
	te.keepSelectors(map[string]bool{})
	te.serveRequests(at(2 * time.Second))
	if r.open() || r.end != "gone" || r.items[0].result != probeGone || len(te.queue) != 0 {
		t.Fatalf("open %v, end %q, answers %v, queued %v", r.open(), r.end, answers(r), queued(te))
	}
}

// While memory is low only outbounds a balancer uses are probed for the
// host, and one closed request is kept.
func TestHostProbeUnderLowMemory(t *testing.T) {
	te := newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.setMemoryLow(true, at(0))
	r := te.ask(t, at(time.Second))
	if got := answers(r); !slices.Equal(got, []string{"a1 pending/", "b1 skipped/memory"}) {
		t.Fatalf("answers %v while memory is low", got)
	}

	te = newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	r = te.ask(t, at(0))
	te.setMemoryLow(true, at(time.Second))
	if !slices.Equal(queued(te), []string{"host:a1"}) {
		t.Fatalf("queued %v after memory became low, want the chosen a1 only", queued(te))
	}
	te.events.drain()
	te.run(t, at(time.Second), nil)
	if got := answers(r); !slices.Equal(got, []string{"a1 ok/probe", "b1 skipped/memory"}) {
		t.Fatalf("answers %v", got)
	}
	if n := len(eventsOf(drainEvents(te), "probe_start")); n != 1 {
		t.Fatalf("%d probe_start events while memory is low, want the one probe that ran", n)
	}
	for i := 0; i < 3; i++ {
		te.ask(t, at(2*time.Second+time.Duration(i)*time.Second), "a1")
	}
	if len(te.requests) != 1 {
		t.Fatalf("%d requests kept while memory is low, want 1", len(te.requests))
	}
}

// The observer keeps the open requests and the latest closed ones only.
func TestHostProbeRequestsAreBounded(t *testing.T) {
	te := newTestEngine(t, hostConfig(nil), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onSuccess("a1", at(0))
	var last uint64
	for i := 0; i < 10; i++ {
		last = te.ask(t, at(time.Duration(i)*time.Millisecond), "a1").id
	}
	te.ask(t, at(time.Second), "b1")
	if len(te.requests) != keptProbeRequests+1 || te.requests[keptProbeRequests-1].id != last {
		t.Fatalf("kept %d requests, the last closed one %d", len(te.requests), te.requests[keptProbeRequests-1].id)
	}
	raw, err := json.Marshal(te.status(at(2 * time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		ProbeRequests []map[string]any `json:"probe_requests"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	open := st.ProbeRequests[len(st.ProbeRequests)-1]
	closed := st.ProbeRequests[0]["outbounds"].([]any)[0].(map[string]any)
	if open["is_done"] != false || open["age_ms"] != 1000.0 || open["balancer"] != "auto" ||
		closed["result"] != "ok" || closed["source"] != "recent" || closed["age_ms"] == nil {
		t.Fatalf("status probe_requests %v", st.ProbeRequests)
	}
}
