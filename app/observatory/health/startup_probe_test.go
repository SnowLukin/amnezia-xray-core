package health

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/features/extension"
	"google.golang.org/protobuf/proto"
)

// startupSelector is a balancer of a at cost 1 and b at cost 2 with
// startup.probe.
func startupSelector(mod func(*SelectorConfig)) *SelectorConfig {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 2), Features: &SelectorFeatureConfig{StartupProbe: proto.Bool(true)}}
	if mod != nil {
		mod(sc)
	}
	return sc
}

func switchNow(sc *SelectorConfig) { sc.Features.ProbeBeforeSwitch = proto.Bool(false) }

// one returns the only job, which probes tag for the reason.
func one(t *testing.T, jobs []*probeJob, tag string, reason probeReason) *probeJob {
	t.Helper()
	if len(jobs) != 1 || jobs[0].tag != tag || jobs[0].reason != reason || jobs[0].selector != "auto" && reason == reasonStartup {
		var got []string
		for _, j := range jobs {
			got = append(got, j.tag+":"+j.reason.String())
		}
		t.Fatalf("jobs %v, want %s:%s", got, tag, reason)
	}
	return jobs[0]
}

func startupQueued(te *testEngine) int {
	n := 0
	for _, j := range te.queue {
		if j.reason == reasonStartup {
			n++
		}
	}
	return n
}

// Without the block the choice waits for a connection, as before; the
// engine still notes the first proof.
func TestWithoutStartupProbeTheChoiceWaitsForTraffic(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 2)}, "a1", "b1")
	for i := range 5 {
		if jobs, _ := te.tick(at(time.Duration(i) * time.Second)); len(jobs) != 0 || len(te.queue) != 0 {
			t.Fatalf("second %d: jobs %v, queue %d", i, jobTags(jobs), len(te.queue))
		}
	}
	if te.sel.confirmed || te.startAsks() != nil {
		t.Fatalf("confirmed %v, asks %v", te.sel.confirmed, te.startAsks())
	}
	te.onSuccess("a1", at(5*time.Second))
	if !te.sel.confirmed {
		t.Fatal("traffic did not confirm the choice")
	}
}

// The choice is probed at once, with no connection of the user, and a
// probe that passes proves it. After that the core is on demand again.
func TestStartupProbeProvesTheChoice(t *testing.T) {
	te := newTestEngine(t, nil, startupSelector(switchNow), "a1", "b1")
	te.lastDemand.Store(0)
	j := one(t, mustTick(te, t0), "a1", reasonStartup)
	if jobs, _ := te.tick(at(time.Second)); len(jobs) != 0 || startupQueued(te) != 0 {
		t.Fatalf("a second probe of the choice: %v, queued %d", jobTags(jobs), startupQueued(te))
	}
	te.onProbeResult(j, probeResult{ok: true}, at(2*time.Second))
	if p := te.path("a1"); p.status != statusUp || !te.sel.confirmed || te.sel.current() != "a1" {
		t.Fatalf("a1 %s, confirmed %v, choice %s", p.status, te.sel.confirmed, te.sel.current())
	}
	var reasons []string
	for _, e := range allEvents(t, te) {
		if e.Type == "probe_start" || e.Type == "probe_result" {
			reasons = append(reasons, e.Type+":"+e.Fields["reason"].(string)+":"+e.Fields["balancer"].(string))
		}
	}
	if !slices.Equal(reasons, []string{"probe_start:startup:auto", "probe_result:startup:auto"}) {
		t.Fatalf("events %v", reasons)
	}
	// A later choice waits for traffic, as without the block.
	te.switchTo(te.sel, "b1", "failover", at(3*time.Second))
	for i := range 5 {
		if jobs, _ := te.tick(at(time.Duration(3+i) * time.Second)); len(jobs) != 0 {
			t.Fatalf("probe after the first proof: %v", jobTags(jobs))
		}
	}
}

func mustTick(te *testEngine, now time.Time) []*probeJob {
	jobs, _ := te.tick(now)
	return jobs
}

func allEvents(t *testing.T, te *testEngine) []Event {
	t.Helper()
	raw, _ := te.events.drain()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		t.Fatal(err)
	}
	return evs
}

// A probe that fails rules the choice out as a failed probe in a new
// network does, and the next choice is probed at once as well.
func TestStartupProbeFailureMovesToTheNextChoice(t *testing.T) {
	te := newTestEngine(t, nil, startupSelector(switchNow), "a1", "b1")
	te.onProbeResult(one(t, mustTick(te, t0), "a1", reasonStartup), probeResult{}, at(time.Second))
	if p := te.path("a1"); p.status != statusDown || p.reason != "probe" || !p.quarantined(at(time.Second)) || te.sel.current() != "b1" || te.sel.confirmed {
		t.Fatalf("a1 %s %s, choice %s, confirmed %v", p.status, p.reason, te.sel.current(), te.sel.confirmed)
	}
	if got := te.sel.switched; got != (viewSwitch{From: "a1", To: "b1", Reason: "failover"}) {
		t.Fatalf("switch %+v", got)
	}
	te.onProbeResult(one(t, mustTick(te, at(5*time.Second)), "b1", reasonStartup), probeResult{ok: true}, at(6*time.Second))
	if !te.sel.confirmed || te.sel.current() != "b1" || te.path("b1").status != statusUp {
		t.Fatalf("b1 %s, choice %s, confirmed %v", te.path("b1").status, te.sel.current(), te.sel.confirmed)
	}
}

// A balancer that probes before it switches proves the next outbound by
// the probe of the failover: no second probe follows.
func TestStartupProbeFailureWithProbeBeforeSwitch(t *testing.T) {
	te := newTestEngine(t, nil, startupSelector(nil), "a1", "b1")
	te.onProbeResult(one(t, mustTick(te, t0), "a1", reasonStartup), probeResult{}, at(time.Second))
	if te.sel.current() != "a1" || len(te.sel.failoverProbing) != 1 {
		t.Fatalf("choice %s, failover probes %v", te.sel.current(), te.sel.failoverProbing)
	}
	te.onProbeResult(one(t, mustTick(te, at(5*time.Second)), "b1", reasonFailover), probeResult{ok: true}, at(6*time.Second))
	if !te.sel.confirmed || te.sel.current() != "b1" {
		t.Fatalf("choice %s, confirmed %v", te.sel.current(), te.sel.confirmed)
	}
	if jobs, _ := te.tick(at(10 * time.Second)); len(jobs) != 0 {
		t.Fatalf("jobs %v", jobTags(jobs))
	}
}

// When every choice failed the balancer runs out as usual: it holds the
// last outbound and rechecks the resting ones while there is demand, and
// the one that comes back is proven by its recheck.
func TestStartupProbeEveryChoiceFailed(t *testing.T) {
	te := newTestEngine(t, nil, startupSelector(switchNow), "a1", "b1")
	te.onProbeResult(one(t, mustTick(te, t0), "a1", reasonStartup), probeResult{}, at(time.Second))
	te.onProbeResult(one(t, mustTick(te, at(5*time.Second)), "b1", reasonStartup), probeResult{}, at(6*time.Second))
	if !te.sel.exhausted || te.sel.held != "b1" || te.sel.confirmed {
		t.Fatalf("exhausted %v, held %q, confirmed %v", te.sel.exhausted, te.sel.held, te.sel.confirmed)
	}
	for s := 7; s < 200 && te.sel.current() == ""; s++ {
		now := at(time.Duration(s) * time.Second)
		te.noteDemand(now)
		for _, j := range mustTick(te, now) {
			if j.reason == reasonStartup {
				t.Fatalf("startup probe of %s while exhausted", j.tag)
			}
			te.onProbeResult(j, probeResult{ok: j.reason == reasonRecheck && j.tag == "a1"}, now)
		}
	}
	if te.sel.current() != "a1" || !te.sel.confirmed {
		t.Fatalf("choice %q, confirmed %v", te.sel.current(), te.sel.confirmed)
	}
}

// A connection of the user that works before the probe, or while it runs,
// proves the choice: nothing is probed twice and a late failure of the
// probe is old news.
func TestStartupProbeAndTraffic(t *testing.T) {
	t.Run("traffic first", func(t *testing.T) {
		te := newTestEngine(t, nil, startupSelector(nil), "a1", "b1")
		te.onSuccess("a1", t0)
		if jobs, _ := te.tick(t0); len(jobs) != 0 || !te.sel.confirmed {
			t.Fatalf("jobs %v, confirmed %v", jobTags(jobs), te.sel.confirmed)
		}
	})
	t.Run("traffic before the probe starts", func(t *testing.T) {
		te := newTestEngine(t, nil, startupSelector(nil), "a1", "b1")
		te.probeAtStart(te.sel, t0)
		te.probeAtStart(te.sel, t0)
		if startupQueued(te) != 1 {
			t.Fatalf("queued %d", startupQueued(te))
		}
		te.onSuccess("a1", t0)
		if jobs, _ := te.tick(t0); len(jobs) != 0 || startupQueued(te) != 0 {
			t.Fatalf("jobs %v, queued %d", jobTags(jobs), startupQueued(te))
		}
	})
	t.Run("traffic while the probe runs", func(t *testing.T) {
		te := newTestEngine(t, nil, startupSelector(nil), "a1", "b1")
		j := one(t, mustTick(te, t0), "a1", reasonStartup)
		te.onSuccess("a1", at(time.Second))
		te.onProbeResult(j, probeResult{}, at(2*time.Second))
		if p := te.path("a1"); p.status != statusUp || te.sel.current() != "a1" || len(te.sel.failoverProbing) != 0 {
			t.Fatalf("a1 %s, choice %s", p.status, te.sel.current())
		}
		if jobs, _ := te.tick(at(3 * time.Second)); len(jobs) != 0 {
			t.Fatalf("jobs %v", jobTags(jobs))
		}
	})
}

// Without a network nothing is probed. When it is back the choice is
// probed: by the probe of the new network when the balancer rechecks its
// choice there, by the startup probe otherwise, never by both.
func TestStartupProbeWithoutNetwork(t *testing.T) {
	ignore := func(sc *SelectorConfig) { sc.Features.NetworkChanges = proto.Bool(false) }
	t.Run("lost before the probe", func(t *testing.T) {
		te := newTestEngine(t, nil, startupSelector(ignore), "a1", "b1")
		te.onNetworkLost(t0)
		if jobs, _ := te.tick(t0); len(jobs) != 0 || len(te.queue) != 0 {
			t.Fatalf("jobs %v, queue %d", jobTags(jobs), len(te.queue))
		}
		te.onNetworkChange("wifi", at(time.Second))
		one(t, mustTick(te, at(time.Second)), "a1", reasonStartup)
	})
	t.Run("lost while the probe runs", func(t *testing.T) {
		te := newTestEngine(t, nil, startupSelector(ignore), "a1", "b1")
		j := one(t, mustTick(te, t0), "a1", reasonStartup)
		te.onNetworkLost(at(time.Second))
		te.onProbeResult(j, probeResult{}, at(2*time.Second))
		if p := te.path("a1"); p.status != statusUnknown || te.sel.current() != "a1" {
			t.Fatalf("a1 %s, choice %s", p.status, te.sel.current())
		}
		if jobs, _ := te.tick(at(3 * time.Second)); len(jobs) != 0 {
			t.Fatalf("probe without a network: %v", jobTags(jobs))
		}
		te.onNetworkChange("wifi", at(10*time.Second))
		one(t, mustTick(te, at(10*time.Second)), "a1", reasonStartup)
	})
	t.Run("new network, the balancer rechecks", func(t *testing.T) {
		te := newTestEngine(t, nil, startupSelector(nil), "a1", "b1")
		old := one(t, mustTick(te, t0), "a1", reasonStartup)
		te.onNetworkChange("lte", at(time.Second))
		te.onProbeResult(old, probeResult{canceled: true}, at(time.Second))
		j := one(t, mustTick(te, at(5*time.Second)), "a1", reasonNetwork)
		te.onProbeResult(j, probeResult{ok: true}, at(6*time.Second))
		if jobs, _ := te.tick(at(10 * time.Second)); len(jobs) != 0 || startupQueued(te) != 0 || !te.sel.confirmed {
			t.Fatalf("jobs %v, queued %d, confirmed %v", jobTags(jobs), startupQueued(te), te.sel.confirmed)
		}
	})
	t.Run("new network, no traffic", func(t *testing.T) {
		te := newTestEngine(t, nil, startupSelector(nil), "a1", "b1")
		old := one(t, mustTick(te, t0), "a1", reasonStartup)
		te.lastDemand.Store(0)
		te.onNetworkChange("lte", at(time.Second))
		te.onProbeResult(old, probeResult{canceled: true}, at(time.Second))
		// The probe of the new network waits for a connection; this one does not.
		one(t, mustTick(te, at(5*time.Second)), "a1", reasonStartup)
	})
}

// A choice pinned at the start is probed too. It is not left when the
// probe fails, and is not probed again.
func TestStartupProbeOfTheStartPin(t *testing.T) {
	te := newTestEngine(t, nil, startupSelector(func(sc *SelectorConfig) { sc.StartPinnedOutbound = "b1" }), "a1", "b1")
	if te.sel.current() != "b1" || te.sel.pinned != "b1" {
		t.Fatalf("choice %s, pinned %s", te.sel.current(), te.sel.pinned)
	}
	te.onProbeResult(one(t, mustTick(te, t0), "b1", reasonStartup), probeResult{}, at(time.Second))
	if te.sel.current() != "b1" || te.sel.pinned != "b1" || te.path("b1").status != statusDown || te.sel.confirmed {
		t.Fatalf("choice %s, pinned %s, b1 %s", te.sel.current(), te.sel.pinned, te.path("b1").status)
	}
	if evs := eventsOf(allEvents(t, te), "pinned_down"); len(evs) != 1 || evs[0].Fields["is_start_pinned"] != true {
		t.Fatalf("pinned_down %v", evs)
	}
	for s := 2; s < 300; s += 7 {
		now := at(time.Duration(s) * time.Second)
		te.noteDemand(now)
		if jobs, _ := te.tick(now); len(jobs) != 0 {
			t.Fatalf("second %d: jobs %v", s, jobTags(jobs))
		}
	}
	// The pin that worked is proven like any choice.
	te = newTestEngine(t, nil, startupSelector(func(sc *SelectorConfig) { sc.StartPinnedOutbound = "b1" }), "a1", "b1")
	te.onProbeResult(one(t, mustTick(te, t0), "b1", reasonStartup), probeResult{ok: true}, at(time.Second))
	if !te.sel.confirmed || te.sel.pinned != "b1" {
		t.Fatalf("confirmed %v, pinned %s", te.sel.confirmed, te.sel.pinned)
	}
}

// The outbound the host passed for the start, and the one remembered for
// the network, are chosen without a probe otherwise; with the block they
// are probed like any first choice.
func TestStartupProbeOfAPassedOutbound(t *testing.T) {
	te := newTestEngine(t, nil, startupSelector(func(sc *SelectorConfig) { sc.StartOutbound = "b1" }), "a1", "b1")
	if te.sel.switched.Reason != "start_with" {
		t.Fatalf("switch %+v", te.sel.switched)
	}
	one(t, mustTick(te, t0), "b1", reasonStartup)

	e := newEngine(legacyScenarioSettings(&Config{Paths: pathSpecs("a1", 1, "b1", 2)}), 1)
	s := e.addSelector("auto", legacyScenarioSelector(&SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 2), Features: &SelectorFeatureConfig{StartupProbe: proto.Bool(true)}}, &Config{}))
	e.networkID = "wifi"
	e.networks["wifi"] = map[string]string{"auto": "b1"}
	e.updateCandidates(s, []string{"a1", "b1"}, t0)
	if s.switched.Reason != "start_remembered" {
		t.Fatalf("switch %+v", s.switched)
	}
	one(t, mustTick(&testEngine{engine: e, sel: s}, t0), "b1", reasonStartup)
}

// A frozen observer starts nothing.
func TestStartupProbeOfAFrozenEngine(t *testing.T) {
	te := newTestEngine(t, nil, startupSelector(nil), "a1", "b1")
	var frozen atomic.Bool
	frozen.Store(true)
	te.frozen = &frozen
	if jobs, _, _ := te.step(t0); len(jobs) != 0 || len(te.queue) != 0 {
		t.Fatalf("jobs %v, queue %d", jobTags(jobs), len(te.queue))
	}
}

// A startup probe waits for the probe budget like every other probe.
func TestStartupProbeKeepsTheBudget(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: &ScheduleConfig{ProbesPerMinute: 1}}, startupSelector(switchNow), "a1", "b1")
	te.onProbeResult(one(t, mustTick(te, t0), "a1", reasonStartup), probeResult{}, at(time.Second))
	if jobs, _ := te.tick(at(5 * time.Second)); len(jobs) != 0 || startupQueued(te) != 1 {
		t.Fatalf("jobs %v, queued %d", jobTags(jobs), startupQueued(te))
	}
	one(t, mustTick(te, at(61*time.Second)), "b1", reasonStartup)
}

// Select implements outbound.HandlerSelector as the outbound manager does.
func (m *fakeManager) Select(prefixes []string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	tags := []string{}
	for tag := range m.handlers {
		if slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(tag, p) }) {
			tags = append(tags, tag)
		}
	}
	sort.Strings(tags)
	return tags
}

// startupFixture is an observer whose balancer auto has a1 and b1, over an
// outbound manager that holds the outbounds named.
type startupFixture struct {
	o   *Observer
	now time.Time
	m   *fakeManager
	h   extension.OutboundSelector
	s   *selector
}

func newStartupFixture(t *testing.T, block bool, handlers ...string) *startupFixture {
	t.Helper()
	paths := []*PathDefinition{{Tag: "a1", Balancer: "auto", Cost: 1}, {Tag: "b1", Balancer: "auto", Cost: 2}}
	f := &startupFixture{now: t0, m: newFakeManager()}
	f.o = newObserver(context.Background(), &Config{Paths: paths, View: &ViewConfig{Balancer: "auto"}, Features: &FeatureConfig{Events: proto.Bool(true)}})
	f.o.clock = func() time.Time { return f.now }
	f.o.ohm = f.m
	t.Cleanup(func() { f.o.Close() })
	f.add(handlers...)
	sc := &SelectorConfig{Paths: paths, FallbackTag: "block", OutboundSelector: []string{"a1", "b1"},
		Features: &SelectorFeatureConfig{ProbeBeforeSwitch: proto.Bool(false)}}
	if block {
		sc.Features.StartupProbe = proto.Bool(true)
	}
	h, err := f.o.OutboundSelector("auto", serial.ToTypedMessage(sc))
	if err != nil {
		t.Fatal(err)
	}
	f.h, f.s = h, f.o.e.selectors["auto"]
	return f
}

func (f *startupFixture) add(tags ...string) {
	for _, tag := range tags {
		f.m.handlers[tag] = &fakeHandler{tag: tag}
	}
}

// turn is one turn of the loop of the observer: the probes it starts, then
// the candidates it brings.
func (f *startupFixture) turn() []*probeJob {
	f.o.mu.Lock()
	jobs, _, _ := f.o.e.step(f.now)
	asks := f.o.e.startAsks()
	f.o.mu.Unlock()
	f.o.demandAtStart(asks)
	return jobs
}

func (f *startupFixture) switches(t *testing.T) []string {
	t.Helper()
	raw, _ := f.o.DrainEvents()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range eventsOf(evs, "switch") {
		out = append(out, e.Fields["reason"].(string)+":"+e.Fields["to"].(string))
	}
	return out
}

func (f *startupFixture) view(t *testing.T) ViewData {
	t.Helper()
	v, err := f.o.View()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// With the block the observer itself brings the balancer its candidates
// when the core has started, and the probe of the choice starts with the
// next turn: the view gets to connected without a connection of the user.
func TestStartupDemandChoosesWithoutAConnection(t *testing.T) {
	f := newStartupFixture(t, true, "a1", "b1")
	if len(f.o.kick) != 1 {
		t.Fatal("a balancer with the block does not wake the loop")
	}
	if v := f.view(t); v.State != viewConnecting || v.Phase != phaseWaiting || !v.IsIdle {
		t.Fatalf("before the first turn: %s %s", v.State, v.Phase)
	}
	if jobs := f.turn(); len(jobs) != 0 || !f.s.started || f.s.current() != "a1" || f.o.e.lastDemand.Load() == 0 {
		t.Fatalf("first turn: jobs %v, started %v, choice %q", jobTags(jobs), f.s.started, f.s.current())
	}
	j := one(t, f.turn(), "a1", reasonStartup)
	if v := f.view(t); v.State != viewConnecting || v.Phase != phaseVerifying || v.Outbound.Tag != "a1" || v.IsIdle ||
		viewPathOf(t, v, "a1").Probing == nil || viewPathOf(t, v, "a1").Probing.Reason != "startup" {
		t.Fatalf("probing: %+v", v)
	}
	f.o.e.onProbeResult(j, probeResult{ok: true}, f.now)
	if v := f.view(t); v.State != viewConnected || v.Phase != "" || v.Outbound.Tag != "a1" {
		t.Fatalf("proven: %+v", v)
	}
	// The first connection of the user finds the choice made.
	if got := f.h.Select([]string{"a1", "b1"}); got != "a1" {
		t.Fatalf("selected %q", got)
	}
	if jobs := f.turn(); len(jobs) != 0 {
		t.Fatalf("jobs %v", jobTags(jobs))
	}
	if got := f.switches(t); !slices.Equal(got, []string{"start:a1"}) {
		t.Fatalf("switches %v", got)
	}
}

// A connection that came before the observer asked has started the
// balancer: the observer chooses nothing anew, and the choice is probed
// once.
func TestStartupDemandAfterTheFirstConnection(t *testing.T) {
	f := newStartupFixture(t, true, "a1", "b1")
	if got := f.h.Select([]string{"a1", "b1"}); got != "a1" {
		t.Fatalf("selected %q", got)
	}
	one(t, f.turn(), "a1", reasonStartup)
	if jobs := f.turn(); len(jobs) != 0 {
		t.Fatalf("jobs %v", jobTags(jobs))
	}
	if got := f.switches(t); !slices.Equal(got, []string{"start:a1"}) {
		t.Fatalf("switches %v", got)
	}
}

// Connections that come while the observer asks make no second choice and
// no second probe.
func TestStartupDemandRacesWithConnections(t *testing.T) {
	f := newStartupFixture(t, true, "a1", "b1")
	var mu sync.Mutex
	var jobs []*probeJob
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			f.h.Select([]string{"a1", "b1"})
		}()
		go func() {
			defer wg.Done()
			started := f.turn()
			mu.Lock()
			jobs = append(jobs, started...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	jobs = append(jobs, f.turn()...)
	one(t, jobs, "a1", reasonStartup)
	if got := f.switches(t); !slices.Equal(got, []string{"start:a1"}) {
		t.Fatalf("switches %v", got)
	}
}

// Candidates that come later, as a set of a provider does, are brought to
// the balancer the same way, and the choice among them is probed at once.
func TestStartupDemandBringsLaterCandidates(t *testing.T) {
	f := newStartupFixture(t, true)
	if jobs := f.turn(); len(jobs) != 0 || !f.s.started || !f.s.exhausted {
		t.Fatalf("jobs %v, started %v, exhausted %v", jobTags(jobs), f.s.started, f.s.exhausted)
	}
	if v := f.view(t); v.State != viewBlocked || v.Phase != phaseWaiting {
		t.Fatalf("nothing to choose and nobody to ask: %s %s", v.State, v.Phase)
	}
	f.now = f.now.Add(time.Second)
	f.add("b1")
	if jobs := f.turn(); len(jobs) != 0 || f.s.current() != "b1" {
		t.Fatalf("jobs %v, choice %q", jobTags(jobs), f.s.current())
	}
	j := one(t, f.turn(), "b1", reasonStartup)
	f.o.e.onProbeResult(j, probeResult{}, f.now)
	// The last outbound failed: the balancer holds it and takes the next
	// candidate that comes.
	f.now = f.now.Add(10 * time.Second)
	f.add("a1")
	for _, recheck := range f.turn() {
		f.o.e.onProbeResult(recheck, probeResult{}, f.now)
	}
	f.now = f.now.Add(10 * time.Second)
	j = one(t, f.turn(), "a1", reasonStartup)
	f.o.e.onProbeResult(j, probeResult{ok: true}, f.now)
	if v := f.view(t); v.State != viewConnected || v.Outbound.Tag != "a1" {
		t.Fatalf("proven: %+v", v)
	}
	if got := f.switches(t); !slices.Equal(got, []string{"candidates_changed:b1", "candidates_changed:a1"}) {
		t.Fatalf("switches %v", got)
	}
	// Proven: candidates that change now wait for a connection again.
	f.m.RemoveHandler(context.Background(), "b1")
	if f.o.e.startAsks() != nil {
		t.Fatal("the observer still asks for a proven balancer")
	}
}

// A provider request through the balancer hands it every outbound there
// is, and the next connection the list of the router again. The observer
// has brought these candidates already: it is no new demand.
func TestStartupDemandBringsTheSameCandidatesOnce(t *testing.T) {
	f := newStartupFixture(t, true, "a1", "b1", "direct")
	f.turn()
	one(t, f.turn(), "a1", reasonStartup)
	<-f.o.kick
	f.now = f.now.Add(time.Minute)
	f.o.e.updateCandidates(f.s, []string{"a1", "b1", "direct"}, f.now)
	demand := f.o.e.lastDemand.Load()
	if jobs := f.turn(); len(jobs) != 0 || f.o.e.lastDemand.Load() != demand || len(f.o.kick) != 0 {
		t.Fatalf("jobs %v, demand moved %v, wakes %d", jobTags(jobs), f.o.e.lastDemand.Load() != demand, len(f.o.kick))
	}
	if snap := *f.s.snapshot.Load(); !slices.Equal(snap, []string{"a1", "b1", "direct"}) {
		t.Fatalf("snapshot %v", snap)
	}
	if got := f.switches(t); !slices.Equal(got, []string{"start:a1"}) {
		t.Fatalf("switches %v", got)
	}
}

// Without the block the observer asks for nothing: the balancer waits for
// its first connection.
func TestStartupDemandNeedsTheBlock(t *testing.T) {
	f := newStartupFixture(t, false, "a1", "b1")
	if len(f.o.kick) != 0 || f.o.e.startAsks() != nil {
		t.Fatalf("wakes %d, asks %v", len(f.o.kick), f.o.e.startAsks())
	}
	for range 3 {
		if jobs := f.turn(); len(jobs) != 0 || f.s.started || f.o.e.lastDemand.Load() != 0 || len(f.o.kick) != 0 {
			t.Fatalf("jobs %v, started %v, demand %d", jobTags(jobs), f.s.started, f.o.e.lastDemand.Load())
		}
		f.now = f.now.Add(time.Second)
	}
	if v := f.view(t); v.State != viewConnecting || v.Phase != phaseWaiting {
		t.Fatalf("view %s %s", v.State, v.Phase)
	}
}

// Status tells whether the balancer has the block. A config built by hand
// that has it needs the selector of the balancer.
func TestStartupProbeFeature(t *testing.T) {
	for _, block := range []bool{true, false} {
		f := newStartupFixture(t, block, "a1")
		var st struct {
			Features map[string]bool `json:"features"`
		}
		if err := json.Unmarshal(f.o.Status(), &st); err != nil {
			t.Fatal(err)
		}
		if on, known := st.Features["balancers.auto.startup_probe"]; !known || on != block {
			t.Fatalf("block %v: feature %v, known %v", block, on, known)
		}
	}
	f := newStartupFixture(t, false)
	_, err := f.o.OutboundSelector("other", serial.ToTypedMessage(&SelectorConfig{Features: &SelectorFeatureConfig{StartupProbe: proto.Bool(true)}}))
	if err == nil || !strings.Contains(err.Error(), "startup.probe requires") {
		t.Fatalf("error %v", err)
	}
}
