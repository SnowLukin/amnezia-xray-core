package health

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

func reasons(jobs []*probeJob) []string {
	out := []string{}
	for _, j := range jobs {
		out = append(out, j.reason.String()+":"+j.tag)
	}
	return out
}

// A return to a cheaper outbound waits while memory is low and goes on once
// it is not.
func TestMemoryLowPutsReturnsOff(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	te.setMemoryLow(true, at(time.Minute))
	for now := at(5 * time.Minute); now.Before(at(20 * time.Minute)); now = now.Add(10 * time.Second) {
		if jobs := te.run(t, now, nil); len(jobs) != 0 {
			t.Fatalf("probes %v while memory is low", reasons(jobs))
		}
	}
	if n := countEvents(te.eventTypes(), "return_start"); n != 0 || te.sel.returnToCheaperTarget != "" {
		t.Fatalf("a return started while memory is low: %d return_start, target %q", n, te.sel.returnToCheaperTarget)
	}
	te.setMemoryLow(false, at(20*time.Minute))
	for now := at(20 * time.Minute); now.Before(at(25*time.Minute)) && te.sel.current() != "a1"; now = now.Add(10 * time.Second) {
		te.run(t, now, nil)
	}
	if te.sel.current() != "a1" {
		t.Fatalf("no return to a1 after memory came back: on %q", te.sel.current())
	}
}

// A return already under way waits for its next probe as well.
func TestMemoryLowHoldsAReturnUnderWay(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(30 * time.Second)}, returnToCheaperConfig(), "a1", "b1")
	failoverToB(t, te, at(0))
	jobs, _ := te.tick(at(6 * time.Minute))
	if len(jobs) != 1 || jobs[0].reason != reasonReturnToCheaper {
		t.Fatalf("setup: %v", reasons(jobs))
	}
	te.setMemoryLow(true, at(6*time.Minute+time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(6*time.Minute+2*time.Second))
	if more, _ := te.tick(at(8 * time.Minute)); len(more) != 0 {
		t.Fatalf("probes %v while memory is low", reasons(more))
	}
	te.setMemoryLow(false, at(9*time.Minute))
	if more, _ := te.tick(at(9 * time.Minute)); !slices.Equal(reasons(more), []string{"return:a1"}) {
		t.Fatalf("probes %v after memory came back, want the next return probe", reasons(more))
	}
}

// Probes after a network change, of the chosen outbound or of the one
// remembered for the network, wait while memory is low.
func TestMemoryLowHoldsNetworkChangeProbes(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onNetworkChange("home", at(0))
	te.run(t, at(time.Second), nil)
	te.switchTo(te.sel, "b1", "pin", at(2*time.Second))
	te.onSuccess("b1", at(3*time.Second))
	te.switchTo(te.sel, "a1", "pin", at(4*time.Second))
	te.setMemoryLow(true, at(time.Minute))
	for i, network := range []string{"cafe", "home"} {
		now := at(time.Duration(i+2) * time.Minute)
		te.onNetworkChange(network, now)
		te.noteDemand(now)
		if jobs, _ := te.tick(now.Add(time.Second)); len(jobs) != 0 {
			t.Fatalf("probes %v after a change to %s while memory is low", reasons(jobs), network)
		}
	}
	te.setMemoryLow(false, at(5*time.Minute))
	te.noteDemand(at(5 * time.Minute))
	if jobs, _ := te.tick(at(5 * time.Minute)); !slices.Equal(reasons(jobs), []string{"remembered:b1"}) {
		t.Fatalf("probes %v after memory came back, want the remembered b1", reasons(jobs))
	}
}

// Finding failures is not put off: a dead outbound is still confirmed,
// failed over and rechecked once everything is down.
func TestMemoryLowKeepsFailureDetection(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.setMemoryLow(true, at(0))
	te.failN(3, "a1", failDial, at(0))
	jobs, _ := te.tick(at(time.Second))
	if !slices.Equal(reasons(jobs), []string{"connectivity:"}) {
		t.Fatalf("probes %v, want the network check first", reasons(jobs))
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(time.Second))
	te.run(t, at(2*time.Second), map[string]bool{"b1": true})
	if te.sel.current() != "b1" {
		t.Fatalf("no failover while memory is low: on %q", te.sel.current())
	}
	suspectWeak(te, "b1", 10*time.Second)
	jobs, _ = te.tick(at(15 * time.Second))
	if !slices.Equal(reasons(jobs), []string{"confirm:b1"}) {
		t.Fatalf("probes %v, want a confirming probe of b1", reasons(jobs))
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(16*time.Second))
	te.exhaust(te.sel, at(20*time.Second))
	te.noteDemand(at(5 * time.Minute))
	if jobs, _ := te.tick(at(5 * time.Minute)); len(jobs) == 0 || jobs[0].reason != reasonRecheck {
		t.Fatalf("probes %v of an exhausted balancer, want a recheck", reasons(jobs))
	}
}

func TestMemoryLowStretchesDoubtfulNetworkChecks(t *testing.T) {
	te := newTestEngine(t, connectivityConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	now := at(0)
	for !te.doubt && now.Before(at(5*time.Minute)) {
		te.second(now, false, nil)
		now = now.Add(time.Second)
	}
	if !te.doubt {
		t.Fatal("setup: the network check never came to doubt")
	}
	te.setMemoryLow(true, now)
	var gaps []time.Duration
	last := time.Time{}
	for end := now.Add(10 * time.Minute); now.Before(end); now = now.Add(time.Second) {
		te.noteDemand(now)
		jobs, _ := te.tick(now)
		for _, j := range jobs {
			if j.reason == reasonConnectivity {
				if !last.IsZero() {
					gaps = append(gaps, now.Sub(last))
				}
				last = now
			}
			te.onProbeResult(j, probeResult{ok: false}, now)
		}
	}
	if len(gaps) == 0 || slices.Min(gaps) < lowMemoryRecheck {
		t.Fatalf("network checks in doubt %v apart while memory is low, want at least %v", gaps, lowMemoryRecheck)
	}
}

func TestMemoryLowSkipsTimedRefreshes(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.Interval = int64(time.Hour)
		p.Triggers.JitterFraction = -1
		p.Triggers.SkipRefreshWhenExhausted = false
	}), &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1", "a2")
	te.setMemoryLow(true, at(time.Minute))
	expectNoRefresh(t, te, at(2*time.Hour))
	if skips := te.eventsOf(t, "provider_skip"); len(skips) != 1 || skips[0].Fields["reason"] != "memory" {
		t.Fatalf("skips %v, want one for memory", skips)
	}
	te.requestRefresh("")
	j := expectRefresh(t, te, at(2*time.Hour+time.Second), "host")
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(2*time.Hour+time.Second))
	te.exhaust(te.sel, at(2*time.Hour+time.Minute))
	j = expectRefresh(t, te, at(2*time.Hour+2*time.Minute), "exhausted")
	te.onRefreshDone(j, refreshOutcome{ok: true, changed: true}, at(2*time.Hour+2*time.Minute))
	te.switchTo(te.sel, "a1", "recovered", at(2*time.Hour+3*time.Minute))
	expectNoRefresh(t, te, at(5*time.Hour))
	te.setMemoryLow(false, at(5*time.Hour))
	expectRefresh(t, te, at(5*time.Hour), "interval")
}

// Low memory postpones work, not what a host can see: the event log keeps its
// limits and every event is written.
func TestMemoryLowKeepsTheEventLog(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	for i := 0; i < 200; i++ {
		te.emit(at(0), "filler", map[string]any{"i": i, "pad": strings.Repeat("x", 40)})
	}
	te.setMemoryLow(true, at(time.Second))
	te.failN(3, "a1", failDial, at(2*time.Second))
	te.run(t, at(3*time.Second), nil)
	raw, dropped := te.events.drain()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		t.Fatal(err)
	}
	if dropped != 0 {
		t.Fatalf("%d events dropped while memory is low, the log should keep its limits", dropped)
	}
	var types []string
	for _, e := range evs {
		if e.Type != "filler" {
			types = append(types, e.Type)
		}
	}
	if !slices.Contains(types, "memory_low") || !slices.Contains(types, "probe_start") || !slices.Contains(types, "probe_result") {
		t.Fatalf("events %v while memory is low", types)
	}
}

// What no balancer offers is forgotten at once, not half an hour later.
func TestMemoryLowCollectsAtOnce(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1", "a2")
	te.path("a2").setStatus(statusUp, "traffic", at(time.Second))
	te.updateCandidates(te.sel, []string{"a1"}, at(2*time.Second))
	te.setMemoryLow(true, at(3*time.Second))
	if _, ok := te.paths["a2"]; ok {
		t.Fatal("a2 is no longer offered but kept while memory is low")
	}
	if _, ok := te.paths["a1"]; !ok {
		t.Fatal("a1 is still offered but forgotten")
	}
}

func TestSetMemoryLow(t *testing.T) {
	o := newObserver(context.Background(), &Config{Paths: observed("a")})
	defer o.Close()
	o.SetMemoryLow(true)
	o.SetMemoryLow(true)
	var st struct {
		MemoryLow bool `json:"is_memory_low"`
	}
	if err := json.Unmarshal(o.Status(), &st); err != nil || !st.MemoryLow {
		t.Fatalf("status memory_low %v, %v", st.MemoryLow, err)
	}
	o.SetMemoryLow(false)
	raw, _ := o.DrainEvents()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		t.Fatal(err)
	}
	var on []any
	for _, e := range evs {
		if e.Type == "memory_low" {
			on = append(on, e.Fields["is_low"])
		}
	}
	if !slices.Equal(on, []any{true, false}) {
		t.Fatalf("memory_low events %v, want one on and one off", on)
	}
}

// Low memory ends the drains of outbounds whose connections have ended
// since they left the set.
func TestMemoryLowEndsIdleDrains(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	f.follow("p-x1", "p-x2")
	x1 := f.m.GetHandler("p-x1").(*fakeHandler)
	tr := f.o.StartDial(context.Background(), "p-x1", net.TCPDestination(net.ParseAddress("192.0.2.7"), 443), nil)
	tr.Dialed(nil, nil, nil, nil)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	tr.Finished(nil)
	if x1.closes() != 0 {
		t.Fatal("setup: p-x1 closed while its connection was open")
	}
	f.o.SetMemoryLow(true)
	if x1.closes() != 1 || len(f.rt.draining) != 0 {
		t.Fatalf("low memory: p-x1 closed %d times, %d draining", x1.closes(), len(f.rt.draining))
	}
}
