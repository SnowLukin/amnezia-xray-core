package health

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/xtls/xray-core/core"
	"google.golang.org/protobuf/proto"
)

func TestProviderDelayDistributionInclusive(t *testing.T) {
	e := newEngine(newSettings(nil), 42)
	bins := make([]int, 11)
	for range 11000 {
		d := e.randomDelay(20, 30)
		if d < 20 || d > 30 {
			t.Fatal(d)
		}
		bins[int(d)-20]++
	}
	for i, n := range bins {
		if n < 800 || n > 1200 {
			t.Fatalf("bin %d = %d", i, n)
		}
	}
	if e.randomDelay(30, 30) != 30 {
		t.Fatal("fixed delay changed")
	}
	for range 100 {
		if e.randomDelay(0, time.Duration(math.MaxInt64)) < 0 {
			t.Fatal("overflow")
		}
	}
}

func TestProviderEventDelayCancelledWhenConditionHeals(t *testing.T) {
	for _, trigger := range []refreshTrigger{triggerExhausted, triggerMinHealthy} {
		t.Run(trigger.String(), func(t *testing.T) {
			te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
			p := te.prov()
			if trigger == triggerExhausted {
				p.cfg.whenExhausted = true
				p.cfg.exhaustedDelay = durationRange{10 * time.Second, 10 * time.Second}
			} else {
				p.cfg.minHealthy = 2
				p.cfg.minHealthyDelay = durationRange{10 * time.Second, 10 * time.Second}
			}
			te.sel.exhausted = true
			expectNoRefresh(t, te, at(time.Second))
			if p.delayed[trigger] != at(11*time.Second) {
				t.Fatal(p.delayed)
			}
			// The original deadline must disappear, rather than surviving recovery.
			te.sel.exhausted = false
			if trigger == triggerMinHealthy {
				p.cfg.minHealthy = 1
			}
			expectNoRefresh(t, te, at(5*time.Second))
			if !p.delayed[trigger].IsZero() {
				t.Fatal("stale delay")
			}
			te.sel.exhausted = true
			if trigger == triggerMinHealthy {
				p.cfg.minHealthy = 2
			}
			expectNoRefresh(t, te, at(11*time.Second))
			expectRefresh(t, te, at(21*time.Second), trigger.String())
		})
	}
}

func TestProviderRoadsUseOnlyPrimaryTriggers(t *testing.T) {
	pc := quietProvider(nil)
	pc.Apply.Balancers = []string{"auto", "backend"}
	te := newProviderEngine(t, pc, &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	p := te.prov()
	p.cfg.whenExhausted = true
	p.cfg.minHealthy = 1
	p.cfg.costAbove = true
	p.cfg.costThreshold = 10
	p.cfg.exhaustedDelay = durationRange{}
	secondary := te.addSelector("backend", newSelectorSettings(&SelectorConfig{Paths: []*PathDefinition{{Tag: "p-road", Balancer: "backend", Cost: 100}}}))
	secondary.started = true
	secondary.exhausted = true
	expectNoRefresh(t, te, at(time.Second))
	secondary.exhausted = false
	secondary.setActive("p-road")
	expectNoRefresh(t, te, at(time.Minute))
	te.sel.exhausted = true
	j := expectRefresh(t, te, at(2*time.Minute), "exhausted")
	if len(j.request.Balancers) != 2 || j.request.Balancers[0].Tag != "auto" || j.request.Balancers[1].Tag != "backend" {
		t.Fatal(j.request)
	}
}

func TestProviderRoadMembershipMovesAtomically(t *testing.T) {
	f := newApplyFixture(t, time.Minute, func(p *ProviderConfig) { p.Apply.Balancers = []string{"auto", "backend"} })
	second := f.o.e.addSelector("backend", newSelectorSettings(&SelectorConfig{}))
	raw := `{"outbounds":[{"tag":"user","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":1}},{"tag":"road","protocol":"vless","selection":{"balancer":"backend","probe":"check","cost":2}}]}`
	a, _ := parseAnswer([]byte(raw))
	if _, err := f.apply(t, a); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*selector{f.o.e.selectors["auto"], second} {
		f.o.e.updateCandidates(s, []string{"p-user", "p-road"}, t0)
	}
	if f.o.e.selectors["auto"].cfg.paths["p-road"] != nil || second.cfg.paths["p-user"] != nil || len(second.inventory) != 1 || second.inventory[0] != "p-road" {
		t.Fatal("membership crossed balancers")
	}
	raw = `{"outbounds":[{"tag":"user","protocol":"vless","selection":{"balancer":"backend","probe":"check","cost":1}},{"tag":"road","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":2}}]}`
	a, _ = parseAnswer([]byte(raw))
	if _, err := f.apply(t, a); err != nil {
		t.Fatal(err)
	}
	if f.o.e.selectors["auto"].cfg.paths["p-user"] != nil || second.cfg.paths["p-road"] != nil || second.cfg.paths["p-user"] == nil {
		t.Fatal("old membership survived a move")
	}
	bad := `{"outbounds":[{"tag":"bad","protocol":"vless","selection":{"balancer":"other","probe":"check","cost":1}}]}`
	a, _ = parseAnswer([]byte(bad))
	if _, err := f.apply(t, a); err == nil {
		t.Fatal("foreign balancer accepted")
	}
	if f.m.GetHandler("p-user") == nil {
		t.Fatal("invalid set removed working set")
	}
	f.rt.cfg.maxOutbounds = 1
	a, _ = parseAnswer([]byte(raw))
	if _, err := f.apply(t, a); err == nil {
		t.Fatal("maxOutbounds not shared")
	}
}

func TestProviderRoadGroupsRequireTheirOwnBalancerFeature(t *testing.T) {
	f := newApplyFixture(t, time.Minute, func(p *ProviderConfig) { p.Apply.Balancers = []string{"auto", "backend"} })
	f.o.e.addSelector("backend", newSelectorSettings(&SelectorConfig{}))
	raw := `{"outbounds":[{"tag":"road","protocol":"vless","selection":{"balancer":"backend","group":"g"}}],"groups":[{"name":"g","cost":1,"probe":"check"}]}`
	a, _ := parseAnswer([]byte(raw))
	if _, err := f.apply(t, a); err == nil {
		t.Fatal("primary groups flag enabled secondary groups")
	}
	f.o.e.addSelector("backend", newSelectorSettings(&SelectorConfig{Features: &SelectorFeatureConfig{Groups: proto.Bool(true)}}))
	a, _ = parseAnswer([]byte(raw))
	if _, err := f.apply(t, a); err != nil {
		t.Fatal(err)
	}
}

func TestAdaptiveHeaderOwnership(t *testing.T) {
	name, value := AdaptiveHeader()
	if name != "Xray-Adaptive" || value != `xray="`+core.Version()+`", api=2, rev=4, provider=1` {
		t.Fatal(name, value)
	}
	for _, name := range []string{"Xray-Adaptive", "xray-adaptive", "XRAY-ADAPTIVE"} {
		if CheckHeaderName(name) == nil {
			t.Fatal("reserved header accepted")
		}
		raw, _ := json.Marshal(map[string]string{name: "forged"})
		if _, err := parseHeaders(raw); err == nil {
			t.Fatal("answer override accepted")
		}
		o := newObserver(context.Background(), nil)
		err := o.SetProviderHeaders("missing", map[string]string{name: "forged"})
		o.Close()
		if err == nil {
			t.Fatal("host override accepted")
		}
		pc := v2ProviderConfig()
		pc.Headers = map[string]string{name: "forged"}
		if validateProviderConfigurations(&Config{Providers: []*ProviderConfig{pc}}) == nil {
			t.Fatal("config override accepted")
		}
		pc.Headers = nil
		pc.Apply.AcceptedHeaders = []string{name}
		if validateProviderConfigurations(&Config{Providers: []*ProviderConfig{pc}}) == nil {
			t.Fatal("acceptedHeaders reserved name accepted")
		}
	}
}

func TestLastRefreshAttemptBound(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), nil, "a1")
	j := &refreshJob{p: te.prov(), trigger: triggerHost}
	for range 100 {
		j.recordAttempt(requestAttempt{Via: "direct", Mirror: 1, Code: "timeout"})
	}
	te.onRefreshDone(j, refreshOutcome{err: "failed"}, t0)
	r := te.providerRequest(te.prov(), j, at(time.Second))
	if len(r.LastRefresh.Attempts) != 16 || r.LastRefresh.AgoMs != 1000 || r.LastRefresh.IsOK {
		t.Fatal(r.LastRefresh)
	}
}

func TestProviderDefaultRangesAndOneSamplePerEvent(t *testing.T) {
	pc := v2ProviderConfig()
	pc.Features.Periodic = proto.Bool(true)
	pc.Features.NetworkChange = proto.Bool(true)
	pc.Features.CostAbove = proto.Bool(true)
	pc.Triggers = &ProviderTriggers{NetworkChangeMode: RefreshOnNetwork_REFRESH_ANY, WhenCostAbove: &CostTrigger{Cost: 10}}
	e := newEngine(newSettings(&Config{Providers: []*ProviderConfig{pc}}), 73)
	p := e.providers[0]
	if p.cfg.interval != 96*time.Minute || p.cfg.intervalMax != 144*time.Minute || p.cfg.costAboveAfter != 15*time.Minute || p.cfg.costAboveAfterMax != 25*time.Minute || p.cfg.networkSettle != 30*time.Second || p.cfg.networkSettleMax != 90*time.Second {
		t.Fatal(p.cfg)
	}
	e.providersOnNetworkChange(false, t0)
	first := p.networkDelay
	if first < 30*time.Second || first > 90*time.Second {
		t.Fatal(first)
	}
	for i := 1; i < 20; i++ {
		e.wantedRefresh(p, at(time.Duration(i)*time.Second))
		if p.networkDelay != first {
			t.Fatal("network delay redrawn on tick")
		}
	}
	pc.Triggers.Interval = &DurationRange{}
	zero := newProvider(newProviderSettings(pc))
	e.scheduleRefresh(zero, 0, false, t0)
	if !zero.nextAt.Equal(t0) || zero.nextTrigger != triggerInterval {
		t.Fatal("explicit zero periodic interval collapsed")
	}
}

func TestProviderExhaustedDelayCountsFromOriginalOccurrence(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	p := te.prov()
	p.cfg.whenExhausted = true
	p.cfg.exhaustedDelay = durationRange{10 * time.Second, 10 * time.Second}
	te.sel.exhausted = true
	expectNoRefresh(t, te, t0)
	j := expectRefresh(t, te, at(10*time.Second), "exhausted")
	te.onRefreshDone(j, refreshOutcome{err: "failed"}, at(11*time.Second))
	want, _, _ := te.wantedRefresh(p, at(12*time.Second))
	if !want[triggerExhausted] {
		t.Fatal("persistent exhausted condition received a second event delay")
	}
}

func TestApplyRejectionNeverChangesAnotherRoadAfterTraceFills(t *testing.T) {
	for _, count := range []int{15, 16, 17} {
		j := &refreshJob{}
		for range count {
			j.recordAttempt(requestAttempt{Via: "failed", Mirror: 1, Code: "timeout"})
		}
		j.recordAttempt(requestAttempt{Via: "final", Mirror: 2, Status: 200, IsOK: true})
		j.rejectLastAttempt("invalid_set")
		last := j.attempts[len(j.attempts)-1]
		if count < 16 {
			if last.Via != "final" || last.Code != "invalid_set" || last.IsOK {
				t.Fatal(last)
			}
		} else if last.Via != "failed" || last.Code != "timeout" || last.Status != 0 {
			t.Fatalf("another road was blamed: %+v", last)
		}
	}
}

// Clients whose refreshes failed spread wider when their balancer runs dry
// again, up to a minute; the first exhaustion keeps the delay of the config.
func TestExhaustedDelayWidensWithFailedRefreshes(t *testing.T) {
	for failures, want := range map[int]time.Duration{0: 10 * time.Second, 1: 20 * time.Second, 2: 40 * time.Second, 3: time.Minute, 40: time.Minute} {
		if got := exhaustedSpread(10*time.Second, failures); got != want {
			t.Errorf("%d failures: %v, want %v", failures, got, want)
		}
	}
	if got := exhaustedSpread(2*time.Minute, 3); got != 2*time.Minute {
		t.Errorf("a wider delay of the config narrowed to %v", got)
	}
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	p := te.prov()
	p.cfg.whenExhausted = true
	p.cfg.exhaustedDelay = durationRange{0, 10 * time.Second}
	te.sel.exhausted = true
	p.failures = 3
	longest := time.Duration(0)
	for range 50 {
		p.delayed[triggerExhausted] = time.Time{}
		te.wantedRefresh(p, t0)
		longest = max(longest, p.delayed[triggerExhausted].Sub(t0))
	}
	if longest <= 10*time.Second || longest > time.Minute {
		t.Fatalf("longest delay after three failures %v", longest)
	}
}
