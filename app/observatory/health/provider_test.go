package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// quietProvider has every trigger off, so a test turns on only the one it
// checks and sees that nothing else fires.
func quietProvider(mod func(*ProviderConfig)) *ProviderConfig {
	pc := &ProviderConfig{
		Tag: "fresh",
		Url: "https://provider.test/set",
		Triggers: &ProviderTriggers{
			Interval:          -1,
			MinHealthy:        -1,
			SkipWhenExhausted: true,
			MaxAge:            -1,
			OnNetworkChange:   RefreshOnNetwork_REFRESH_NEVER,
		},
		Limits: &ProviderLimits{},
		Apply:  &ProviderApply{Balancer: "auto", TagPrefix: "p-"},
	}
	if mod != nil {
		mod(pc)
	}
	return pc
}

func newProviderEngine(t *testing.T, pc *ProviderConfig, sc *SelectorConfig, candidates ...string) *testEngine {
	t.Helper()
	cfg := &Config{
		Providers: []*ProviderConfig{pc},
		Schedule:  &ScheduleConfig{PauseWhenIdle: int64(time.Hour)},
	}
	te := newTestEngine(t, cfg, sc, candidates...)
	te.tickProviders(t0)
	return te
}

func (te *testEngine) prov() *provider { return te.providers[0] }

// traffic marks user demand at now, as connections through the balancer do.
func (te *testEngine) traffic(now time.Time) {
	te.noteDemand(now)
}

// refreshAt ticks the providers with traffic at now and returns the
// triggers of refreshes that started.
func (te *testEngine) refreshAt(now time.Time) ([]*refreshJob, time.Time) {
	te.traffic(now)
	return te.tickProviders(now)
}

func triggers(jobs []*refreshJob) []string {
	out := []string{}
	for _, j := range jobs {
		out = append(out, j.trigger.String())
	}
	return out
}

func (te *testEngine) eventsOf(t *testing.T, typ string) []Event {
	t.Helper()
	raw, _ := te.engine.events.drain()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		t.Fatal(err)
	}
	var out []Event
	for _, e := range evs {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

func expectRefresh(t *testing.T, te *testEngine, now time.Time, want string) *refreshJob {
	t.Helper()
	jobs, _ := te.refreshAt(now)
	if len(jobs) != 1 || jobs[0].trigger.String() != want {
		t.Fatalf("at %v: refreshes %v, want [%s]", now.Sub(t0), triggers(jobs), want)
	}
	return jobs[0]
}

func expectNoRefresh(t *testing.T, te *testEngine, now time.Time) {
	t.Helper()
	if jobs, _ := te.refreshAt(now); len(jobs) != 0 {
		t.Fatalf("at %v: unexpected refreshes %v", now.Sub(t0), triggers(jobs))
	}
}

func TestQuietProviderNeverRefreshes(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("a", 1, "d", 50)}, "a1", "d1")
	for h := 0; h < 48; h++ {
		now := at(time.Duration(h) * time.Hour)
		if h == 10 {
			te.exhaust(te.sel, now)
		}
		if h == 20 {
			te.onNetworkChange(fmt.Sprint("net", h), now)
		}
		expectNoRefresh(t, te, now)
	}
}

func TestIntervalRefreshIsJittered(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.Interval = int64(2 * time.Hour)
		p.Triggers.Jitter = 0.2
	}), nil, "a1")
	jobs, wake := te.refreshAt(t0)
	if len(jobs) != 0 {
		t.Fatalf("refreshed at start: %v", triggers(jobs))
	}
	if d := wake.Sub(t0); d < 96*time.Minute || d > 144*time.Minute {
		t.Fatalf("first refresh in %v, want 2h +-20%%", d)
	}
	expectNoRefresh(t, te, wake.Add(-time.Second))
	j := expectRefresh(t, te, wake, "interval")
	te.onRefreshDone(j, refreshOutcome{ok: true, changed: true}, wake)
	_, next := te.refreshAt(wake)
	if d := next.Sub(wake); d < 96*time.Minute || d > 144*time.Minute {
		t.Fatalf("next refresh in %v, want 2h +-20%%", d)
	}
}

func TestTimedRefreshWaitsForTraffic(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.Interval = int64(time.Hour)
		p.Triggers.Jitter = -1
	}), nil, "a1")
	te.s.schedule.pauseWhenIdle = 2 * time.Minute
	te.refreshAt(t0)
	// No traffic since t0: due, but nobody needs it now and no timer is set.
	jobs, wake := te.tickProviders(at(time.Hour))
	if len(jobs) != 0 || !wake.IsZero() {
		t.Fatalf("idle refresh %v, wake %v", triggers(jobs), wake)
	}
	if skips := te.eventsOf(t, "provider_skip"); len(skips) != 1 || skips[0].Fields["reason"] != "idle" {
		t.Fatalf("want one idle skip, got %+v", skips)
	}
	expectRefresh(t, te, at(90*time.Minute), "interval")
}

func TestRefreshWaitsForTheNetworkButNotTheHost(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.Interval = int64(time.Hour)
		p.Triggers.Jitter = -1
	}), nil, "a1")
	te.onNetworkLost(at(30 * time.Minute))
	expectNoRefresh(t, te, at(time.Hour))
	if skips := te.eventsOf(t, "provider_skip"); len(skips) != 1 || skips[0].Fields["reason"] != "network_down" {
		t.Fatalf("want one network_down skip, got %+v", skips)
	}
	te.requestRefresh("fresh")
	j := expectRefresh(t, te, at(time.Hour+time.Second), "host")
	te.onRefreshDone(j, refreshOutcome{err: "timeout"}, at(time.Hour+time.Second))
	te.onNetworkChange("lte", at(time.Hour+time.Minute))
	expectRefresh(t, te, at(time.Hour+2*time.Minute), "host")
}

// While the balancer of the provider is pinned, by the host or by the start,
// every trigger but the host waits for the release: the host decides about
// the outbound it pinned, and a refresh would only load the backend.
func TestRefreshWaitsForThePinButNotTheHost(t *testing.T) {
	for _, start := range []bool{false, true} {
		name := "pin of the host"
		if start {
			name = "start pin"
		}
		t.Run(name, func(t *testing.T) {
			sc := &SelectorConfig{Costs: costs("a", 1, "b", 3)}
			if start {
				sc.StartPinned = "a1"
			}
			te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
				p.Triggers.Interval = int64(time.Hour)
				p.Triggers.Jitter = -1
				p.Triggers.MinHealthy = 2
			}), sc, "a1", "b1")
			if !start {
				te.pin("auto", "a1", at(time.Second))
			}
			// One candidate is left: min_healthy wants a refresh at once,
			// interval after an hour.
			te.path("b1").status = statusDown
			expectNoRefresh(t, te, at(2*time.Second))
			expectNoRefresh(t, te, at(time.Hour))
			var held []string
			for _, e := range te.eventsOf(t, "provider_skip") {
				if e.Fields["reason"] != "pinned" {
					t.Fatalf("skip %v, want pinned", e.Fields)
				}
				held = append(held, e.Fields["trigger"].(string))
			}
			if fmt.Sprint(held) != "[min_healthy interval]" {
				t.Fatalf("triggers held by the pin %v", held)
			}
			te.requestRefresh("fresh")
			j := expectRefresh(t, te, at(time.Hour+time.Second), "host")
			te.onRefreshDone(j, refreshOutcome{ok: true}, at(time.Hour+time.Second))
			expectNoRefresh(t, te, at(3*time.Hour))
			te.release("auto", at(3*time.Hour))
			expectRefresh(t, te, at(3*time.Hour), "interval")
		})
	}
}

// A lease due for renewal waits for the release as well, although it does
// not wait for traffic.
func TestRenewalWaitsForThePin(t *testing.T) {
	te := leaseEngine(t, renewing(func(x *ExpiryTrigger) { x.Jitter = -1 }),
		&SelectorConfig{Costs: costs("p-", 1)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1", "p-x2")
	te.pin("auto", "p-x1", at(time.Second))
	if jobs, _ := te.tickProviders(at(40 * time.Second)); len(jobs) != 0 {
		t.Fatalf("idle under the pin: refreshes %v", triggers(jobs))
	}
	if skips := te.eventsOf(t, "provider_skip"); len(skips) != 1 || skips[0].Fields["reason"] != "pinned" ||
		skips[0].Fields["trigger"] != "expiry" {
		t.Fatalf("want one pinned skip of the renewal, got %+v", skips)
	}
	te.release("auto", at(41*time.Second))
	if jobs, _ := te.tickProviders(at(41 * time.Second)); len(jobs) != 1 || jobs[0].trigger != triggerExpiry {
		t.Fatalf("after the release: refreshes %v, want the renewal", triggers(jobs))
	}
}

func TestMinHealthyFiresOnceBelowThreshold(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.MinHealthy = 2
		p.Limits.MinInterval = int64(time.Minute)
	}), &SelectorConfig{Costs: costs("a", 1)}, "a1", "a2", "a3")
	expectNoRefresh(t, te, t0)
	te.path("a3").status = statusDown
	expectNoRefresh(t, te, at(time.Second))
	te.path("a2").status = statusSuspect
	j := expectRefresh(t, te, at(2*time.Second), "min_healthy")
	te.onRefreshDone(j, refreshOutcome{ok: true, changed: true}, at(3*time.Second))
	// Still below the threshold after the refresh: no refresh in a loop.
	expectNoRefresh(t, te, at(10*time.Minute))
	// Back above it re-arms the trigger.
	te.path("a2").status = statusUp
	expectNoRefresh(t, te, at(11*time.Minute))
	te.path("a2").status = statusDown
	expectRefresh(t, te, at(12*time.Minute), "min_healthy")
}

func TestMinHealthyRetriesAfterFailure(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.MinHealthy = 2
		p.Limits.MinInterval = int64(time.Minute)
	}), nil, "a1", "a2")
	te.path("a2").status = statusDown
	j := expectRefresh(t, te, t0, "min_healthy")
	te.onRefreshDone(j, refreshOutcome{err: "timeout"}, at(time.Second))
	expectNoRefresh(t, te, at(30*time.Second))
	expectRefresh(t, te, at(2*time.Minute), "min_healthy")
}

func TestExhaustedRefreshesAtOnceWhileNetworkIsUp(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.SkipWhenExhausted = false
	}), nil, "a1", "a2")
	expectNoRefresh(t, te, t0)
	te.networkDown = true
	te.exhaust(te.sel, at(time.Second))
	expectNoRefresh(t, te, at(2*time.Second))
	te.networkDown = false
	j := expectRefresh(t, te, at(3*time.Second), "exhausted")
	if j.retry != 0 {
		t.Fatalf("first refresh counted as retry %d", j.retry)
	}
	// A failure is retried after the backoff, not after exhaustedMinInterval alone.
	te.onRefreshDone(j, refreshOutcome{err: "status 500"}, at(4*time.Second))
	expectNoRefresh(t, te, at(20*time.Second))
	j = expectRefresh(t, te, at(50*time.Second), "exhausted")
	if j.retry != 1 {
		t.Fatalf("retry %d, want 1", j.retry)
	}
}

func TestExhaustedWaitsUntilTheBalancerSawTheNewSet(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.SkipWhenExhausted = false
	}), &SelectorConfig{Costs: costs("a", 1, "p-", 2)}, "a1")
	te.failN(3, "a1", failDial, t0)
	if !te.sel.exhausted {
		t.Fatalf("setup: active %q, want exhausted", te.sel.current())
	}
	j := expectRefresh(t, te, t0, "exhausted")
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-n1"}, added: []string{"p-n1"}}, at(time.Second))
	te.onRefreshDone(j, refreshOutcome{ok: true, changed: true}, at(time.Second))
	// No connection through the balancer since the apply: it is exhausted
	// only by the old set.
	expectNoRefresh(t, te, at(time.Minute))
	te.updateCandidates(te.sel, []string{"a1", "p-n1"}, at(61*time.Second))
	if te.sel.current() != "p-n1" {
		t.Fatalf("active %q, want the new p-n1", te.sel.current())
	}
	te.failN(3, "p-n1", failDial, at(62*time.Second))
	if !te.sel.exhausted {
		t.Fatalf("setup: active %q, want exhausted again", te.sel.current())
	}
	expectRefresh(t, te, at(63*time.Second), "exhausted")
}

func TestSameSetWhileExhaustedBacksOff(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.SkipWhenExhausted = false
		p.Limits.MaxPerHour = -1
	}), nil, "a1")
	te.exhaust(te.sel, t0)
	now := t0
	var gaps []time.Duration
	last := now
	for len(gaps) < 4 {
		jobs, _ := te.refreshAt(now)
		if len(jobs) == 1 {
			if now != t0 {
				gaps = append(gaps, now.Sub(last))
			}
			last = now
			te.onRefreshDone(jobs[0], refreshOutcome{ok: true, changed: false}, now)
		}
		now = now.Add(time.Second)
	}
	for i := 1; i < len(gaps); i++ {
		if gaps[i] <= gaps[i-1] {
			t.Fatalf("gaps between refreshes of the same dead set do not grow: %v", gaps)
		}
	}
}

func TestCostAboveFiresAfterTimeOnExpensive(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.CostAbove = &CostTrigger{Cost: 10, After: int64(15 * time.Minute)}
	}), &SelectorConfig{Costs: costs("a", 1, "d", 50)}, "a1", "d1")
	expectNoRefresh(t, te, at(30*time.Minute))
	te.switchTo(te.sel, "d1", "failover", at(time.Hour))
	_, wake := te.refreshAt(at(time.Hour))
	if wake != at(75*time.Minute) {
		t.Fatalf("cost trigger due at %v, want 1h15m", wake.Sub(t0))
	}
	expectNoRefresh(t, te, at(74*time.Minute))
	j := expectRefresh(t, te, at(75*time.Minute), "cost_above")
	te.onRefreshDone(j, refreshOutcome{ok: true, changed: true}, at(75*time.Minute))
	// Still expensive: the clock starts again from the refresh.
	expectNoRefresh(t, te, at(89*time.Minute))
	expectRefresh(t, te, at(90*time.Minute), "cost_above")
}

func TestCostAboveForgetsTimeOnCheapPath(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.CostAbove = &CostTrigger{Cost: 10, After: int64(15 * time.Minute)}
	}), &SelectorConfig{Costs: costs("a", 1, "d", 50)}, "a1", "d1")
	te.switchTo(te.sel, "d1", "failover", t0)
	expectNoRefresh(t, te, at(10*time.Minute))
	te.switchTo(te.sel, "a1", "upgrade", at(11*time.Minute))
	expectNoRefresh(t, te, at(12*time.Minute))
	te.switchTo(te.sel, "d1", "failover", at(13*time.Minute))
	expectNoRefresh(t, te, at(20*time.Minute))
	expectRefresh(t, te, at(28*time.Minute), "cost_above")
}

func TestMaxAgeKeepsAskingUntilASuccess(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.MaxAge = int64(6 * time.Hour)
	}), nil, "a1")
	expectNoRefresh(t, te, at(6*time.Hour-time.Second))
	j := expectRefresh(t, te, at(6*time.Hour), "max_age")
	te.onRefreshDone(j, refreshOutcome{err: "timeout"}, at(6*time.Hour))
	j = expectRefresh(t, te, at(6*time.Hour+5*time.Minute), "max_age")
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(6*time.Hour+5*time.Minute))
	expectNoRefresh(t, te, at(12*time.Hour))
	expectRefresh(t, te, at(12*time.Hour+5*time.Minute), "max_age")
}

func TestNextRefreshInReplacesInterval(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.Interval = int64(2 * time.Hour)
		p.Triggers.Jitter = 0.1
		p.Limits.MinInterval = int64(time.Minute)
	}), nil, "a1")
	te.prov().host = true
	j := expectRefresh(t, te, t0, "host")
	te.onRefreshDone(j, refreshOutcome{ok: true, nextRefreshIn: 10 * time.Minute}, t0)
	_, wake := te.refreshAt(t0)
	if d := wake.Sub(t0); d < 9*time.Minute || d > 11*time.Minute {
		t.Fatalf("next refresh in %v, want 10m +-10%%", d)
	}
	expectNoRefresh(t, te, wake.Add(-time.Second))
	j = expectRefresh(t, te, wake, "next_refresh")
	// An answer without the hint brings the interval back.
	te.onRefreshDone(j, refreshOutcome{ok: true}, wake)
	_, next := te.refreshAt(wake)
	if d := next.Sub(wake); d < 108*time.Minute || d > 132*time.Minute {
		t.Fatalf("refresh after an answer without hint in %v, want 2h +-10%%", d)
	}
}

func TestExhaustedUsesShorterMinInterval(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.SkipWhenExhausted = false
		p.Triggers.MaxAge = int64(time.Hour)
		p.Limits.MinInterval = int64(5 * time.Minute)
		p.Limits.ExhaustedMinInterval = int64(30 * time.Second)
	}), nil, "a1")
	j := expectRefresh(t, te, at(time.Hour), "max_age")
	te.onRefreshDone(j, refreshOutcome{ok: true, changed: true}, at(time.Hour))
	te.failN(3, "a1", failDial, at(time.Hour+time.Second))
	if !te.sel.exhausted {
		t.Fatalf("setup: active %q, want exhausted", te.sel.current())
	}
	expectNoRefresh(t, te, at(time.Hour+29*time.Second))
	expectRefresh(t, te, at(time.Hour+30*time.Second), "exhausted")
}

func TestNetworkChangeTriggerModes(t *testing.T) {
	cases := []struct {
		mode      RefreshOnNetwork
		known     bool
		expensive bool
		want      bool
	}{
		{RefreshOnNetwork_REFRESH_UNKNOWN_NOT_CHEAPEST, false, true, true},
		{RefreshOnNetwork_REFRESH_UNKNOWN_NOT_CHEAPEST, false, false, false},
		{RefreshOnNetwork_REFRESH_UNKNOWN_NOT_CHEAPEST, true, true, false},
		{RefreshOnNetwork_REFRESH_UNKNOWN, false, false, true},
		{RefreshOnNetwork_REFRESH_UNKNOWN, true, true, false},
		{RefreshOnNetwork_REFRESH_ANY, true, false, true},
		{RefreshOnNetwork_REFRESH_NEVER, false, true, false},
	}
	for _, c := range cases {
		name := fmt.Sprintf("%v/known=%v/expensive=%v", c.mode, c.known, c.expensive)
		t.Run(name, func(t *testing.T) {
			te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
				p.Triggers.OnNetworkChange = c.mode
				p.Triggers.NetworkSettle = int64(30 * time.Second)
			}), &SelectorConfig{Costs: costs("a", 1, "b", 3), OnNetworkChange: NetworkChangeMode_FORGET}, "a1", "b1")
			if c.known {
				te.networks["cafe"] = map[string]string{"auto": "a1"}
			}
			te.onNetworkChange("cafe", at(time.Minute))
			if c.expensive {
				te.switchTo(te.sel, "b1", "failover", at(70*time.Second))
			}
			expectNoRefresh(t, te, at(80*time.Second))
			jobs, _ := te.refreshAt(at(90 * time.Second))
			if got := len(jobs) == 1 && jobs[0].trigger == triggerNetwork; got != c.want {
				t.Fatalf("refresh after settle: %v, want %v", triggers(jobs), c.want)
			}
		})
	}
}

func TestHostRequestIgnoresIdleButNotLimits(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), nil, "a1")
	if te.requestRefresh("other") {
		t.Fatal("unknown provider accepted")
	}
	if !te.requestRefresh("fresh") {
		t.Fatal("provider not found")
	}
	jobs, _ := te.tickProviders(at(5 * time.Hour))
	if len(jobs) != 1 || jobs[0].trigger != triggerHost {
		t.Fatalf("host request while idle: %v", triggers(jobs))
	}
	te.onRefreshDone(jobs[0], refreshOutcome{ok: true}, at(5*time.Hour))
	te.requestRefresh("")
	if jobs, wake := te.tickProviders(at(5*time.Hour + 10*time.Second)); len(jobs) != 0 || wake != at(5*time.Hour+30*time.Second) {
		t.Fatalf("second host request %v, wake %v; want to wait exhaustedMinInterval", triggers(jobs), wake.Sub(t0))
	}
	skips := te.eventsOf(t, "provider_skip")
	if len(skips) != 1 || skips[0].Fields["reason"] != "min_interval" || skips[0].Fields["trigger"] != "host" {
		t.Fatalf("want one min_interval skip of host, got %+v", skips)
	}
	jobs, _ = te.tickProviders(at(5*time.Hour + 30*time.Second))
	if len(jobs) != 1 {
		t.Fatalf("host request not served after the interval")
	}
}

func TestFailedHostRefreshRetriesWithGrowingJitteredBackoff(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Limits.MaxPerHour = -1
		p.Limits.ExhaustedMinInterval = int64(time.Second)
		p.Limits.Backoff = &Backoff{Initial: int64(30 * time.Second), Multiplier: 2, Max: int64(10 * time.Minute), Jitter: 0.3}
	}), nil, "a1")
	te.requestRefresh("fresh")
	var starts []time.Time
	for now := t0; now.Before(at(2 * time.Hour)); now = now.Add(time.Second) {
		jobs, _ := te.refreshAt(now)
		for _, j := range jobs {
			starts = append(starts, now)
			te.onRefreshDone(j, refreshOutcome{err: "timeout"}, now)
		}
	}
	if len(starts) < 6 {
		t.Fatalf("only %d attempts", len(starts))
	}
	spread := 0
	for i := 1; i < len(starts); i++ {
		gap := starts[i].Sub(starts[i-1])
		base := (30 * time.Second) << (i - 1)
		if base > 10*time.Minute {
			base = 10 * time.Minute
		}
		lo, hi := time.Duration(float64(base)*0.7)-time.Second, time.Duration(float64(base)*1.3)+time.Second
		if gap < lo || gap > hi {
			t.Fatalf("gap %d is %v, want %v +-30%%", i, gap, base)
		}
		if d := gap - base; d > time.Second || d < -time.Second {
			spread++
		}
	}
	// Clients that failed together must not come back together.
	if spread < (len(starts)-1)/2 {
		t.Fatalf("only %d of %d pauses differ from the bare backoff: no jitter", spread, len(starts)-1)
	}
}

func TestMinIntervalHoldsTriggersAndSkipIsReportedOnce(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.Interval = int64(time.Minute)
		p.Triggers.Jitter = -1
		p.Limits.MinInterval = int64(5 * time.Minute)
	}), nil, "a1")
	j := expectRefresh(t, te, at(time.Minute), "interval")
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(time.Minute))
	te.eventsOf(t, "")
	for s := 2; s < 6; s++ {
		expectNoRefresh(t, te, at(time.Duration(s)*time.Minute))
	}
	if skips := te.eventsOf(t, "provider_skip"); len(skips) != 1 || skips[0].Fields["reason"] != "min_interval" {
		t.Fatalf("want one min_interval skip, got %+v", skips)
	}
	expectRefresh(t, te, at(6*time.Minute), "interval")
}

func TestMaxPerHour(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Limits.MaxPerHour = 3
		p.Limits.ExhaustedMinInterval = int64(time.Second)
	}), nil, "a1")
	var starts []time.Time
	for now := t0; now.Before(at(90 * time.Minute)); now = now.Add(10 * time.Second) {
		te.requestRefresh("fresh")
		jobs, _ := te.refreshAt(now)
		for _, j := range jobs {
			starts = append(starts, now)
			te.onRefreshDone(j, refreshOutcome{ok: true}, now)
		}
	}
	if len(starts) != 6 || starts[3].Sub(starts[0]) < time.Hour {
		t.Fatalf("starts %v, want 3 per hour", starts)
	}
	found := false
	for _, e := range te.eventsOf(t, "provider_skip") {
		found = found || e.Fields["reason"] == "max_per_hour"
	}
	if !found {
		t.Fatal("no max_per_hour skip event")
	}
}

func TestRefreshWhileFetchingWaits(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), nil, "a1")
	te.requestRefresh("fresh")
	j := expectRefresh(t, te, t0, "host")
	te.requestRefresh("fresh")
	expectNoRefresh(t, te, at(time.Minute))
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(time.Minute))
	// A request made during a successful refresh is answered by it.
	expectNoRefresh(t, te, at(2*time.Minute))
}

// 1000 clients started together with the same schedule must not come back
// together: no minute may get more than 5% of the refreshes of a wave.
func TestRefreshesOfManyClientsSpreadOut(t *testing.T) {
	const clients = 1000
	pc := quietProvider(func(p *ProviderConfig) {
		p.Triggers.Interval = int64(2 * time.Hour)
		p.Triggers.Jitter = 0.2
	})
	byMinute := map[int]int{}
	waves := 0
	for c := 0; c < clients; c++ {
		s := newSettings(&Config{Providers: []*ProviderConfig{pc}, Schedule: &ScheduleConfig{PauseWhenIdle: int64(24 * time.Hour)}})
		e := newEngine(s, int64(c+1))
		e.noteDemand(t0)
		_, wake := e.tickProviders(t0)
		n := 0
		for wake.Before(at(12 * time.Hour)) {
			jobs, _ := e.tickProviders(wake)
			if len(jobs) != 1 {
				t.Fatalf("client %d: no refresh at its wake time", c)
			}
			byMinute[int(wake.Sub(t0)/time.Minute)]++
			e.onRefreshDone(jobs[0], refreshOutcome{ok: true}, wake)
			_, wake = e.tickProviders(wake)
			n++
		}
		waves = max(waves, n)
	}
	peak, minute := 0, 0
	for m, n := range byMinute {
		if n > peak {
			peak, minute = n, m
		}
	}
	if peak > clients*5/100 {
		t.Fatalf("minute %d got %d refreshes of %d clients", minute, peak, clients)
	}
	t.Logf("%d waves, busiest minute %d with %d refreshes (%.1f%%)", waves, minute, peak, float64(peak)*100/clients)
}

func TestProviderCostsRankCandidates(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("a", 1, "p-", 5)}, "a1")
	te.failN(3, "a1", failDial, t0)
	te.run(t, at(time.Second), nil)
	if !te.sel.exhausted {
		t.Fatalf("setup: want exhausted, active %q", te.sel.current())
	}
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x", "p-y"}, costs: map[string]float64{"p-y": 2},
		added: []string{"p-x", "p-y"}}, at(2*time.Second))
	te.updateCandidates(te.sel, []string{"a1", "p-x", "p-y"}, at(2*time.Second))
	if te.sel.current() != "p-y" {
		t.Fatalf("active %q, want p-y by the cost the provider sent", te.sel.current())
	}
	if c := te.cost(te.sel, "p-x"); c != 5 {
		t.Fatalf("p-x cost %v, want 5 from the balancer config", c)
	}
}

func TestRetiredOutboundHandsOverAfterProbe(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-x", 1, "p-y", 2)}, "p-x", "p-y")
	if te.sel.current() != "p-x" {
		t.Fatalf("setup: active %q", te.sel.current())
	}
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-y"}, retire: []string{"p-x"}}, at(time.Second))
	if te.sel.current() != "p-x" {
		t.Fatalf("left the retiring outbound before probing the next one")
	}
	jobs := te.run(t, at(2*time.Second), nil)
	if len(jobs) != 1 || jobs[0].tag != "p-y" {
		t.Fatalf("probes %+v, want one of p-y", jobs)
	}
	sw := te.eventsOf(t, "switch")
	last := sw[len(sw)-1]
	if te.sel.current() != "p-y" || last.Fields["reason"] != "retired" {
		t.Fatalf("active %q after %+v", te.sel.current(), last)
	}
	// The retiring outbound is cheaper but no longer a candidate: no return to it.
	te.traffic(at(3 * time.Hour))
	for _, j := range te.run(t, at(3*time.Hour), nil) {
		t.Fatalf("probe %s of %s while p-x retires", j.reason, j.tag)
	}
	if te.sel.current() != "p-y" {
		t.Fatalf("returned to the retiring %q", te.sel.current())
	}
}

// A balancer that leaves an outbound that left its set tells so also when
// the outbound failed meanwhile, as it did before leases; only an expired
// one that failed is left as a failover.
func TestRetiredOutboundThatFailedIsLeftAsRetired(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-x", 1, "p-y", 2)}, "p-x", "p-y")
	te.failN(3, "p-x", failDial, t0)
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-y"}, retire: []string{"p-x"}}, at(time.Second))
	if te.path("p-x").status != statusDown || te.sel.current() != "p-x" {
		t.Fatalf("setup: p-x %v, active %q", te.path("p-x").status, te.sel.current())
	}
	te.run(t, at(2*time.Second), nil)
	sw := te.eventsOf(t, "switch")
	if te.sel.current() != "p-y" || sw[len(sw)-1].Fields["reason"] != "retired" {
		t.Fatalf("active %q after %+v", te.sel.current(), sw[len(sw)-1].Fields)
	}
}

func TestRetiredOutboundStaysWhenNothingElseWorks(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-", 1)}, "p-x", "p-y")
	te.applyProviderSet(te.prov(), providerChange{retire: []string{"p-x"}}, at(time.Second))
	te.run(t, at(2*time.Second), map[string]bool{"p-y": false})
	if te.sel.current() != "p-x" || te.sel.exhausted {
		t.Fatalf("active %q exhausted %v, want to stay on the retiring p-x", te.sel.current(), te.sel.exhausted)
	}
	// After the drain the outbound is gone and the balancer has nothing left.
	te.dropProviderOutbound("p-x", at(time.Minute))
	if te.sel.current() != "" || !te.sel.exhausted {
		t.Fatalf("active %q after the drain, want exhausted", te.sel.current())
	}
}

func TestDrainEndsAfterTheFailedCandidateRested(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-", 1)}, "p-x", "p-y")
	te.applyProviderSet(te.prov(), providerChange{retire: []string{"p-x"}}, at(time.Second))
	te.run(t, at(2*time.Second), map[string]bool{"p-y": false})
	// p-y rested its quarantine before the drain of p-x ended: the failover
	// tries it again instead of giving up.
	te.eventsOf(t, "")
	te.dropProviderOutbound("p-x", at(10*time.Minute))
	te.run(t, at(10*time.Minute), nil)
	if te.sel.current() != "p-y" || len(te.eventsOf(t, "exhausted")) != 0 {
		t.Fatalf("active %q after the drain, want p-y without being exhausted on the way", te.sel.current())
	}
}

func TestRevivedOutboundIsACandidateAgain(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-x", 1, "p-y", 2)}, "p-x", "p-y")
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-y"}, retire: []string{"p-x"}}, at(time.Second))
	te.run(t, at(2*time.Second), nil)
	if te.sel.current() != "p-y" {
		t.Fatalf("setup: active %q", te.sel.current())
	}
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x", "p-y"}, revive: []string{"p-x"}}, at(time.Minute))
	if !te.sel.hasCandidate("p-x") {
		t.Fatalf("revived p-x is not a candidate: %v", te.sel.candidates)
	}
}

func TestReplacedOutboundIsJudgedAfresh(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-x", 1, "p-y", 2)}, "p-x", "p-y")
	te.failN(3, "p-x", failDial, t0)
	if te.path("p-x").status != statusDown {
		t.Fatalf("setup: p-x %v", te.path("p-x").status)
	}
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x", "p-y"}, reset: []string{"p-x"}}, at(time.Second))
	if p := te.path("p-x"); p.status != statusUnknown || p.quarantined(at(time.Second)) {
		t.Fatalf("replaced p-x is %v, quarantined %v", p.status, p.quarantined(at(time.Second)))
	}
}

func TestSwappedHandlerDoesNotMoveTheBalancer(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-x", 1, "p-y", 2)}, "p-x", "p-y")
	te.replacingTags([]string{"p-x"})
	// A connection between the removal of the old handler and the
	// registration of the new one sees p-x missing.
	te.updateCandidates(te.sel, []string{"p-y"}, at(time.Second))
	if te.sel.current() != "p-x" || !te.sel.offers("p-x") {
		t.Fatalf("active %q, candidates %v: the balancer left an outbound being swapped", te.sel.current(), te.sel.candidates)
	}
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x", "p-y"}, reset: []string{"p-x"}}, at(time.Second))
	te.updateCandidates(te.sel, []string{"p-y"}, at(2*time.Second))
	te.run(t, at(2*time.Second), nil)
	if te.sel.current() != "p-y" {
		t.Fatalf("active %q: an outbound that really left is still chosen", te.sel.current())
	}
}

func TestReleaseLeavesARetiredPinnedOutbound(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-", 1), StartPinned: "p-x"}, "p-x", "p-y")
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-y"}, retire: []string{"p-x"}}, at(time.Second))
	te.run(t, at(2*time.Second), nil)
	if te.sel.current() != "p-x" {
		t.Fatalf("pinned balancer moved to %q", te.sel.current())
	}
	te.release("auto", at(time.Minute))
	te.run(t, at(time.Minute), nil)
	sw := te.eventsOf(t, "switch")
	if te.sel.current() != "p-y" || sw[len(sw)-1].Fields["reason"] != "retired" {
		t.Fatalf("after release active %q, want p-y instead of the retired p-x", te.sel.current())
	}
}

func TestReleaseKeepsARetiredPinnedOutboundWhenNothingElseWorks(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-", 1), StartPinned: "p-x"}, "p-x", "p-y")
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-y"}, retire: []string{"p-x"}}, at(time.Second))
	te.failN(3, "p-y", failDial, at(30*time.Second))
	if te.path("p-y").status != statusDown {
		t.Fatalf("setup: p-y %v", te.path("p-y").status)
	}
	te.release("auto", at(time.Minute))
	te.run(t, at(time.Minute), nil)
	if te.sel.current() != "p-x" || te.sel.exhausted {
		t.Fatalf("after release active %q exhausted %v, want to stay on the working p-x", te.sel.current(), te.sel.exhausted)
	}
}

func TestResolveVia(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("a", 1)}, "a1", "a2")
	if tag, skip := te.resolveVia("auto", t0); tag != "a1" || skip != "" {
		t.Fatalf("balancer resolved to %q %q", tag, skip)
	}
	if tag, skip := te.resolveVia("direct", t0); tag != "direct" || skip != "" {
		t.Fatalf("outbound resolved to %q %q", tag, skip)
	}
	te.failN(3, "a2", failDial, t0)
	if _, skip := te.resolveVia("a2", t0); skip == "" {
		t.Fatal("a quarantined outbound is not skipped")
	}
	te.exhaust(te.sel, t0)
	if _, skip := te.resolveVia("auto", t0); skip == "" {
		t.Fatal("an exhausted balancer is not skipped")
	}
}

func TestProviderDefaultsAndOff(t *testing.T) {
	p := newProviderSettings(&ProviderConfig{Apply: &ProviderApply{Balancer: "auto", TagPrefix: "p-"}})
	if p.interval != 2*time.Hour || p.jitter != 0.2 || p.minHealthy != 2 || !p.whenExhausted || p.costAbove ||
		p.maxAge != 6*time.Hour || p.onNetwork != RefreshOnNetwork_REFRESH_UNKNOWN_NOT_CHEAPEST {
		t.Fatalf("trigger defaults %+v", p)
	}
	if p.minInterval != 5*time.Minute || p.exhaustedMinInterval != 30*time.Second || p.maxPerHour != 6 ||
		p.backoff != defaultRefreshRetry || p.drain != 5*time.Minute || p.timeout != 10*time.Second {
		t.Fatalf("limit defaults %+v", p)
	}
	off := newProviderSettings(quietProvider(func(p *ProviderConfig) {
		p.Triggers.Jitter = -1
		p.Limits.MaxPerHour = -1
	}))
	if off.interval != 0 || off.jitter != 0 || off.minHealthy != 0 || off.whenExhausted || off.maxAge != 0 || off.maxPerHour != 0 {
		t.Fatalf("off %+v", off)
	}
	s := newSettings(&Config{SubjectSelector: []string{"a"}, Providers: []*ProviderConfig{quietProvider(nil)}})
	if !s.observes("p-x1") {
		t.Fatal("outbounds of a provider are not observed")
	}
}

func TestRetryAfterForms(t *testing.T) {
	wall := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	// The device clock runs an hour ahead of the server.
	date := wall.Add(-time.Hour)
	for _, tc := range []struct {
		name, value string
		date        time.Time
		want        time.Duration
	}{
		{"seconds", "120", date, 2 * time.Minute},
		{"seconds with spaces", " 7 ", time.Time{}, 7 * time.Second},
		{"date from Date", date.Add(90 * time.Second).Format(http.TimeFormat), date, 90 * time.Second},
		{"date without Date", wall.Add(time.Minute).Format(http.TimeFormat), time.Time{}, time.Minute},
		{"date gone by", date.Add(-time.Minute).Format(http.TimeFormat), date, 0},
		{"zero", "0", date, 0},
		{"negative", "-5", date, 0},
		{"garbage", "soon", date, 0},
		{"none", "", date, 0},
		{"far", "9999999999", date, 24 * time.Hour},
	} {
		if got := parseRetryAfter(tc.value, tc.date, wall); got != tc.want {
			t.Errorf("%s: %q gives %v, want %v", tc.name, tc.value, got, tc.want)
		}
	}
}

// A busy backend holds every trigger, the host and an exhausted balancer
// too, and the pause is no failure: the backoff does not grow.
func TestRetryAfterHoldsEveryTriggerWithoutGrowingBackoff(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.SkipWhenExhausted = false
		p.Limits.ExhaustedMinInterval = int64(time.Second)
	}), nil, "a1")
	te.exhaust(te.sel, t0)
	j := expectRefresh(t, te, at(time.Second), "exhausted")
	te.onRefreshDone(j, refreshOutcome{err: "status 429", retryAfter: time.Minute}, at(2*time.Second))
	p := te.prov()
	until := p.retryAfterUntil
	if p.failures != 0 || !p.backoffUntil.IsZero() || until.Before(at(62*time.Second)) || until.After(at(80*time.Second)) {
		t.Fatalf("failures %d, backoff until %v, wait until %v", p.failures, p.backoffUntil, until.Sub(t0))
	}
	expectNoRefresh(t, te, at(3*time.Second))
	te.requestRefresh("fresh")
	for now := at(4 * time.Second); now.Before(until); now = now.Add(5 * time.Second) {
		if jobs, wake := te.refreshAt(now); len(jobs) != 0 || !wake.Equal(until) {
			t.Fatalf("at %v: refreshes %v, wake %v; want none until %v", now.Sub(t0), triggers(jobs), wake.Sub(t0), until.Sub(t0))
		}
	}
	var held []string
	for _, e := range te.eventsOf(t, "provider_skip") {
		held = append(held, fmt.Sprint(e.Fields["trigger"], ":", e.Fields["reason"]))
	}
	if fmt.Sprint(held) != "[exhausted:retry_after host:retry_after]" {
		t.Fatalf("skips %v, want one retry_after per trigger", held)
	}
	if j := expectRefresh(t, te, until, "host"); j.retry != 0 {
		t.Fatalf("the refresh after the pause counts %d failures", j.retry)
	}
}

// The spread only adds to the pause: no client comes back sooner than the
// backend asked.
func TestRetryAfterJitterOnlyLater(t *testing.T) {
	shortest, longest := time.Hour, time.Duration(0)
	for seed := int64(1); seed <= 500; seed++ {
		e := newEngine(newSettings(&Config{Providers: []*ProviderConfig{quietProvider(nil)}}), seed)
		p := e.providers[0]
		e.onRefreshDone(&refreshJob{p: p, trigger: triggerHost}, refreshOutcome{err: "status 503", retryAfter: 10 * time.Second}, t0)
		wait := p.retryAfterUntil.Sub(t0)
		shortest, longest = min(shortest, wait), max(longest, wait)
	}
	if shortest < 10*time.Second || longest > 13*time.Second || shortest > 10500*time.Millisecond || longest < 12500*time.Millisecond {
		t.Fatalf("waits from %v to %v, want 10 s to 13 s, spread upward over the whole range", shortest, longest)
	}
}

// 1000 clients told the same pause at once must not come back at once.
func TestRetryAfterOfManyClientsSpreadOut(t *testing.T) {
	const clients = 1000
	bySecond := map[int]int{}
	for c := 0; c < clients; c++ {
		e := newEngine(newSettings(&Config{Providers: []*ProviderConfig{quietProvider(nil)}}), int64(c+1))
		p := e.providers[0]
		e.onRefreshDone(&refreshJob{p: p, trigger: triggerExhausted}, refreshOutcome{err: "status 429", retryAfter: time.Minute}, t0)
		back := p.retryAfterUntil.Sub(t0)
		if back < time.Minute || back > 78*time.Second {
			t.Fatalf("client %d comes back after %v, want 60-78 s", c, back)
		}
		bySecond[int(back/time.Second)]++
	}
	for sec, n := range bySecond {
		if n > clients/10 {
			t.Fatalf("second %d gets %d of %d clients", sec, n, clients)
		}
	}
}
