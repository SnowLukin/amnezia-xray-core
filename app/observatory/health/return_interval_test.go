package health

import (
	"encoding/json"
	"math"
	"slices"
	"testing"
	"time"
)

func TestCostReturnIntervalUsesFirstMatchingEntry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []*CostEntry
		want    time.Duration
	}{
		{"own interval", []*CostEntry{{Match: "b", Cost: 3, ReturnInterval: int64(90 * time.Second)}}, 90 * time.Second},
		{"absent interval", []*CostEntry{{Match: "b", Cost: 3}}, 5 * time.Minute},
		{"explicit zero", []*CostEntry{{Match: "b", Cost: 3, ReturnInterval: -1}}, 0},
		{"first match without interval", []*CostEntry{{Match: "b", Cost: 3}, {Match: "b1", Cost: 4, ReturnInterval: int64(90 * time.Second)}}, 5 * time.Minute},
		{"first regexp match", []*CostEntry{{Match: `^b[0-9]+$`, IsRegexp: true, Cost: 3, ReturnInterval: int64(90 * time.Second)}, {Match: "b", Cost: 4, ReturnInterval: int64(2 * time.Minute)}}, 90 * time.Second},
		{"no matching entry", []*CostEntry{{Match: "c", Cost: 3, ReturnInterval: int64(90 * time.Second)}}, 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := &SelectorConfig{
				Costs:         append([]*CostEntry{{Match: "a", Cost: 1}}, tc.entries...),
				StartOutbound: "b1", MinHoldTime: int64(time.Minute),
				ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(5 * time.Minute), SkipRandomPhase: true},
			}
			te := newTestEngine(t, nil, sc, "a1", "b1")
			got := te.sel.returnToCheaperAt
			if tc.want == 0 {
				if !got.IsZero() {
					t.Fatalf("disabled period scheduled return at %v", got)
				}
				return
			}
			if want := t0.Add(tc.want); !got.Equal(want) {
				t.Fatalf("return at %v, want %v", got, want)
			}
		})
	}
}

func TestCostReturnIntervalFollowsSelectedOutbound(t *testing.T) {
	sc := &SelectorConfig{
		Costs:         []*CostEntry{{Match: "a", Cost: 1}, {Match: "b", Cost: 3, ReturnInterval: int64(40 * time.Second)}, {Match: "c", Cost: 5}},
		StartOutbound: "c1", MinHoldTime: int64(time.Second),
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(5 * time.Minute), SkipRandomPhase: true},
	}
	te := newTestEngine(t, nil, sc, "a1", "b1", "c1")
	if want := t0.Add(5 * time.Minute); !te.sel.returnToCheaperAt.Equal(want) {
		t.Fatalf("initial return at %v, want %v", te.sel.returnToCheaperAt, want)
	}
	te.switchTo(te.sel, "b1", "test", at(10*time.Second))
	if want := at(50 * time.Second); !te.sel.returnToCheaperAt.Equal(want) {
		t.Fatalf("return from b1 at %v, want %v", te.sel.returnToCheaperAt, want)
	}
	te.switchTo(te.sel, "c1", "test", at(20*time.Second))
	if want := at(5*time.Minute + 20*time.Second); !te.sel.returnToCheaperAt.Equal(want) {
		t.Fatalf("return from c1 at %v, want %v", te.sel.returnToCheaperAt, want)
	}
}

func TestCostReturnIntervalControlsPenaltyDelay(t *testing.T) {
	sc := &SelectorConfig{
		Costs:         []*CostEntry{{Match: "a", Cost: 1}, {Match: "b", Cost: 3, ReturnInterval: int64(2 * time.Minute)}},
		StartOutbound: "b1", MinHoldTime: int64(time.Minute),
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(5 * time.Minute), SkipRandomPhase: true},
	}
	te := newTestEngine(t, nil, sc, "a1", "b1")
	te.sel.penalty["a1"] = 2
	now := at(2 * time.Minute)
	te.maybeReturnToCheaper(te.sel, now)
	if want := now.Add(2 * time.Minute); !te.sel.returnToCheaperAt.Equal(want) {
		t.Fatalf("penalized return at %v, want %v", te.sel.returnToCheaperAt, want)
	}
}

func TestDisabledCostReturnPeriodStillAllowsSoonReturn(t *testing.T) {
	sc := &SelectorConfig{
		Costs:         []*CostEntry{{Match: "a", Cost: 1}, {Match: "b", Cost: 3, ReturnInterval: -1}},
		StartOutbound: "b1", MinHoldTime: int64(time.Minute),
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(5 * time.Minute), SkipRandomPhase: true},
	}
	te := newTestEngine(t, nil, sc, "a1", "b1")
	if !te.sel.returnToCheaperAt.IsZero() {
		t.Fatalf("period scheduled return at %v", te.sel.returnToCheaperAt)
	}
	te.scheduleReturnToCheaper(te.sel, at(10*time.Second), true)
	if want := at(time.Minute); !te.sel.returnToCheaperAt.Equal(want) {
		t.Fatalf("soon return at %v, want %v", te.sel.returnToCheaperAt, want)
	}
}

func TestCostReturnIntervalAppliesToDynamicOutbound(t *testing.T) {
	sc := &SelectorConfig{
		Costs:           []*CostEntry{{Match: "a", Cost: 1}, {Match: "p-", Cost: 3, ReturnInterval: int64(30 * time.Second)}},
		MinHoldTime:     int64(time.Second),
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(5 * time.Minute), SkipRandomPhase: true},
	}
	te := newTestEngine(t, nil, sc, "a1")
	te.costs["auto"] = map[string]float64{"p-x": 3}
	te.updateCandidates(te.sel, []string{"a1", "p-x"}, at(10*time.Second))
	te.switchTo(te.sel, "p-x", "test", at(10*time.Second))
	if want := at(40 * time.Second); !te.sel.returnToCheaperAt.Equal(want) {
		t.Fatalf("dynamic outbound return at %v, want %v", te.sel.returnToCheaperAt, want)
	}
}

func TestCostReturnIntervalBoundsRandomPhaseAndRescueSpread(t *testing.T) {
	sc := &SelectorConfig{
		Costs:         []*CostEntry{{Match: "a", Cost: 1}, {Match: "b", Cost: 3, ReturnInterval: int64(30 * time.Second)}},
		StartOutbound: "b1", MinHoldTime: int64(time.Second),
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(5 * time.Minute)},
	}
	for seed := int64(1); seed <= 5; seed++ {
		te := newSeededTestEngine(t, seed, nil, sc, "a1", "b1")
		if got := te.sel.returnToCheaperAt; got.Before(at(time.Second)) || !got.Before(at(30*time.Second)) {
			t.Fatalf("seed %d: random return at %v, want within first 30s", seed, got)
		}
		te.scheduleReturn(te.sel, at(10*time.Second))
		if got := te.sel.returnToCheaperAt; got.Before(at(10*time.Second)) || !got.Before(at(40*time.Second)) {
			t.Fatalf("seed %d: rescued return at %v, want within 30s", seed, got)
		}
	}
}

func TestCostReturnIntervalCapsPenaltyAfterSelectingOutbound(t *testing.T) {
	for _, tc := range []struct {
		name      string
		global    time.Duration
		own       time.Duration
		limit     time.Duration
		wantExtra time.Duration
	}{
		{"own interval exceeds limit", time.Minute, 10 * time.Minute, 5 * time.Minute, 0},
		{"own interval below global", 8760 * time.Hour, 10 * time.Second, 2 * time.Hour, 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := &SelectorConfig{
				Costs:         []*CostEntry{{Match: "a", Cost: 1}, {Match: "b", Cost: 3, ReturnInterval: int64(tc.own)}},
				StartOutbound: "b1", MinHoldTime: int64(time.Second), SkipProbeBeforeSwitch: true,
				ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(tc.global), SkipRandomPhase: true,
					MaxPenaltyDelay: int64(tc.limit)},
			}
			te := newTestEngine(t, &Config{Schedule: quarantine(time.Second)}, sc, "a1", "b1")
			te.switchTo(te.sel, "a1", "return", t0)
			te.markDown(te.path("a1"), "traffic", true, at(time.Second))
			if got := te.sel.penaltyFor("a1"); got != 2 {
				t.Fatalf("penalty multiplier = %v, want 2", got)
			}
			if got := te.sel.current(); got != "b1" {
				t.Fatalf("fallback selected %q, want b1", got)
			}
			now := te.sel.returnToCheaperAt
			te.maybeReturnToCheaper(te.sel, now)
			if tc.wantExtra == 0 {
				if got := te.sel.returnToCheaperTarget; got != "a1" {
					t.Fatalf("return target after zero extra delay = %q, want a1", got)
				}
			} else if got := te.sel.returnToCheaperAt.Sub(now); got != tc.wantExtra {
				t.Fatalf("penalty extra delay = %v, want %v", got, tc.wantExtra)
			}
		})
	}
}

func TestZeroCostReturnIntervalAndHoldKeepPenaltyFinite(t *testing.T) {
	sc := &SelectorConfig{
		Costs:         []*CostEntry{{Match: "a", Cost: 1}, {Match: "b", Cost: 3, ReturnInterval: -1}},
		StartOutbound: "b1", MinHoldTime: -1, SkipProbeBeforeSwitch: true,
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(time.Minute), SkipRandomPhase: true,
			MaxPenaltyDelay: int64(time.Hour)},
	}
	te := newTestEngine(t, &Config{Schedule: quarantine(time.Second)}, sc, "a1", "b1")
	te.switchTo(te.sel, "a1", "return", t0)
	te.markDown(te.path("a1"), "traffic", true, at(time.Second))
	for i := 2; i < 65; i++ {
		now := at(time.Duration(i) * time.Second)
		te.sel.returnedTo, te.sel.returnedAt = "a1", now
		te.path("a1").status = statusUp
		te.markDown(te.path("a1"), "traffic", true, now)
	}
	penalty := te.sel.penaltyFor("a1")
	if math.IsInf(penalty, 0) || math.IsNaN(penalty) || penalty <= 1 {
		t.Fatalf("invalid penalty multiplier %v", penalty)
	}
	raw, _ := te.events.drain()
	if !json.Valid(raw) {
		t.Fatalf("invalid event JSON: %s", raw)
	}
	var events []Event
	if err := json.Unmarshal(raw, &events); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(events, func(e Event) bool { return e.Type == "return_penalty" }) {
		t.Fatal("penalty event was not serialized")
	}
	te.path("a1").status = statusUp
	te.path("a1").quarantineUntil = time.Time{}
	te.scheduleReturnToCheaper(te.sel, at(70*time.Second), true)
	if got := te.sel.returnToCheaperAt; !got.Equal(at(70 * time.Second)) {
		t.Fatalf("soon return at %v, want 70s", got)
	}
	te.maybeReturnToCheaper(te.sel, at(70*time.Second))
	if got := te.sel.returnToCheaperTarget; got != "a1" {
		t.Fatalf("zero-base return target = %q, want a1", got)
	}
}

func TestCostReturnPenaltyRecoversAfterDelayAndHold(t *testing.T) {
	sc := &SelectorConfig{
		Costs:         []*CostEntry{{Match: "a", Cost: 1}, {Match: "b", Cost: 3, ReturnInterval: int64(10 * time.Second)}},
		StartOutbound: "b1", MinHoldTime: int64(time.Second), SkipProbeBeforeSwitch: true,
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(time.Minute), SkipRandomPhase: true,
			PenaltyWindow: int64(time.Minute), MaxPenaltyDelay: int64(time.Hour)},
	}
	te := newTestEngine(t, &Config{Schedule: quarantine(time.Second)}, sc, "a1", "b1")
	te.switchTo(te.sel, "a1", "return", t0)
	te.markDown(te.path("a1"), "traffic", true, at(time.Second))
	now := te.sel.returnToCheaperAt
	te.maybeReturnToCheaper(te.sel, now)
	if got := te.sel.returnToCheaperAt.Sub(now); got != 10*time.Second {
		t.Fatalf("penalty extra delay = %v, want 10s", got)
	}
	back := te.sel.returnToCheaperAt
	te.maybeReturnToCheaper(te.sel, back)
	if got := te.sel.returnToCheaperTarget; got != "a1" {
		t.Fatalf("return after penalty delay targets %q, want a1", got)
	}
	te.switchTo(te.sel, "a1", "return", back)
	te.tick(back.Add(time.Minute))
	if got := te.sel.penaltyFor("a1"); got != 1 {
		t.Fatalf("penalty after successful hold = %v, want 1", got)
	}
}

func TestGlobalReturnPenaltyKeepsExistingLimit(t *testing.T) {
	sc := &SelectorConfig{
		Costs:         []*CostEntry{{Match: "a", Cost: 1}, {Match: "b", Cost: 3}},
		StartOutbound: "b1", MinHoldTime: int64(time.Second), SkipProbeBeforeSwitch: true,
		ReturnToCheaper: &ReturnToCheaperConfig{Interval: int64(time.Minute), SkipRandomPhase: true,
			MaxPenaltyDelay: int64(3 * time.Minute)},
	}
	te := newTestEngine(t, &Config{Schedule: quarantine(time.Second)}, sc, "a1", "b1")
	te.sel.penalty["a1"] = 2
	te.switchTo(te.sel, "a1", "return", t0)
	te.markDown(te.path("a1"), "traffic", true, at(time.Second))
	if got := te.sel.penaltyFor("a1"); got != 3 {
		t.Fatalf("global penalty multiplier = %v, want capped 3", got)
	}
	now := te.sel.returnToCheaperAt
	te.maybeReturnToCheaper(te.sel, now)
	if got := te.sel.returnToCheaperAt.Sub(now); got != 2*time.Minute {
		t.Fatalf("global penalty extra delay = %v, want 2m", got)
	}
}
