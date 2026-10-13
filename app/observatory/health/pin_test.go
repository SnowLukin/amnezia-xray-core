package health

import (
	"slices"
	"testing"
	"time"
)

// A network change under a pin is rechecked once the pin is released, by
// the host or by its timer, as it would have been without the pin.
func TestNetworkChangeUnderPinIsRecheckedOnRelease(t *testing.T) {
	for _, by := range []string{"host", "timer"} {
		t.Run(by, func(t *testing.T) {
			sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), StartPinnedOutbound: "a1", StartPinTimeout: int64(30 * time.Second)}
			te := newTestEngine(t, nil, sc, "a1", "b1")
			te.onNetworkChange("cafe", at(10*time.Second))
			te.noteDemand(at(10 * time.Second))
			if jobs, _ := te.tick(at(11 * time.Second)); len(jobs) != 0 {
				t.Fatalf("probes %v under the pin", reasons(jobs))
			}
			releaseAt := at(31 * time.Second)
			if by == "host" {
				releaseAt = at(20 * time.Second)
				te.release("auto", releaseAt)
			}
			jobs, _ := te.tick(releaseAt)
			if !slices.Equal(reasons(jobs), []string{"network_change:a1"}) {
				t.Fatalf("probes %v after the release, want a1 rechecked", reasons(jobs))
			}
			te.onProbeResult(jobs[0], probeResult{ok: false}, releaseAt.Add(time.Second))
			te.run(t, releaseAt.Add(5*time.Second), nil)
			if te.sel.current() != "b1" {
				t.Fatalf("a1 failed its recheck, the balancer is on %q", te.sel.current())
			}
		})
	}
}

// A release probes nothing when traffic or a probe has already shown that the
// pinned outbound works in the new network.
func TestReleaseSkipsTheRecheckOfAnOutboundSeenWorking(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), StartPinnedOutbound: "a1", StartPinTimeout: int64(30 * time.Second)}
	for _, how := range []string{"traffic", "host probe"} {
		t.Run(how, func(t *testing.T) {
			te := newTestEngine(t, nil, sc, "a1", "b1")
			te.onNetworkChange("cafe", at(10*time.Second))
			te.noteDemand(at(10 * time.Second))
			if how == "traffic" {
				te.onSuccess("a1", at(11*time.Second))
			} else {
				te.ask(t, at(11*time.Second), "a1")
				if jobs := te.run(t, at(11*time.Second), nil); !slices.Equal(reasons(jobs), []string{"host:a1"}) {
					t.Fatalf("setup: probes %v for the host", reasons(jobs))
				}
			}
			if jobs := te.run(t, at(31*time.Second), nil); len(jobs) != 0 || te.sel.pinned != "" {
				t.Fatalf("probes %v after the timer released the pin (pinned %q)", reasons(jobs), te.sel.pinned)
			}
		})
	}
}

// A release probes nothing when the network did not change under the pin:
// after a change before the pin, and after a recheck an earlier release
// already made.
func TestReleaseWithoutNetworkChangeUnderPinProbesNothing(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), StartPinnedOutbound: "a1", StartPinTimeout: int64(30 * time.Second)}
	te := newTestEngine(t, nil, sc, "a1", "b1")
	te.noteDemand(at(10 * time.Second))
	if jobs := te.run(t, at(31*time.Second), nil); len(jobs) != 0 || te.sel.pinned != "" {
		t.Fatalf("probes %v when the start pin ran out (pinned %q)", reasons(jobs), te.sel.pinned)
	}
	te.onNetworkChange("cafe", at(time.Minute))
	te.noteDemand(at(time.Minute))
	if jobs := te.run(t, at(time.Minute), nil); !slices.Equal(reasons(jobs), []string{"network_change:a1"}) {
		t.Fatalf("setup: probes %v after the change", reasons(jobs))
	}
	te.pin("auto", "a1", at(2*time.Minute))
	te.release("auto", at(3*time.Minute))
	te.noteDemand(at(3 * time.Minute))
	if jobs := te.run(t, at(3*time.Minute), nil); len(jobs) != 0 {
		t.Fatalf("probes %v after a release with the network changed before the pin", reasons(jobs))
	}
	te.pin("auto", "a1", at(4*time.Minute))
	te.onNetworkChange("home", at(5*time.Minute))
	te.release("auto", at(6*time.Minute))
	te.noteDemand(at(6 * time.Minute))
	if jobs := te.run(t, at(6*time.Minute), nil); !slices.Equal(reasons(jobs), []string{"network_change:a1"}) {
		t.Fatalf("probes %v after a release with the network changed under the pin", reasons(jobs))
	}
	te.pin("auto", "a1", at(7*time.Minute))
	te.release("auto", at(8*time.Minute))
	te.noteDemand(at(8 * time.Minute))
	if jobs := te.run(t, at(8*time.Minute), nil); len(jobs) != 0 {
		t.Fatalf("probes %v after a second release in the same network", reasons(jobs))
	}
}

// A pin holds back the recheck of its own balancer only, and its release
// rechecks nothing else.
func TestPinHoldsTheRecheckOfItsBalancerOnly(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	other := te.addSelector("other", legacyScenarioSelector(&SelectorConfig{Paths: pathSpecs("c1", 1, "d1", 3)}, nil))
	te.updateCandidates(other, []string{"c1", "d1"}, t0)
	te.pin("other", "c1", at(time.Second))
	te.onNetworkChange("cafe", at(10*time.Second))
	te.noteDemand(at(10 * time.Second))
	if jobs := te.run(t, at(10*time.Second), nil); !slices.Equal(reasons(jobs), []string{"network_change:a1"}) {
		t.Fatalf("probes %v after the change, want a1 of the balancer without a pin", reasons(jobs))
	}
	if te.release("auto", at(20*time.Second)) {
		t.Fatal("released a balancer that was not pinned")
	}
	if jobs := te.run(t, at(20*time.Second), nil); len(jobs) != 0 {
		t.Fatalf("probes %v after a release of a balancer without a pin", reasons(jobs))
	}
	te.release("other", at(30*time.Second))
	if jobs := te.run(t, at(30*time.Second), nil); !slices.Equal(reasons(jobs), []string{"network_change:c1"}) {
		t.Fatalf("probes %v after the release, want c1 of the released balancer only", reasons(jobs))
	}
}

// The recheck after a release follows the rules of a network change: the
// outbound remembered for the network goes first, and it waits while memory
// is low.
func TestRecheckOnReleaseRemembersNetworksAndWaitsForMemory(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onNetworkChange("home", at(0))
	te.run(t, at(time.Second), nil)
	te.switchTo(te.sel, "b1", "pin", at(2*time.Second))
	te.onSuccess("b1", at(3*time.Second))
	te.onNetworkChange("lte", at(time.Minute))
	te.run(t, at(time.Minute), nil)
	te.pin("auto", "a1", at(2*time.Minute))
	te.onNetworkChange("home", at(3*time.Minute))
	te.setMemoryLow(true, at(3*time.Minute))
	te.release("auto", at(4*time.Minute))
	te.noteDemand(at(4 * time.Minute))
	if jobs, _ := te.tick(at(4 * time.Minute)); len(jobs) != 0 {
		t.Fatalf("probes %v after the release while memory is low", reasons(jobs))
	}
	te.setMemoryLow(false, at(5*time.Minute))
	te.noteDemand(at(5 * time.Minute))
	if jobs := te.run(t, at(5*time.Minute), nil); !slices.Equal(reasons(jobs), []string{"remembered:b1"}) {
		t.Fatalf("probes %v after memory came back, want the remembered b1", reasons(jobs))
	}
	if te.sel.current() != "b1" {
		t.Fatalf("the balancer is on %q, want b1 remembered for home", te.sel.current())
	}
}

// The host chose the pinned outbound, so traffic on it does not replace the
// outbound remembered for the network, and the release goes back to that one.
func TestPinnedOutboundIsNotRememberedForTheNetwork(t *testing.T) {
	te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	te.onNetworkChange("home", at(0))
	te.run(t, at(time.Second), nil)
	te.pin("auto", "b1", at(time.Minute))
	te.onNetworkChange("home", at(2*time.Minute))
	te.onSuccess("b1", at(2*time.Minute+time.Second))
	te.release("auto", at(3*time.Minute))
	te.noteDemand(at(3 * time.Minute))
	if jobs := te.run(t, at(3*time.Minute), nil); !slices.Equal(reasons(jobs), []string{"remembered:a1"}) {
		t.Fatalf("probes %v after the release, want a1 remembered for home", reasons(jobs))
	}
	if te.sel.current() != "a1" {
		t.Fatalf("the balancer is on %q after the release, want a1", te.sel.current())
	}
}

// A release after a change to a network the core does not know starts a
// return to a cheaper outbound as soon as the change would have.
func TestReleaseInUnknownNetworkReturnsSoon(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), StartPinnedOutbound: "b1", MinHoldTime: v2Int64(int64(time.Second)),
		ReturnToCheaper: &ReturnToCheaperConfig{ProbesToReturn: 1, SkipRandomPhase: true}}
	for _, tc := range []struct {
		name     string
		networks []string
		soon     bool
	}{
		{"no change", nil, false},
		{"unknown network", []string{"office"}, true},
		{"known network", []string{"office", "cafe"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := newTestEngine(t, nil, sc, "a1", "b1")
			// cafe learns b1 between the start pin and a pin of the host.
			te.onNetworkChange("cafe", at(time.Second))
			te.release("auto", at(time.Second))
			te.onSuccess("b1", at(2*time.Second))
			te.pin("auto", "b1", at(3*time.Second))
			for i, network := range tc.networks {
				te.onNetworkChange(network, at(time.Duration(i+4)*time.Second))
				te.onSuccess("b1", at(time.Duration(i+4)*time.Second+time.Millisecond))
			}
			te.release("auto", at(20*time.Second))
			if soon := !te.sel.returnToCheaperAt.After(at(21 * time.Second)); soon != tc.soon {
				t.Fatalf("return at %v after the release, want soon %v", te.sel.returnToCheaperAt.Sub(at(20*time.Second)), tc.soon)
			}
		})
	}
}
