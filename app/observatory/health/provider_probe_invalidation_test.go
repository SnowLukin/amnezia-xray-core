package health

import (
	"slices"
	"testing"
	"time"
)

func providerProbeEngine(t *testing.T) *testEngine {
	t.Helper()
	return newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: []*PathDefinition{
		{Tag: "p-x", Balancer: "auto", Cost: 1, Probe: "light"},
		{Tag: "p-y", Balancer: "auto", Cost: 2, Probe: "light"},
	}}, "p-x", "p-y")
}

func providerPaths(profile string) []*PathDefinition {
	return []*PathDefinition{{Tag: "p-x", Balancer: "auto", Cost: 1, Probe: profile}, {Tag: "p-y", Balancer: "auto", Cost: 2, Probe: "light"}}
}

func TestReplacedHandlerInvalidatesOldProbe(t *testing.T) {
	for _, oldState := range []pathStatus{statusDown, statusDegraded} {
		t.Run(oldState.String(), func(t *testing.T) {
			te := providerProbeEngine(t)
			p := te.path("p-x")
			p.setStatus(statusUp, "probe", t0)
			p.lastSuccess = t0
			p.lastAnsweredStart = 91
			p.lastProbeEnd = t0
			p.lastProbeResult = probeResult{ok: true, state: statusUp, profile: "light", reason: "confirm"}
			old := te.enqueue("p-x", reasonHost, "auto", t0)
			te.running[old.id] = old
			p.probing = true
			queued := te.enqueue("p-x", reasonRecheck, "auto", t0)
			te.applyProviderSet(te.prov(), providerChange{
				tags: []string{"p-x", "p-y"}, paths: providerPaths("light"), reset: []string{"p-x"},
			}, at(time.Second))
			if !old.invalidated || slices.Contains(te.queue, queued) || p.status != statusUnknown ||
				!p.lastProbeEnd.IsZero() || p.lastProbeResult.profile != "" || p.lastProbeResult.reason != "" || !p.lastSuccess.IsZero() || p.lastAnsweredStart != 0 || p.probing {
				t.Fatalf("replacement kept old proof or job: state=%v result=%+v old=%+v", p.status, p.lastProbeResult, old)
			}
			r := probeResult{state: oldState, profile: "light", class: "timeout", err: "old handler"}
			if oldState == statusDegraded {
				r = degradedProbe()
				r.profile = "light"
			}
			te.onProbeResult(old, r, at(2*time.Second))
			if p.status != statusUnknown || p.quarantined(at(2*time.Second)) || p.lastProbeResult.profile != "" || p.lastProbeResult.reason != "" || !p.lastProbeEnd.IsZero() {
				t.Fatalf("old result judged new handler: state=%v result=%+v", p.status, p.lastProbeResult)
			}
			events := te.eventsOf(t, "probe_result")
			if len(events) != 1 || events[0].Fields["is_stale"] != true {
				t.Fatalf("old result was not recorded as stale: %+v", events)
			}
			fresh := &probeJob{id: old.id + 100, tag: "p-x", reason: reasonHost, generation: te.generation}
			te.onProbeResult(fresh, probeResult{ok: true, state: statusUp, profile: "light"}, at(3*time.Second))
			if p.status != statusUp || p.lastProbeResult.profile != "light" || p.lastProbeResult.reason != "host" || !p.lastProbeEnd.Equal(at(3*time.Second)) {
				t.Fatalf("new handler result was not accepted: state=%v result=%+v", p.status, p.lastProbeResult)
			}
		})
	}
}

func TestProfileChangeInvalidatesOldQualityWithoutReplacingHandler(t *testing.T) {
	te := providerProbeEngine(t)
	p := te.path("p-x")
	p.setStatus(statusUp, "probe", t0)
	p.lastSuccess = t0
	p.lastProbeEnd = t0
	p.lastProbeResult = probeResult{ok: true, state: statusUp, profile: "light", reason: "confirm"}
	p.uses = 7
	p.sleeper = sleeperYes
	old := te.enqueue("p-x", reasonHost, "auto", t0)
	te.running[old.id] = old
	p.probing = true
	te.applyProviderSet(te.prov(), providerChange{
		tags: []string{"p-x", "p-y"}, paths: providerPaths("full"),
	}, at(time.Second))
	if p.status != statusUnknown || p.reason != "probe_changed" || p.lastProbeResult.profile != "" || p.lastProbeResult.reason != "" ||
		!p.lastProbeEnd.IsZero() || !old.invalidated || p.probing || p.uses != 7 || p.sleeper != sleeperYes {
		t.Fatalf("profile change kept old quality or reset handler: state=%v result=%+v uses=%d sleeper=%v", p.status, p.lastProbeResult, p.uses, p.sleeper)
	}
	te.onProbeResult(old, degradedProbe(), at(2*time.Second))
	if p.status != statusUnknown || p.lastProbeResult.profile != "" || p.lastProbeResult.reason != "" {
		t.Fatalf("old light probe restored quality: state=%v result=%+v", p.status, p.lastProbeResult)
	}
	fresh := &probeJob{id: old.id + 100, tag: "p-x", reason: reasonHost, generation: te.generation}
	te.onProbeResult(fresh, degradedProbe(), at(3*time.Second))
	if p.status != statusDegraded || p.lastProbeResult.profile != "full" || p.lastProbeResult.reason != "host" || !p.lastProbeEnd.Equal(at(3*time.Second)) {
		t.Fatalf("new full probe was not applied: state=%v result=%+v", p.status, p.lastProbeResult)
	}
}

func TestProfileChangePreservesTrafficJudgment(t *testing.T) {
	for _, state := range []pathStatus{statusUp, statusDown} {
		t.Run(state.String(), func(t *testing.T) {
			te := providerProbeEngine(t)
			p := te.path("p-x")
			p.setStatus(state, "traffic", t0)
			p.lastProbeEnd = t0
			p.lastProbeResult = probeResult{profile: "light", state: statusUp, ok: true}
			if state == statusDown {
				p.quarantineUntil = at(time.Minute)
			} else {
				p.lastSuccess = at(time.Second)
			}
			te.applyProviderSet(te.prov(), providerChange{
				tags: []string{"p-x", "p-y"}, paths: providerPaths("full"),
			}, at(2*time.Second))
			if p.status != state || p.reason != "traffic" || p.lastProbeResult.profile != "" || !p.lastProbeEnd.IsZero() {
				t.Fatalf("traffic judgment lost: state=%v reason=%q result=%+v", p.status, p.reason, p.lastProbeResult)
			}
			if state == statusDown && !p.quarantined(at(2*time.Second)) {
				t.Fatal("traffic quarantine was cleared by profile change")
			}
		})
	}
}

func TestReplacementRestartsPendingHostProbe(t *testing.T) {
	te := providerProbeEngine(t)
	id, _, err := te.requestProbe("auto", []string{"p-x"}, t0)
	if err != nil {
		t.Fatal(err)
	}
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0].reason != reasonHost {
		t.Fatalf("host probe did not start: %+v", jobs)
	}
	old := jobs[0]
	te.applyProviderSet(te.prov(), providerChange{
		tags: []string{"p-x", "p-y"}, paths: providerPaths("light"), reset: []string{"p-x"},
	}, at(time.Second))
	if !old.invalidated || !te.waitedFor("p-x") {
		t.Fatal("replacement lost the pending host request")
	}
	te.onProbeResult(old, probeResult{state: statusDown, class: "timeout"}, at(2*time.Second))
	jobs, _ = te.tick(at(61 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonHost || jobs[0].request != id {
		t.Fatalf("fresh host probe was not queued: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp, profile: "light"}, at(61*time.Second))
	if te.waitedFor("p-x") {
		t.Fatal("fresh result did not close the host request")
	}
}

func TestForgottenOutboundRejectsOldRunningProbe(t *testing.T) {
	for _, state := range []pathStatus{statusDown, statusDegraded} {
		for _, readded := range []bool{false, true} {
			name := state.String()
			if readded {
				name += "_readded"
			}
			t.Run(name, func(t *testing.T) {
				te := providerProbeEngine(t)
				_, _, err := te.requestProbe("auto", []string{"p-x"}, t0)
				if err != nil {
					t.Fatal(err)
				}
				jobs, _ := te.tick(t0)
				if len(jobs) != 1 || jobs[0].reason != reasonHost {
					t.Fatalf("host probe did not start: %+v", jobs)
				}
				old := jobs[0]
				te.dropProviderOutbound("p-x", at(time.Second))
				if !old.invalidated || !old.forgotten {
					t.Fatal("running probe survived forget")
				}
				if readded {
					te.applyProviderSet(te.prov(), providerChange{
						tags: []string{"p-x", "p-y"}, paths: providerPaths("full"), added: []string{"p-x"},
					}, at(2*time.Second))
					te.updateCandidates(te.sel, []string{"p-x", "p-y"}, at(2*time.Second))
				}
				r := probeResult{state: statusDown, class: "timeout", profile: "light", steps: []probeStepResult{{name: "transport", class: "timeout", destination: -1}}}
				if state == statusDegraded {
					r = degradedProbe()
					r.profile = "light"
				}
				te.onProbeResult(old, r, at(3*time.Second))
				p := te.paths["p-x"]
				if readded {
					if p == nil || p.status != statusUnknown || p.lastProbeResult.profile != "" || !p.lastProbeEnd.IsZero() {
						t.Fatalf("old result judged new handler: %+v", p)
					}
				} else if p != nil {
					t.Fatalf("old result recreated forgotten path: %+v", p)
				}
				if te.report.stepCounts["p-x"] != nil {
					t.Fatal("old result recreated forgotten probe step counters")
				}
				if readded {
					te.serveRequests(at(3 * time.Minute))
				} else {
					te.serveRequests(at(4 * time.Second))
				}
				if te.waitedFor("p-x") {
					t.Fatal("host request waited forever for the forgotten probe")
				}
			})
		}
	}
}
