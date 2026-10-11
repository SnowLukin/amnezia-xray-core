package health

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestProbeSurfacesShowCurrentNetworkResultAndSafeStepFields(t *testing.T) {
	te := setEngine(t)
	p := te.path("p-x2")
	p.lastProbeEnd = at(3 * time.Second)
	p.lastProbeResult = probeResult{
		ok: true, profile: "full", reason: "confirm", state: statusDegraded,
		steps: []probeStepResult{
			{name: "transport", ok: true, connect: 820 * time.Millisecond, elapsed: 1400 * time.Millisecond, bytes: 32768, destination: 0},
			{name: "egress", connect: 790 * time.Millisecond, elapsed: 310 * time.Millisecond, destination: -1, class: "closed"},
		},
	}
	p.lastRTT = 1400 * time.Millisecond
	p.setStatus(statusDegraded, "egress", at(3*time.Second))

	statusRaw, err := json.Marshal(te.status(at(7 * time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	if err := json.Unmarshal(statusRaw, &status); err != nil {
		t.Fatal(err)
	}
	paths := status["paths"].([]any)
	var statusPath map[string]any
	for _, path := range paths {
		v := path.(map[string]any)
		if v["outbound"] == "p-x2" {
			statusPath = v
		}
	}
	probe := statusPath["probe"].(map[string]any)
	steps := probe["steps"].([]any)
	if statusPath["state"] != "degraded" || probe["state"] != "degraded" || probe["profile"] != "full" || probe["reason"] != "confirm" || probe["ago_ms"] != 4000.0 ||
		steps[0].(map[string]any)["destination"] != 0.0 || steps[0].(map[string]any)["connect_ms"] != 820.0 ||
		steps[1].(map[string]any)["destination"] != nil || steps[1].(map[string]any)["error"] != "closed" {
		t.Fatalf("status probe: %s", statusRaw)
	}
	if strings.Contains(string(statusRaw), "isPassed") || strings.Contains(string(statusRaw), "connectMs") {
		t.Fatalf("Status keys are not snake_case: %s", statusRaw)
	}

	body, err := json.Marshal(describeOutbound(p, "x2", 0, at(7*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	var outbound map[string]any
	if err := json.Unmarshal(body, &outbound); err != nil {
		t.Fatal(err)
	}
	providerProbe := outbound["probe"].(map[string]any)
	providerSteps := providerProbe["steps"].([]any)
	if providerProbe["isPassed"] != true || providerProbe["state"] != "degraded" || providerProbe["reason"] != "confirm" || providerProbe["rttMs"] != 1400.0 ||
		providerProbe["error"] != nil || providerSteps[0].(map[string]any)["destination"] != 0.0 ||
		providerSteps[1].(map[string]any)["destination"] != nil || providerSteps[1].(map[string]any)["error"] != "closed" {
		t.Fatalf("provider probe: %s", body)
	}
	if strings.Contains(string(body), "is_passed") || strings.Contains(string(body), "connect_ms") {
		t.Fatalf("provider keys are not camelCase: %s", body)
	}

	te.onNetworkChange("next", at(8*time.Second))
	for _, v := range te.status(at(9 * time.Second)).Paths {
		if v.Outbound == "p-x2" && v.Probe != nil {
			t.Fatalf("old network probe survives in Status: %+v", v.Probe)
		}
	}
	if got := describeOutbound(p, "x2", 0, at(9*time.Second)).Probe; got != nil {
		t.Fatalf("old network probe survives in provider: %+v", got)
	}
}

func TestProbeEventStepsUseSnakeCaseAndAttemptedResults(t *testing.T) {
	r := probeResult{steps: []probeStepResult{
		{name: "transport", ok: true, connect: 20 * time.Millisecond, elapsed: 35 * time.Millisecond, destination: 0},
		{name: "egress", elapsed: 50 * time.Millisecond, destination: -1, class: "status_503"},
	}}
	raw, err := json.Marshal(r.eventSteps())
	if err != nil {
		t.Fatal(err)
	}
	var steps []map[string]any
	if err := json.Unmarshal(raw, &steps); err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 || steps[0]["is_passed"] != true || steps[0]["connect_ms"] != 20.0 || steps[0]["destination"] != 0.0 ||
		steps[1]["is_passed"] != false || steps[1]["error"] != "status_503" || steps[1]["destination"] != nil ||
		strings.Contains(string(raw), "isPassed") {
		t.Fatalf("event steps: %s", raw)
	}
}

func TestReportCountsAttemptedStepsAndDegradedWindowTime(t *testing.T) {
	te := newReportEngine(t, reportEvery(time.Second), nil, "a1", "b1")
	p := te.path("a1")
	p.setStatus(statusDegraded, "egress", at(time.Second))
	te.emit(at(time.Second), "path_state", map[string]any{"outbound": "a1", "state": "degraded", "reason": "egress"})
	probeEvent := func(atTime time.Time, stale bool, steps []map[string]any) {
		te.emit(atTime, "probe_result", map[string]any{
			"outbound": "a1", "reason": "confirm", "is_ok": true, "is_stale": stale,
			"bytes": int64(10), "steps": steps,
		})
	}
	probeEvent(at(2*time.Second), false, []map[string]any{{"name": "transport", "is_passed": true}, {"name": "egress", "is_passed": false}})
	probeEvent(at(3*time.Second), true, []map[string]any{{"name": "transport", "is_passed": false}})
	te.emit(at(3*time.Second), "switch", map[string]any{"from": "a1", "to": "b1", "reason": "failover"})
	first := te.snapshotReport(at(4 * time.Second))
	if first == nil || first.PathDegraded != 1 || first.Paths["a1"].DegradedMs != 3000 ||
		!reflect.DeepEqual(first.Paths["a1"].ProbeSteps, []reportProbeStep{{Name: "transport", Passed: 1, Failed: 1}, {Name: "egress", Failed: 1}}) {
		t.Fatalf("first snapshot: %+v", first)
	}
	first = te.takeReport(false, at(5*time.Second))
	if first == nil || first.Paths["a1"].DegradedMs != 4000 {
		t.Fatalf("first report: %+v", first)
	}
	probeEvent(at(6*time.Second), false, []map[string]any{{"name": "transport", "is_passed": true}, {"name": "egress", "is_passed": false}})
	te.s.probes = append(te.s.probes, probeSettings{name: "new", steps: []probeStepSettings{{name: "new_egress"}, {name: "transport"}}})
	te.s.paths.Store("a1", pathSettings{probe: "new"})
	probeEvent(at(6500*time.Millisecond), false, []map[string]any{{"name": "transport", "is_passed": true}, {"name": "new_egress", "is_passed": true}})
	p.setStatus(statusUp, "probe", at(7*time.Second))
	te.emit(at(7*time.Second), "switch", map[string]any{"from": "b1", "to": "a1", "reason": "return"})
	second := te.takeReport(false, at(8*time.Second))
	if second == nil || second.PathDegraded != 0 || second.Paths["a1"].DegradedMs != 2000 ||
		!reflect.DeepEqual(second.Paths["a1"].ProbeSteps, []reportProbeStep{{Name: "new_egress", Passed: 1}, {Name: "transport", Passed: 2}, {Name: "egress", Failed: 1}}) {
		t.Fatalf("second report: %+v", second)
	}
}

func TestStaleProbeStepsCountWithoutReplacingCurrentNetworkSnapshot(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	j := te.enqueue("a1", reasonRecheck, "auto", t0)
	te.begin(j, at(time.Second))
	te.onNetworkChange("next", at(2*time.Second))
	te.onProbeResult(j, probeResult{
		ok: true, profile: "full", state: statusUp,
		steps: []probeStepResult{{name: "transport", ok: true, destination: 0}},
	}, at(3*time.Second))
	if probe := te.path("a1").statusProbe(at(4 * time.Second)); probe != nil {
		t.Fatalf("stale result replaced current-network snapshot: %+v", probe)
	}
	te.emit(at(4*time.Second), "switch", map[string]any{"from": "a1", "to": "b1", "reason": "failover"})
	v := te.takeReport(true, at(5*time.Second))
	if v == nil || !reflect.DeepEqual(v.Paths["a1"].ProbeSteps, []reportProbeStep{{Name: "transport", Passed: 1}}) {
		t.Fatalf("stale probe missing from report: %+v", v)
	}
}

func TestProbeSnapshotReasonComesFromCurrentJob(t *testing.T) {
	for _, reason := range []probeReason{reasonHost, reasonConfirm, reasonRecheck, reasonNetwork} {
		t.Run(reason.String(), func(t *testing.T) {
			te := newReportEngine(t, nil, nil, "a1", "b1")
			j := te.enqueue("a1", reason, "auto", t0)
			te.begin(j, t0)
			te.onProbeResult(j, probeResult{ok: true, profile: "full", reason: "untrusted", state: statusUp}, at(time.Second))
			p := te.path("a1")
			status := p.statusProbe(at(2 * time.Second))
			provider := describeOutbound(p, "a1", 0, at(2*time.Second)).Probe
			if status == nil || status.Reason != reason.String() || provider == nil || provider.Reason != reason.String() {
				t.Fatalf("job reason %q lost: status=%+v provider=%+v", reason, status, provider)
			}
		})
	}
}

func TestStaleAndCanceledProbesKeepCurrentSnapshotReason(t *testing.T) {
	for _, scenario := range []string{"invalidated", "old_generation", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			te := newReportEngine(t, nil, nil, "a1", "b1")
			first := te.enqueue("a1", reasonRecheck, "auto", t0)
			te.begin(first, t0)
			te.onProbeResult(first, probeResult{ok: true, profile: "full", state: statusUp}, at(time.Second))
			p := te.path("a1")
			beforeStatus := p.statusProbe(at(4 * time.Second))
			beforeProvider := describeOutbound(p, "a1", 0, at(4*time.Second)).Probe
			late := te.enqueue("a1", reasonHost, "auto", at(2*time.Second))
			te.begin(late, at(2*time.Second))
			result := probeResult{profile: "full", reason: "untrusted", state: statusDown, class: "timeout"}
			switch scenario {
			case "invalidated":
				late.invalidated = true
			case "old_generation":
				late.generation--
			case "canceled":
				result.canceled = true
			}
			te.onProbeResult(late, result, at(3*time.Second))
			afterStatus := p.statusProbe(at(4 * time.Second))
			afterProvider := describeOutbound(p, "a1", 0, at(4*time.Second)).Probe
			if beforeStatus == nil || beforeStatus.Reason != "recheck" || beforeProvider == nil || beforeProvider.Reason != "recheck" ||
				!reflect.DeepEqual(beforeStatus, afterStatus) || !reflect.DeepEqual(beforeProvider, afterProvider) {
				t.Fatalf("%s replaced snapshot: status=%+v -> %+v provider=%+v -> %+v", scenario, beforeStatus, afterStatus, beforeProvider, afterProvider)
			}
			events := te.eventsOf(t, "probe_result")
			if scenario == "canceled" {
				if len(events) != 1 {
					t.Fatalf("canceled probe produced a result event: %+v", events)
				}
			} else if len(events) != 2 || events[1].Fields["reason"] != "host" || events[1].Fields["is_stale"] != true {
				t.Fatalf("stale event lost its own job reason: %+v", events)
			}
		})
	}
}

func TestReportProbeStepAccountingKeepsOnlyFirst64Tags(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	for i := 64; i >= 0; i-- {
		tag := fmt.Sprintf("path-%03d", i)
		te.path(tag)
		te.emit(at(time.Second), "probe_result", map[string]any{
			"outbound": tag, "is_ok": true,
			"steps": []map[string]any{{"name": "transport", "is_passed": true}},
		})
	}
	if got := len(te.report.stepCounts); got != reportPaths || te.report.stepCounts["path-000"] == nil || te.report.stepCounts["path-064"] != nil {
		t.Fatalf("tracked %d tags with wrong boundaries, want first %d", got, reportPaths)
	}
}

func TestForgottenPathStartsProbeStepCountsFromZero(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	te.path("b1")
	te.emit(at(time.Second), "probe_result", map[string]any{
		"outbound": "b1", "is_ok": true,
		"steps": []map[string]any{{"name": "transport", "is_passed": true}},
	})
	te.forget("b1")
	te.path("b1")
	te.emit(at(2*time.Second), "probe_result", map[string]any{
		"outbound": "b1", "is_ok": false,
		"steps": []map[string]any{{"name": "transport", "is_passed": false}},
	})
	te.emit(at(3*time.Second), "switch", map[string]any{"from": "a1", "to": "b1", "reason": "failover"})
	v := te.takeReport(true, at(4*time.Second))
	if v == nil || !reflect.DeepEqual(v.Paths["b1"].ProbeSteps, []reportProbeStep{{Name: "transport", Failed: 1}}) {
		t.Fatalf("recreated path retained old probe counts: %+v", v)
	}
}
