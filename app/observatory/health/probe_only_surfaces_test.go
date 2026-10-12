package health

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestProbeOnlyReportCountersRespectWindows(t *testing.T) {
	te := newReportEngine(t, &Config{Report: &ReportConfig{Interval: int64(8 * time.Second)}}, nil, "a1", "b1")
	te.emit(at(time.Second), "probe_start", map[string]any{"outbound": "a1", "reason": "candidate"})
	te.emit(at(2*time.Second), "probe_result", map[string]any{"outbound": "a1", "reason": "candidate", "is_ok": false, "is_stale": true})
	te.emit(at(3*time.Second), "probe_start", map[string]any{"outbound": "b1", "reason": "candidate"})
	te.emit(at(4*time.Second), "probe_result", map[string]any{"outbound": "b1", "reason": "candidate", "is_ok": true})
	te.emit(at(5*time.Second), "probe_only_promoted", map[string]any{"outbound": "b1"})
	te.emit(at(6*time.Second), "fresh_path_down", map[string]any{"outbound": "b1"})
	te.emit(at(7*time.Second), "switch", map[string]any{"from": "a1", "to": "b1", "reason": "failover"})
	first := te.takeReport(false, at(8*time.Second))
	if first == nil || first.CandidateProbes != 2 || first.CandidateProbesFailed != 1 || first.ProbeOnlyPromoted != 1 || first.FreshPathDown != 1 {
		t.Fatalf("candidate report: %+v", first)
	}
	raw, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]float64{"candidate_probes": 2, "candidate_probes_failed": 1, "probe_only_promoted_count": 1, "fresh_path_down_count": 1} {
		if report[key] != want {
			t.Fatalf("%s=%v, want %v", key, report[key], want)
		}
	}
	second := te.takeReport(true, at(9*time.Second))
	if second == nil || second.CandidateProbes != 0 || second.CandidateProbesFailed != 0 || second.ProbeOnlyPromoted != 0 || second.FreshPathDown != 0 {
		t.Fatalf("next window: %+v", second)
	}
}

func TestProbeOnlyStatusIncludesBothModes(t *testing.T) {
	te := setEngine(t)
	te.s.paths.Store("p-x1", pathSettings{isProbeOnly: true})
	te.path("p-x1")
	te.path("p-x2")
	raw, err := json.Marshal(te.status(at(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatal(err)
	}
	modes := map[string]bool{}
	for _, value := range status["paths"].([]any) {
		path := value.(map[string]any)
		flag, present := path["is_probe_only"].(bool)
		if !present {
			t.Fatalf("missing boolean mode: %s", raw)
		}
		modes[path["outbound"].(string)] = flag
	}
	if !modes["p-x1"] || modes["p-x2"] {
		t.Fatalf("modes: %v", modes)
	}
	for _, value := range status["providers"].([]any) {
		if _, present := value.(map[string]any)["probe_only_verified"]; !present {
			t.Fatalf("missing provider verified: %s", raw)
		}
	}
}

func TestProbeOnlyDispatchPolicyPreservesHandlerMode(t *testing.T) {
	o := newObserver(context.Background(), &Config{Paths: observed("same-tag")})
	user := context.Background()
	probe := context.WithValue(user, probeContextKey{}, &probeState{})
	if o.AllowDispatch(user, "same-tag", true) {
		t.Fatal("old probe handler admitted user after promotion")
	}
	if o.AllowDispatch(probe, "same-tag", true) {
		t.Fatal("old probe handler admitted a generic probe after promotion")
	}
	if !o.AllowDispatch(user, "same-tag", false) {
		t.Fatal("real handler was rejected")
	}
	o.s.paths.Store("same-tag", pathSettings{isProbeOnly: true})
	if o.AllowDispatch(user, "same-tag", false) {
		t.Fatal("selected real handler admitted a new user after demotion")
	}
	if o.AllowDispatch(probe, "same-tag", false) {
		t.Fatal("real handler admitted a generic probe after demotion")
	}
}

func TestProbeOnlyStatusVerifiedBelongsToProviderSet(t *testing.T) {
	te := setEngine(t)
	for _, tag := range []string{"p-x1", "other-candidate"} {
		te.s.paths.Store(tag, pathSettings{isProbeOnly: true})
		p := te.path(tag)
		p.candidateEpoch = te.candidateEpoch
		p.candidateVerifiedAt = at(time.Second)
		p.lastProbeResult = probeResult{ok: true, reason: reasonCandidate.String()}
	}
	te.providers = append(te.providers, &provider{cfg: &providerSettings{tag: "other"}, tags: []string{"other-candidate"}})
	status := te.status(at(2 * time.Second))
	if len(status.Providers) != 2 {
		t.Fatalf("providers: %+v", status.Providers)
	}
	for _, provider := range status.Providers {
		if provider.ProbeOnlyVerified != 1 {
			t.Fatalf("provider %s borrowed verification from another set: %+v", provider.Provider, provider)
		}
	}
}
