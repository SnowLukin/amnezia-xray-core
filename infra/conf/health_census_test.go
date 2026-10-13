package conf_test

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/infra/conf"
)

func TestHealthCensusPresenceAndDuration(t *testing.T) {
	cfg, err := buildHealth(t, `{}`)
	if err != nil || cfg.Census != nil {
		t.Fatalf("absent census: %v %v", cfg, err)
	}
	cfg, err = buildHealth(t, `{"telemetry":{"report":{},"census":{"sampleFraction":0.03}}}`)
	if err != nil || cfg.Census == nil || cfg.Census.SampleFraction != 0.03 || !cfg.Features.GetReport() || cfg.Census.StartAfter != 0 {
		t.Fatalf("present census: %v %v", cfg, err)
	}
	cfg, err = buildHealth(t, `{"telemetry":{"report":{},"census":{"sampleFraction":1,"startAfter":"1s","onDemand":["reserve","relay"]}}}`)
	if err != nil || cfg.Census.StartAfter != int64(time.Second) || len(cfg.Census.OnDemand) != 2 {
		t.Fatalf("live duration and names: %v %v", cfg, err)
	}
}

func TestHealthCensusRejects(t *testing.T) {
	for name, raw := range map[string]string{
		"missing report":    `{"telemetry":{"census":{"sampleFraction":1}}}`,
		"missing sample":    `{"telemetry":{"report":{},"census":{}}}`,
		"zero sample":       `{"telemetry":{"report":{},"census":{"sampleFraction":0}}}`,
		"negative sample":   `{"telemetry":{"report":{},"census":{"sampleFraction":-0.1}}}`,
		"large sample":      `{"telemetry":{"report":{},"census":{"sampleFraction":1.01}}}`,
		"null sample":       `{"telemetry":{"report":{},"census":{"sampleFraction":null}}}`,
		"null block":        `{"telemetry":{"report":{},"census":null}}`,
		"null names":        `{"telemetry":{"report":{},"census":{"sampleFraction":1,"onDemand":null}}}`,
		"zero duration":     `{"telemetry":{"report":{},"census":{"sampleFraction":1,"startAfter":"0s"}}}`,
		"negative duration": `{"telemetry":{"report":{},"census":{"sampleFraction":1,"startAfter":"-1s"}}}`,
		"unknown field":     `{"telemetry":{"report":{},"census":{"sampleFraction":1,"period":"1h"}}}`,
		"enable true":       `{"telemetry":{"report":{},"census":{"sampleFraction":1,"isEnabled":true}}}`,
		"enable false":      `{"telemetry":{"report":{},"census":{"sampleFraction":1,"isEnabled":false}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := buildHealth(t, raw); err == nil || !strings.Contains(err.Error(), "telemetry.census") {
				t.Fatalf("accepted invalid census or incorrect error path: %v", err)
			}
		})
	}
	for _, sample := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		h := conf.HealthObservatoryConfig{Telemetry: conf.HealthTelemetryConfig{Report: conf.HealthReportConfig{HealthFeatureConfig: conf.HealthFeatureConfig{IsEnabled: ptrCensus(true)}}, Census: &conf.HealthCensusConfig{SampleFraction: &sample}}}
		if _, err := h.Build(); err == nil || !strings.Contains(err.Error(), "sampleFraction") {
			t.Fatalf("nonfinite sample accepted: %v", err)
		}
	}
}

func ptrCensus[T any](value T) *T { return &value }

func TestHealthCensusOnDemandPathsAndGroups(t *testing.T) {
	for name, tc := range map[string]struct {
		onDemand    []string
		groupPolicy string
		tagPolicy   string
		wantError   string
	}{
		"known lazy tag":    {onDemand: []string{"b1"}, tagPolicy: "onDemand"},
		"unknown tag":       {onDemand: []string{"missing"}, wantError: "unknown path or group"},
		"ordinary tag":      {onDemand: []string{"b1"}, wantError: "probePolicy:onDemand"},
		"lazy group":        {onDemand: []string{"relay"}, groupPolicy: "onDemand"},
		"lazy group member": {onDemand: []string{"b1"}, groupPolicy: "onDemand"},
		"ordinary group":    {onDemand: []string{"relay"}, groupPolicy: "regular", wantError: "probePolicy:onDemand"},
		"empty name":        {onDemand: []string{""}, wantError: "unknown path or group"},
		"omitted names":     {},
	} {
		t.Run(name, func(t *testing.T) {
			var cfg map[string]any
			if err := json.Unmarshal([]byte(leastCostConfig(`,"healthObservatory":{"telemetry":{"report":{},"census":{"sampleFraction":1}}}`, `{}`)), &cfg); err != nil {
				t.Fatal(err)
			}
			if tc.onDemand != nil {
				cfg["healthObservatory"].(map[string]any)["telemetry"].(map[string]any)["census"].(map[string]any)["onDemand"] = tc.onDemand
			}
			selection := cfg["outbounds"].([]any)[1].(map[string]any)["selection"].(map[string]any)
			if tc.tagPolicy != "" {
				selection["probePolicy"] = tc.tagPolicy
			}
			if tc.groupPolicy != "" {
				delete(selection, "cost")
				delete(selection, "probe")
				selection["group"] = "relay"
				settings := cfg["routing"].(map[string]any)["balancers"].([]any)[0].(map[string]any)["strategy"].(map[string]any)["settings"].(map[string]any)
				settings["groups"] = []any{map[string]any{"name": "relay", "cost": 3, "probe": "test", "probePolicy": tc.groupPolicy}}
			}
			encoded, _ := json.Marshal(cfg)
			err := loadConfig(string(encoded))
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "telemetry.census.onDemand[0]") || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("invalid onDemand accepted or incorrect path: %v", err)
			}
		})
	}
}

func TestHealthCensusValidatesAllBalancersForGroupNames(t *testing.T) {
	for _, secondPolicy := range []string{"onDemand", "regular"} {
		raw := `{"outbounds":[{"tag":"a","protocol":"freedom","selection":{"balancer":"first","group":"first-relay"}},{"tag":"b","protocol":"freedom","selection":{"balancer":"second","group":"relay"}},{"tag":"block","protocol":"blackhole"}],"routing":{"balancers":[{"tag":"first","fallbackTag":"block","strategy":{"type":"leastCost","settings":{"groups":[{"name":"first-relay","cost":1,"probe":"test","probePolicy":"onDemand"}]}}},{"tag":"second","fallbackTag":"block","strategy":{"type":"leastCost","settings":{"groups":[{"name":"relay","cost":1,"probe":"test","probePolicy":"` + secondPolicy + `"}]}}}]},"healthObservatory":{"probes":{"profiles":[{"name":"test","steps":[{"name":"payload","destinations":["https://example.test/check"]}]}]},"telemetry":{"report":{},"census":{"sampleFraction":1,"onDemand":["relay"]}}}}`
		err := loadConfig(raw)
		if secondPolicy == "onDemand" && err != nil {
			t.Fatal(err)
		}
		if secondPolicy == "regular" && (err == nil || !strings.Contains(err.Error(), "probePolicy:onDemand")) {
			t.Fatalf("regular group in second selector accepted: %v", err)
		}
	}
}

func TestHealthCensusProbeOnlyWithoutAutonomousCandidates(t *testing.T) {
	raw := leastCostConfig(`,"healthObservatory":{"telemetry":{"report":{},"census":{"sampleFraction":1}}}`, `{}`)
	raw = strings.Replace(raw, `"cost":3,"probe":"test"`, `"cost":3,"probe":"test","isProbeOnly":true`, 1)
	if err := loadConfig(raw); err != nil {
		t.Fatal(err)
	}
	// Removing both consumers still refuses the probe-only path.
	var withoutCensus map[string]any
	if err := json.Unmarshal([]byte(raw), &withoutCensus); err != nil {
		t.Fatal(err)
	}
	delete(withoutCensus["healthObservatory"].(map[string]any)["telemetry"].(map[string]any), "census")
	encoded, _ := json.Marshal(withoutCensus)
	if err := loadConfig(string(encoded)); err == nil || !strings.Contains(err.Error(), "isProbeOnly requires") {
		t.Fatalf("unconsumed probe-only path accepted: %v", err)
	}
}

func TestHealthCensusRejectsAmbiguousReportKeysOnlyWhenEnabled(t *testing.T) {
	raw := `{"outbounds":[{"tag":"a","protocol":"freedom","selection":{"balancer":"first","group":"relay"}},{"tag":"b","protocol":"freedom","selection":{"balancer":"second","group":"relay"}},{"tag":"block","protocol":"blackhole"}],"routing":{"balancers":[{"tag":"first","fallbackTag":"block","strategy":{"type":"leastCost","settings":{"groups":[{"name":"relay","cost":1,"probe":"test"}]}}},{"tag":"second","fallbackTag":"block","strategy":{"type":"leastCost","settings":{"groups":[{"name":"relay","cost":1,"probe":"test"}]}}}]},"healthObservatory":{"probes":{"profiles":[{"name":"test","steps":[{"name":"payload","destinations":["https://example.test/check"]}]}]},"telemetry":{"report":{}}}}`
	if err := loadConfig(raw); err != nil {
		t.Fatal("census off compatibility:", err)
	}
	raw = strings.Replace(raw, `"report":{}`, `"report":{},"census":{"sampleFraction":1}`, 1)
	if err := loadConfig(raw); err == nil || !strings.Contains(err.Error(), "ambiguous report key relay") {
		t.Fatalf("colliding groups accepted: %v", err)
	}
	// A solo outbound named like another group's row also cannot be represented.
	raw = strings.Replace(raw, `"tag":"a"`, `"tag":"relay"`, 1)
	raw = strings.Replace(raw, `"balancer":"first","group":"relay"`, `"balancer":"first","cost":1,"probe":"test"`, 1)
	raw = strings.Replace(raw, `"groups":[{"name":"relay","cost":1,"probe":"test"}]`, `"groups":[]`, 1)
	if err := loadConfig(raw); err == nil || !strings.Contains(err.Error(), "ambiguous report key relay") {
		t.Fatalf("group/path collision accepted: %v", err)
	}
}
