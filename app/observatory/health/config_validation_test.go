package health

import (
	"context"
	"strings"
	"testing"

	"github.com/xtls/xray-core/core"
)

func TestNewRejectsInvalidProbeSteps(t *testing.T) {
	valid := func() *ProbeStep {
		return &ProbeStep{Name: "transport", Destinations: []string{"https://node.test/check"}}
	}
	for _, tc := range []struct {
		name    string
		profile *ProbeProfile
		field   string
	}{
		{"null profile", nil, "probes[0]"},
		{"empty profile name", &ProbeProfile{Steps: []*ProbeStep{valid()}}, "probes[0].name"},
		{"empty legacy name", &ProbeProfile{Destination: "https://node.test/check"}, "probes[0].name"},
		{"empty steps", &ProbeProfile{Name: "full", Steps: []*ProbeStep{}}, "steps"},
		{"null step", &ProbeProfile{Name: "full", Steps: []*ProbeStep{nil}}, "steps[0]"},
		{"optional first", &ProbeProfile{Name: "full", Steps: []*ProbeStep{{Name: "transport", Destinations: []string{"https://node.test/check"}, IsOptional: true}}}, "steps[0]"},
		{"no destinations", &ProbeProfile{Name: "full", Steps: []*ProbeStep{{Name: "transport"}}}, "destinations"},
		{"empty URL", &ProbeProfile{Name: "full", Steps: []*ProbeStep{{Name: "transport", Destinations: []string{""}}}}, "destinations"},
		{"five destinations", &ProbeProfile{Name: "full", Steps: []*ProbeStep{{Name: "transport", Destinations: []string{"https://a.test", "https://b.test", "https://c.test", "https://d.test", "https://e.test"}}}}, "destinations"},
		{"five steps", &ProbeProfile{Name: "full", Steps: []*ProbeStep{valid(), valid(), valid(), valid(), valid()}}, "steps"},
		{"duplicate names", &ProbeProfile{Name: "full", Steps: []*ProbeStep{valid(), valid()}}, "name"},
		{"mandatory after optional", &ProbeProfile{Name: "full", Steps: []*ProbeStep{
			valid(), {Name: "egress", Destinations: []string{"https://one.test"}, IsOptional: true},
			{Name: "egress_ip", Destinations: []string{"https://two.test"}},
		}}, "steps[2]"},
		{"invalid URL", &ProbeProfile{Name: "full", Steps: []*ProbeStep{{Name: "transport", Destinations: []string{"ftp://node.test/check"}}}}, "destinations"},
		{"bad method", &ProbeProfile{Name: "full", Steps: []*ProbeStep{{Name: "transport", Destinations: []string{"https://node.test/check"}, Method: "POST"}}}, "method"},
		{"HEAD body", &ProbeProfile{Name: "full", Steps: []*ProbeStep{{Name: "transport", Destinations: []string{"https://node.test/check"}, Method: "HEAD", MinBytes: 1}}}, "minBytes"},
		{"zero timeout", &ProbeProfile{Name: "full", Steps: []*ProbeStep{{Name: "transport", Destinations: []string{"https://node.test/check"}, Timeout: -1}}}, "timeout"},
		{"zero progress timeout", &ProbeProfile{Name: "full", Steps: []*ProbeStep{{Name: "transport", Destinations: []string{"https://node.test/check"}, ProgressTimeout: -1}}}, "progressTimeout"},
		{"profile method", &ProbeProfile{Name: "full", Method: "GET", Steps: []*ProbeStep{valid()}}, "method"},
		{"profile timeout", &ProbeProfile{Name: "full", Timeout: 1, Steps: []*ProbeStep{valid()}}, "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), core.XrayKey(1), new(core.Instance))
			_, err := New(ctx, &Config{Features: &FeatureConfig{StepProfiles: v2Bool(true)}, Probes: []*ProbeProfile{tc.profile}})
			if err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("New error %v must name %s", err, tc.field)
			}
		})
	}
}

func TestNewRejectsDuplicateProbeProfiles(t *testing.T) {
	step := &ProbeStep{Name: "transport", Destinations: []string{"https://node.test/check"}}
	stepped := func() *ProbeProfile { return &ProbeProfile{Name: "full", Steps: []*ProbeStep{step}} }
	legacy := func() *ProbeProfile { return &ProbeProfile{Name: "full", Destination: "https://node.test/check"} }
	for name, profiles := range map[string][]*ProbeProfile{
		"steps and steps":  {stepped(), stepped()},
		"legacy and steps": {legacy(), stepped()},
		"steps and legacy": {stepped(), legacy()},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), core.XrayKey(1), new(core.Instance))
			_, err := New(ctx, &Config{Probes: profiles})
			if err == nil || !strings.Contains(err.Error(), "probes[1].name") {
				t.Fatalf("New error %v must name duplicate profile", err)
			}
		})
	}
}

func TestValidateProbeStepsAllowsDefaults(t *testing.T) {
	cfg := &Config{Paths: []*PathDefinition{{Tag: "path", Probe: "full"}}, Probes: []*ProbeProfile{{Name: "full", Steps: []*ProbeStep{{
		Name: "transport", Destinations: []string{"https://node.test/check"},
	}}}}}
	if err := cfg.ValidateProbeSteps(); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), core.XrayKey(1), new(core.Instance))
	observer, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	step := observer.s.probeFor("path").steps[0]
	if step.timeout != defaultProbeTimeout || step.progressTimeout != defaultProbeProgress || step.method != "GET" {
		t.Fatalf("default step %+v", step)
	}
}

func TestNewAllowsNoProbeProfiles(t *testing.T) {
	ctx := context.WithValue(context.Background(), core.XrayKey(1), new(core.Instance))
	if _, err := New(ctx, &Config{}); err != nil {
		t.Fatal(err)
	}
}
