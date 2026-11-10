package health

import (
	"testing"
	"time"
)

func TestPathProbeSelectionUsesOnlyNamedProfile(t *testing.T) {
	s := newSettings(&Config{
		Paths:  []*PathDefinition{{Tag: "plain"}, {Tag: "named", Probe: "default"}},
		Probes: []*ProbeProfile{stepProfile("default", "https://example.test/probe", 0)},
	})
	if got := s.probeFor("plain").steps[0].destinations[0]; got != "" {
		t.Fatalf("plain probe %s", got)
	}
	if got := s.probeFor("named").steps[0].destinations[0]; got != "https://example.test/probe" {
		t.Fatalf("named probe %s", got)
	}
	if got := s.probeFor("unlisted").steps[0].destinations[0]; got != "" {
		t.Fatalf("unlisted probe %s", got)
	}
}

func TestExactPathObservation(t *testing.T) {
	s := newSettings(&Config{Paths: []*PathDefinition{{Tag: "relay-2"}}})
	if !s.observes("relay-2") || s.observes("relay-20") || s.observes("other") {
		t.Fatal("observation used a prefix")
	}
}

func TestProbeStepsNormalization(t *testing.T) {
	s := newSettings(&Config{
		Paths: []*PathDefinition{{Tag: "stepped", Probe: "full"}},
		Probes: []*ProbeProfile{
			{Name: "full", Steps: []*ProbeStep{
				{Name: "transport", Destinations: []string{"https://node.test/check"}, MinBytes: 12},
				{Name: "egress", Destinations: []string{"https://one.test/204", "https://two.test/204"}, IsOptional: true,
					Method: "head", ExpectedStatuses: []int32{204}, Timeout: int64(2 * time.Second), ProgressTimeout: int64(time.Second)},
			}},
		},
	})
	full := s.probeFor("stepped")
	if len(full.steps) != 2 || full.steps[0].name != "transport" || full.steps[0].isOptional ||
		full.steps[0].timeout != defaultProbeTimeout || full.steps[0].progressTimeout != defaultProbeProgress ||
		!full.steps[1].isOptional || len(full.steps[1].destinations) != 2 || full.steps[1].method != "HEAD" ||
		full.steps[1].timeout != 2*time.Second || full.steps[1].progressTimeout != time.Second ||
		!full.steps[1].statusOK(204) || full.steps[1].statusOK(200) {
		t.Fatalf("stepped profile %+v", full)
	}
	builtin := s.probeFor("other")
	if len(builtin.steps) != 1 || builtin.steps[0].destinations[0] != "" {
		t.Fatalf("built-in steps %+v", builtin.steps)
	}
}

func TestProbeOnlyRunsAllMandatorySteps(t *testing.T) {
	s := newSettings(&Config{
		Paths: []*PathDefinition{{Tag: "candidate", Probe: "full", IsProbeOnly: true}, {Tag: "real", Probe: "full"}},
		Probes: []*ProbeProfile{{Name: "full", Steps: []*ProbeStep{
			{Name: "transport", Destinations: []string{"http://node.test/check"}, MinBytes: 32768},
			{Name: "transport-second", Destinations: []string{"http://other.test/check"}, MinBytes: 8192},
			{Name: "egress", Destinations: []string{"https://egress.test/204"}, IsOptional: true},
		}}},
	})
	candidate := s.probeFor("candidate")
	if !candidate.isProbeOnly || !s.isProbeOnly("candidate") || len(candidate.steps) != 2 ||
		candidate.steps[0].name != "transport" || candidate.steps[0].minBytes != 32768 ||
		candidate.steps[1].name != "transport-second" || candidate.steps[1].minBytes != 8192 {
		t.Fatalf("candidate profile %+v", candidate)
	}
	for _, step := range candidate.steps {
		if step.isOptional {
			t.Fatalf("candidate kept optional step %+v", step)
		}
	}
	real := s.probeFor("real")
	if real.isProbeOnly || s.isProbeOnly("real") || len(real.steps) != 3 || !real.steps[2].isOptional {
		t.Fatalf("real profile %+v", real)
	}
}

func TestProbeOnlyProfileSnapshotSurvivesModeSwitch(t *testing.T) {
	s := newSettings(&Config{
		Paths: []*PathDefinition{{Tag: "same", Probe: "full", IsProbeOnly: true}},
		Probes: []*ProbeProfile{{Name: "full", Steps: []*ProbeStep{
			{Name: "transport", Destinations: []string{"http://node.test/check"}},
			{Name: "egress", Destinations: []string{"https://egress.test/204"}, IsOptional: true},
		}}},
	})
	candidate := s.probeFor("same")
	s.paths.Store("same", pathSettings{probe: "full"})
	real := s.probeFor("same")
	if !candidate.isProbeOnly || len(candidate.steps) != 1 || real.isProbeOnly || len(real.steps) != 2 {
		t.Fatalf("promotion changed snapshots: candidate %+v, real %+v", candidate, real)
	}
	s.paths.Store("same", pathSettings{probe: "full", isProbeOnly: true})
	demoted := s.probeFor("same")
	if !demoted.isProbeOnly || len(demoted.steps) != 1 || real.isProbeOnly || len(real.steps) != 2 {
		t.Fatalf("demotion changed snapshots: demoted %+v, real %+v", demoted, real)
	}
}

func TestProbeOnlyWithoutProfileHasNoExternalProbe(t *testing.T) {
	s := newSettings(&Config{Paths: []*PathDefinition{{Tag: "builtin", IsProbeOnly: true}}})
	if p := s.probeFor("builtin"); !p.isProbeOnly || len(p.steps) != 1 || p.steps[0].destinations[0] != "" {
		t.Fatalf("unconfigured candidate created external probe %+v", p)
	}
}

func stepProfile(name, destination string, minBytes int64) *ProbeProfile {
	return &ProbeProfile{Name: name, Steps: []*ProbeStep{{Name: name, Destinations: []string{destination}, MinBytes: minBytes}}}
}
