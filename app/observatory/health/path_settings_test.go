package health

import (
	"testing"
	"time"
)

func TestPathProbeSelectionUsesOnlyNamedProfile(t *testing.T) {
	s := newSettings(&Config{
		Paths:  []*PathDefinition{{Tag: "plain"}, {Tag: "named", Probe: "default"}},
		Probes: []*ProbeProfile{{Name: "default", Destination: "https://example.test/probe"}},
	})
	if got := s.probeFor("plain").destination; got != defaultProbeDestination {
		t.Fatalf("plain probe %s", got)
	}
	if got := s.probeFor("named").destination; got != "https://example.test/probe" {
		t.Fatalf("named probe %s", got)
	}
	if got := s.probeFor("unlisted").destination; got != defaultProbeDestination {
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
		Paths: []*PathDefinition{{Tag: "legacy", Probe: "old"}, {Tag: "stepped", Probe: "full"}},
		Probes: []*ProbeProfile{
			{Name: "old", Destination: "https://legacy.test/check", MinBytes: 5},
			{Name: "full", Steps: []*ProbeStep{
				{Name: "transport", Destinations: []string{"https://node.test/check"}, MinBytes: 12},
				{Name: "egress", Destinations: []string{"https://one.test/204", "https://two.test/204"}, IsOptional: true,
					Method: "head", ExpectedStatuses: []int32{204}, Timeout: int64(2 * time.Second), ProgressTimeout: int64(time.Second)},
			}},
		},
	})
	legacy := s.probeFor("legacy")
	if len(legacy.steps) != 1 || legacy.steps[0].name != "old" || legacy.steps[0].isOptional ||
		legacy.steps[0].destinations[0] != "https://legacy.test/check" || legacy.steps[0].minBytes != 5 {
		t.Fatalf("legacy steps %+v", legacy.steps)
	}
	full := s.probeFor("stepped")
	if len(full.steps) != 2 || full.steps[0].name != "transport" || full.steps[0].isOptional ||
		full.steps[0].timeout != defaultProbeTimeout || full.steps[0].progressTimeout != defaultProbeProgress ||
		!full.steps[1].isOptional || len(full.steps[1].destinations) != 2 || full.steps[1].method != "HEAD" ||
		full.steps[1].timeout != 2*time.Second || full.steps[1].progressTimeout != time.Second ||
		!full.steps[1].statusOK(204) || full.steps[1].statusOK(200) {
		t.Fatalf("stepped profile %+v", full)
	}
	if full.destination != "https://node.test/check" || full.minBytes != 12 {
		t.Fatalf("legacy fields %+v", full)
	}
	builtin := s.probeFor("other")
	if len(builtin.steps) != 1 || builtin.steps[0].isOptional || builtin.steps[0].destinations[0] != defaultProbeDestination {
		t.Fatalf("built-in steps %+v", builtin.steps)
	}
}
