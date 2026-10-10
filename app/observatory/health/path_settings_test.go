package health

import "testing"

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
