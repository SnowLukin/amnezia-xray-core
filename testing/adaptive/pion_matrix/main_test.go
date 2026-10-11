package main

import "testing"

func TestApplicationRouteRejectsAnyOtherDestination(t *testing.T) {
	expected := "198.18.0.1:34014"
	valid := []routeEvent{
		{AtUnixNano: 150, Direction: "send", Destination: expected, ApplicationData: true},
		{AtUnixNano: 160, Direction: "recv", Destination: expected, ApplicationData: true},
	}
	if !applicationRouteProved(valid, expected) {
		t.Fatal("expected bidirectional application route rejected")
	}
	for _, direction := range []string{"send", "recv"} {
		t.Run(direction, func(t *testing.T) {
			events := append(append([]routeEvent(nil), valid...), routeEvent{
				AtUnixNano: 170, Direction: direction, Destination: "192.0.2.100:55555", ApplicationData: true,
			})
			routes := map[string]any{"events": events}
			if applicationRouteProved(applicationRoutes(routes, 100, 200), expected) {
				t.Fatal("foreign destination in transfer window accepted despite valid send and recv")
			}
			if !applicationRouteProved(applicationRoutes(routes, 100, 165), expected) {
				t.Fatal("packet outside transfer window polluted application route")
			}
		})
	}
	if applicationRouteProved(valid[:1], expected) {
		t.Fatal("send without receive accepted")
	}
}
