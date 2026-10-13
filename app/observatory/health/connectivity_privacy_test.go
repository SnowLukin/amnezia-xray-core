package health

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestConnectivityEventsExposeOnlyFailureClass(t *testing.T) {
	const privateError = `Get "https://private-probe.example/check": dial tcp 192.0.2.42:443: connection refused`
	for _, tc := range []struct {
		name   string
		result probeResult
		want   string
	}{
		{"failed", probeResult{err: privateError, class: "refused"}, "refused"},
		{"unclassified", probeResult{err: privateError}, "other"},
		{"passed", probeResult{ok: true, err: privateError, class: "refused"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := newTestEngine(t, connectivityConfig(), nil, "a1")
			te.events.drain()
			te.onConnectivity(tc.result, at(time.Second))
			raw, _ := te.events.drain()
			for _, secret := range []string{privateError, "private-probe.example", "192.0.2.42"} {
				if strings.Contains(string(raw), secret) {
					t.Fatalf("events expose a private probe destination: %s", raw)
				}
			}
			var events []Event
			if err := json.Unmarshal(raw, &events); err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, event := range events {
				if event.Type != "connectivity" && event.Type != "network_down" {
					continue
				}
				seen[event.Type] = true
				if got := event.Fields["error"]; got != tc.want {
					t.Fatalf("%s error = %v, want %q", event.Type, got, tc.want)
				}
			}
			if !seen["connectivity"] || (!tc.result.ok && !seen["network_down"]) {
				t.Fatalf("missing connectivity outcome events: %s", raw)
			}
			if te.networkDown == tc.result.ok {
				t.Fatalf("network down = %v after ok = %v", te.networkDown, tc.result.ok)
			}
		})
	}
}
