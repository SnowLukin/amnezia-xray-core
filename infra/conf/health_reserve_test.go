package conf_test

import (
	"fmt"
	"testing"
)

func TestProviderProbeOnlyRoadsPresence(t *testing.T) {
	for _, tc := range []struct {
		block   string
		enabled bool
		bad     bool
	}{
		{}, {block: `,"probeOnlyRoads":{}`, enabled: true},
		{block: `,"probeOnlyRoads":null`, bad: true}, {block: `,"probeOnlyRoads":true`, bad: true},
		{block: `,"probeOnlyRoads":{"isEnabled":false}`, bad: true}, {block: `,"probeOnlyRoads":{"limit":3}`, bad: true},
	} {
		cfg, err := buildHealth(t, fmt.Sprintf(`{"providers":[{"tag":"p","urls":["https://backend.test/set"],"apply":{"balancers":["auto"],"tagPrefix":"p-"}%s}]}`, tc.block))
		if tc.bad {
			if err == nil {
				t.Fatalf("accepted %s", tc.block)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Providers[0].GetProbeOnlyRoads() != tc.enabled {
			t.Fatal("presence lost")
		}
	}
}
