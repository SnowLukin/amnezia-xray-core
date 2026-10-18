package conf_test

import (
	"fmt"
	"strings"
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

func TestTelemetryTraceConfig(t *testing.T) {
	base := `{"probes":{"profiles":[{"name":"check","steps":[{"name":"transport","destinations":["http://127.0.0.1/probe"]}]}]},"telemetry":{"trace":%s}}`
	for raw, want := range map[string]string{`{}`: "", `{"maxBytes":131072,"connections":{"isEnabled":false}}`: "", `{"maxBytes":100}`: "maxBytes", `{"connections":{"isEnabled":true}}`: "only accepts false", `{"connections":{"x":1}}`: "x"} {
		_, err := buildHealth(t, fmt.Sprintf(base, raw))
		if (want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), want)) {
			t.Errorf("%s: %v", raw, err)
		}
	}
}
