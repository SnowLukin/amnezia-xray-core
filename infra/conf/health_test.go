package conf_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	. "github.com/xtls/xray-core/infra/conf"
)

func TestHealthObservatoryConfig(t *testing.T) {
	creator := func() Buildable { return new(HealthObservatoryConfig) }
	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"subjectSelector": ["a", "b"],
				"passive": {"dialTimeout": "6s", "failures": 3},
				"connectivity": {"outbound": "direct", "destination": "https://example.com/204",
				                 "timeout": "4s", "maxPause": "90s",
				                 "recheck": {"initial": "2s", "multiplier": 2, "max": "30s"}}
			}`,
			Parser: loadJSON(creator),
			Output: &health.Config{
				SubjectSelector: []string{"a", "b"},
				Passive:         &health.PassiveConfig{DialTimeout: int64(6 * time.Second), Failures: 3},
				Connectivity: &health.ConnectivityConfig{
					Outbound:    "direct",
					Destination: "https://example.com/204",
					Timeout:     int64(4 * time.Second),
					MaxPause:    int64(90 * time.Second),
					Recheck:     &health.Backoff{Initial: int64(2 * time.Second), Multiplier: 2, Max: int64(30 * time.Second)},
				},
			},
		},
	})
}

func leastCostConfig(health, settings string, selector ...string) string {
	sel, _ := json.Marshal(selector)
	return `{
		"outbounds": [{"tag": "a1", "protocol": "freedom"}, {"tag": "b1", "protocol": "freedom"}],
		"routing": {"balancers": [{"tag": "auto", "selector": ` + string(sel) + `,
			"strategy": {"type": "leastCost", "settings": ` + settings + `}}]}` + health + `
	}`
}

func TestLeastCostConfigChecks(t *testing.T) {
	observed := `, "healthObservatory": {"subjectSelector": ["a", "b"]}`
	for _, tc := range []struct {
		name, input string
		ok          bool
	}{
		{"valid", leastCostConfig(observed, `{"costs": [{"match": "a", "value": 1}]}`, "a", "b"), true},
		{"no observatory", leastCostConfig("", `{}`, "a"), false},
		{"selector not observed", leastCostConfig(`, "healthObservatory": {"subjectSelector": ["a"]}`, `{}`, "a", "b"), false},
		{"zero cost", leastCostConfig(observed, `{"costs": [{"match": "a", "value": 0}]}`, "a"), false},
		{"broken regexp", leastCostConfig(observed, `{"costs": [{"regexp": true, "match": "a(", "value": 1}]}`, "a"), false},
	} {
		c := new(Config)
		if err := json.Unmarshal([]byte(tc.input), c); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		_, err := c.Build()
		if (err == nil) != tc.ok {
			t.Errorf("%s: build error %v, want ok %v", tc.name, err, tc.ok)
		}
	}
}

func TestHealthObservatoryConfigRejects(t *testing.T) {
	for _, input := range []string{
		`{}`,
		`{"subjectSelector": ["a"], "connectivity": {"outbound": "direct"}}`,
		`{"subjectSelector": ["a"], "connectivity": {"outbound": "direct", "destination": "ftp://x/"}}`,
		`{"subjectSelector": ["a"], "probes": [{"destination": "https://x/", "method": "POST"}]}`,
		`{"subjectSelector": ["a"], "probes": [{"destination": "https://x/", "method": "HEAD", "minBytes": 1}]}`,
		`{"subjectSelector": ["a"], "connectivity": {"outbound": "direct", "destination": "https://x/", "witnesses": [""]}}`,
		`{"subjectSelector": ["a"], "schedule": {"spacingJitter": 1}}`,
	} {
		c := new(HealthObservatoryConfig)
		if err := json.Unmarshal([]byte(input), c); err != nil {
			t.Fatalf("%s: %v", input, err)
		}
		if _, err := c.Build(); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
}
