package conf

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
)

func TestProbeOnlyScheduleJSON(t *testing.T) {
	for _, tc := range []struct {
		raw      string
		verified int32
		interval int64
	}{
		{`{}`, 0, 0},
		{`{"keepVerified":1}`, 1, 0},
		{`{"keepVerified":4,"recheckInterval":"1.000000001s"}`, 4, int64(time.Second) + 1},
	} {
		var cfg HealthObservatoryConfig
		if err := json.Unmarshal([]byte(`{"probes":{"candidates":`+tc.raw+`}}`), &cfg); err != nil {
			t.Fatal(err)
		}
		built, err := cfg.Build()
		if err != nil {
			t.Fatal(err)
		}
		po := built.(*health.Config).GetSchedule().GetProbeOnly()
		if po.GetKeepVerified() != tc.verified || po.GetRecheckInterval() != tc.interval {
			t.Fatalf("%s: probe-only config %+v", tc.raw, po)
		}
	}
}

func TestProbeOnlyScheduleJSONRejects(t *testing.T) {
	for _, tc := range []struct{ raw, field string }{
		{`{"keepVerified":0}`, "keepVerified"},
		{`{"keepVerified":-1}`, "keepVerified"},
		{`{"keepVerified":5}`, "keepVerified"},
		{`{"keepVerified":null}`, "keepVerified"},
		{`{"keepVerified":"1"}`, "keepVerified"},
		{`{"recheckInterval":"0s"}`, "recheckInterval"},
		{`{"recheckInterval":"-1s"}`, "recheckInterval"},
		{`{"recheckInterval":null}`, "recheckInterval"},
		{`{"unknown":true}`, "unknown"},
	} {
		var cfg HealthObservatoryConfig
		err := json.Unmarshal([]byte(`{"probes":{"candidates":`+tc.raw+`}}`), &cfg)
		if err == nil {
			_, err = cfg.Build()
		}
		if err == nil || !strings.Contains(err.Error(), tc.field) {
			t.Errorf("%s: error %v must name %s", tc.raw, err, tc.field)
		}
	}
}

func TestProbeOnlyPathJSONAndStartValidation(t *testing.T) {
	for _, group := range []bool{false, true} {
		path := `,"selection":{"balancer":"auto","cost":1,"isProbeOnly":true}`
		settings := `{}`
		if group {
			path = `,"selection":{"balancer":"auto","group":"relay","isProbeOnly":true}`
			settings = `{"groups":[{"name":"relay","cost":1}]}`
		}
		t.Run(fmt.Sprintf("group=%v", group), func(t *testing.T) {
			var cfg Config
			if err := json.Unmarshal([]byte(pathConfig(path, settings, `{"probes":{"candidates":{}}}`)), &cfg); err != nil {
				t.Fatal(err)
			}
			built, err := cfg.Build()
			if err != nil {
				t.Fatal(err)
			}
			if len(built.Outbound) == 0 || !built.Outbound[0].IsProbeOnly {
				t.Fatal("handler lost immutable probe credentials mode")
			}
			if len(cfg.HealthObservatory.Paths) != 1 || !cfg.HealthObservatory.Paths[0].IsProbeOnly {
				t.Fatalf("probe-only paths %+v", cfg.HealthObservatory.Paths)
			}
			for _, field := range []string{"outbound", "pin"} {
				startup := `"startup":{"outbound":"a"}`
				if field == "pin" {
					startup = `"startup":{"pin":{"outbound":"a","timeout":"0s"}}`
				}
				startSettings := `{` + startup + `}`
				if group {
					startSettings = `{"groups":[{"name":"relay","cost":1}],` + startup + `}`
				}
				observer := `{"probes":{"candidates":{}},"host":{"pinning":{}}}`
				if err := pathConfigError(pathConfig(path, startSettings, observer)); err == nil || !strings.Contains(err.Error(), "probe-only") {
					t.Fatalf("%s: %v", field, err)
				}
				ordinary := strings.Replace(path, `"isProbeOnly":true`, `"isProbeOnly":false`, 1)
				if err := pathConfigError(pathConfig(ordinary, startSettings, observer)); err != nil {
					t.Fatalf("ordinary %s: %v", field, err)
				}
			}
		})
	}
}

func TestProbeOnlyPathJSONRequiresBoolean(t *testing.T) {
	for _, value := range []string{`{}`, `1`, `"true"`, `{}`, `[]`} {
		raw := pathConfig(`,"selection":{"balancer":"auto","cost":1,"isProbeOnly":`+value+`}`, `{}`, `{}`)
		if err := pathConfigError(raw); err == nil || !strings.Contains(err.Error(), "isProbeOnly") {
			t.Errorf("%s: error %v must name isProbeOnly", value, err)
		}
	}
}
