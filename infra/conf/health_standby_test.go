package conf

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
)

func TestStandbyScheduleJSON(t *testing.T) {
	for _, tc := range []struct {
		raw      string
		interval int64
	}{
		{`{}`, 0},
		{`{"standby":{}}`, 0},
		{`{"standby":{"recheckInterval":"1.000000001s"}}`, int64(time.Second) + 1},
	} {
		var cfg HealthObservatoryConfig
		if err := json.Unmarshal([]byte(`{"probes":`+tc.raw+`}`), &cfg); err != nil {
			t.Fatal(err)
		}
		built, err := cfg.Build()
		if err != nil {
			t.Fatal(err)
		}
		if got := built.(*health.Config).GetSchedule().GetStandby().GetRecheckInterval(); got != tc.interval {
			t.Fatalf("%s: recheck interval %d", tc.raw, got)
		}
	}
	for _, raw := range []string{`{"recheckInterval":"0s"}`, `{"recheckInterval":"-1s"}`, `{"recheckInterval":null}`, `{"unknown":true}`} {
		var cfg HealthObservatoryConfig
		err := json.Unmarshal([]byte(`{"probes":{"standby":`+raw+`}}`), &cfg)
		if err == nil {
			_, err = cfg.Build()
		}
		if err == nil || !strings.Contains(err.Error(), "standby") {
			t.Errorf("%s: error %v", raw, err)
		}
	}
}

// standby probes on demand, as onDemand does, and marks the outbound
// or the group to be kept checked while not chosen.
func TestProbePolicyStandby(t *testing.T) {
	raw := `{"outbounds":[{"tag":"solo","protocol":"freedom","selection":{"balancer":"auto","cost":2,"probePolicy":"standby","probe":"test"}},{"tag":"lazy","protocol":"freedom","selection":{"balancer":"auto","cost":3,"probePolicy":"onDemand","probe":"test"}},{"tag":"relay-1","protocol":"freedom","selection":{"balancer":"auto","group":"relay"}},{"tag":"block","protocol":"blackhole"}],"routing":{"balancers":[{"tag":"auto","fallbackTag":"block","strategy":{"type":"leastCost","settings":{"groups":[{"name":"relay","cost":1,"probe":"test","probePolicy":"standby"}]}}}]},"healthObservatory":{"probes":{"profiles":[{"name":"test","steps":[{"name":"test","destinations":["https://example.test/204"]}]}]}}}`
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Build(); err != nil {
		t.Fatal(err)
	}
	p := c.HealthObservatory.Paths
	if len(p) != 3 || p[0].Tag != "solo" || !p[0].IsLazy || !p[0].IsStandby || p[1].Tag != "lazy" || !p[1].IsLazy || p[1].IsStandby {
		t.Fatalf("selections %v", p)
	}
	built, err := c.RouterConfig.Balancers[0].leastCost.Build()
	if err != nil {
		t.Fatal(err)
	}
	if g := built.(*health.SelectorConfig).Groups[0]; !g.IsLazy || !g.IsStandby {
		t.Fatalf("group %v", g)
	}
	err = pathConfigError(pathConfig(`,"selection":{"balancer":"auto","cost":1,"probePolicy":"keepVerified"}`, `{}`, `{}`))
	if err == nil || !strings.Contains(err.Error(), "probePolicy must be regular, onDemand or standby") {
		t.Fatalf("unknown policy: %v", err)
	}
}
