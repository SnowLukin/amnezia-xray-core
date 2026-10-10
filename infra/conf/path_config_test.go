package conf

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"time"
)

func pathConfig(outboundPath, strategy, observatory string) string {
	return fmt.Sprintf(`{"outbounds":[{"tag":"a","protocol":"freedom"%s}],
		"routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":%s}}]},
		"healthObservatory":%s}`, outboundPath, strategy, observatory)
}

func pathConfigError(raw string) error {
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return err
	}
	_, err := c.Build()
	return err
}

func TestPathLoadValidation(t *testing.T) {
	provider := `{"providers":[{"tag":"fresh","url":"https://example.test/set","apply":{"balancer":"auto","tagPrefix":"p-"}}]}`
	for _, tc := range []struct{ name, raw, want string }{
		{"missing path has no candidates", pathConfig("", `{}`, `{}`), "no paths and no provider"},
		{"unknown balancer", pathConfig(`,"path":{"balancer":"missing","cost":1}`, `{}`, `{}`), "outbound a: path.balancer missing"},
		{"non leastCost balancer", strings.Replace(pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{}`, `{}`), `"leastCost"`, `"random"`, 1), "must name a leastCost balancer"},
		{"group and cost", pathConfig(`,"path":{"balancer":"auto","group":"relay","cost":1}`, `{"groups":[{"name":"relay","cost":2}]}`, `{}`), "cost is set by group relay"},
		{"group and explicit false", pathConfig(`,"path":{"balancer":"auto","group":"relay","isLazy":false}`, `{"groups":[{"name":"relay","cost":2}]}`, `{}`), "isLazy is set by group relay"},
		{"group and explicit zero", pathConfig(`,"path":{"balancer":"auto","group":"relay","returnInterval":"0s"}`, `{"groups":[{"name":"relay","cost":2}]}`, `{}`), "returnInterval is set by group relay"},
		{"group and empty probe", pathConfig(`,"path":{"balancer":"auto","group":"relay","probe":""}`, `{"groups":[{"name":"relay","cost":2}]}`, `{}`), "probe is set by group relay"},
		{"group and null cost", pathConfig(`,"path":{"balancer":"auto","group":"relay","cost":null}`, `{"groups":[{"name":"relay","cost":2}]}`, `{}`), "path.cost must not be null"},
		{"group and null lazy", pathConfig(`,"path":{"balancer":"auto","group":"relay","isLazy":null}`, `{"groups":[{"name":"relay","cost":2}]}`, `{}`), "path.isLazy must not be null"},
		{"group and null interval", pathConfig(`,"path":{"balancer":"auto","group":"relay","returnInterval":null}`, `{"groups":[{"name":"relay","cost":2}]}`, `{}`), "path.returnInterval must not be null"},
		{"group and null probe", pathConfig(`,"path":{"balancer":"auto","group":"relay","probe":null}`, `{"groups":[{"name":"relay","cost":2}]}`, `{}`), "path.probe must not be null"},
		{"empty group name", pathConfig(`,"path":{"balancer":"auto","group":"","cost":1}`, `{}`, `{}`), "group needs nonempty name"},
		{"no group or cost", pathConfig(`,"path":{"balancer":"auto"}`, `{}`, `{}`), "path needs cost above zero"},
		{"unknown group", pathConfig(`,"path":{"balancer":"auto","group":"missing"}`, `{}`, `{}`), "unknown group missing"},
		{"base group empty even with provider", pathConfig("", `{"groups":[{"name":"relay","cost":1}]}`, provider), "group relay has no outbounds"},
		{"duplicate group", pathConfig(`,"path":{"balancer":"auto","group":"relay"}`, `{"groups":[{"name":"relay","cost":1},{"name":"relay","cost":2}]}`, `{}`), "duplicate group relay"},
		{"unknown probe", pathConfig(`,"path":{"balancer":"auto","cost":1,"probe":"absent"}`, `{}`, `{}`), "unknown probe absent"},
		{"unknown group probe", pathConfig(`,"path":{"balancer":"auto","group":"relay"}`, `{"groups":[{"name":"relay","cost":1,"probe":"absent"}]}`, `{}`), "unknown probe absent"},
		{"user supplied path definitions", pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{"paths":[{"tag":"injected","cost":1}]}`, `{}`), "paths is not supported"},
		{"leastCost selector", strings.Replace(pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{}`, `{}`), `"tag":"auto","strategy"`, `"tag":"auto","selector":["a"],"strategy"`, 1), "leastCost selector is not supported"},
		{"removed costs", pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{"costs":[]}`, `{}`), "costs"},
		{"removed defaultCost", pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{"defaultCost":1}`, `{}`), "defaultCost"},
		{"removed lazyOutbounds", pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{"lazyOutbounds":["a"]}`, `{}`), "lazyOutbounds"},
		{"removed subjectSelector", pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{}`, `{"subjectSelector":["a"]}`), "subjectSelector"},
		{"removed probe selector", pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{}`, `{"probes":[{"name":"p","selector":["a"]}]}`), "probes[].selector"},
		{"unnamed probe profile", pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{}`, `{"probes":[{"destination":"https://example.test/"}]}`), "probes[].name"},
		{"duplicate probe profile", pathConfig(`,"path":{"balancer":"auto","cost":1}`, `{}`, `{"probes":[{"name":"p"},{"name":"p"}]}`), "probes[].name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := pathConfigError(tc.raw); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
}

func TestPathDeclarationsKeepOrderAndDurationPrecision(t *testing.T) {
	raw := `{"outbounds":[{"tag":"relay-10","protocol":"freedom","path":{"balancer":"auto","group":"relay"}},
		{"tag":"ignored","protocol":"freedom"},
		{"tag":"relay-2","protocol":"freedom","path":{"balancer":"auto","group":"relay"}},
		{"tag":"solo","protocol":"freedom","path":{"balancer":"auto","cost":3,"returnInterval":"1.000000001s"}}],
		"routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{"groups":[{"name":"relay","cost":1,"returnInterval":"0s"}]}}}]},
		"healthObservatory":{}}`
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Build(); err != nil {
		t.Fatal(err)
	}
	p := c.HealthObservatory.Paths
	if len(p) != 3 || p[0].Tag != "relay-10" || p[1].Tag != "relay-2" || p[2].Tag != "solo" {
		t.Fatalf("path order %+v", p)
	}
	if p[2].ReturnInterval != int64(time.Second)+1 {
		t.Fatalf("solo interval %d", p[2].ReturnInterval)
	}
	var settings strategyLeastCostConfig
	if err := json.Unmarshal(*c.RouterConfig.Balancers[0].Strategy.Settings, &settings); err != nil {
		t.Fatal(err)
	}
	built, err := settings.Build()
	if err != nil {
		t.Fatal(err)
	}
	if got := built.(*health.SelectorConfig).Groups[0].ReturnInterval; got != -1 {
		t.Fatalf("group explicit zero %d", got)
	}
}

func TestProviderOnlyLeastCostAllowsNoBasePaths(t *testing.T) {
	raw := pathConfig("", `{}`, `{"providers":[{"tag":"fresh","url":"https://example.test/set","apply":{"balancer":"auto","tagPrefix":"p-"}}]}`)
	if err := pathConfigError(raw); err != nil {
		t.Fatal(err)
	}
}
