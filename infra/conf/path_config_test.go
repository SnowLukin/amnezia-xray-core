package conf

import (
	"encoding/json"
	"fmt"
	"github.com/xtls/xray-core/app/observatory/health"
	"strings"
	"testing"
	"time"
)

func pathConfig(selection, strategy, observatory string) string {
	var h map[string]any
	if json.Unmarshal([]byte(observatory), &h) == nil && h != nil {
		probes, _ := h["probes"].(map[string]any)
		if probes == nil {
			probes = map[string]any{}
			h["probes"] = probes
		}
		probes["profiles"] = []any{map[string]any{"name": "test", "steps": []any{map[string]any{"name": "test", "destinations": []string{"https://example.test/204"}}}}}
		body, _ := json.Marshal(h)
		observatory = string(body)
	}
	var fields map[string]any
	if json.Unmarshal([]byte("{"+strings.TrimPrefix(selection, ",")+"}"), &fields) == nil {
		if p, ok := fields["selection"].(map[string]any); ok && p["group"] == nil && p["probe"] == nil {
			p["probe"] = "test"
			body, _ := json.Marshal(fields)
			selection = "," + string(body[1:len(body)-1])
		}
	}
	var settings map[string]any
	if json.Unmarshal([]byte(strategy), &settings) == nil {
		if groups, ok := settings["groups"].([]any); ok {
			for _, item := range groups {
				g := item.(map[string]any)
				if g["probe"] == nil {
					g["probe"] = "test"
				}
			}
			body, _ := json.Marshal(settings)
			strategy = string(body)
		}
	}
	return fmt.Sprintf(`{"outbounds":[{"tag":"a","protocol":"freedom"%s},{"tag":"block","protocol":"blackhole"}],"routing":{"balancers":[{"tag":"auto","fallbackTag":"block","strategy":{"type":"leastCost","settings":%s}}]},"healthObservatory":%s}`, selection, strategy, observatory)
}
func pathConfigError(raw string) error {
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return err
	}
	_, err := c.Build()
	return err
}
func TestSelectionLoadValidation(t *testing.T) {
	group := `{"groups":[{"name":"relay","cost":2}]}`
	for _, tc := range []struct{ name, selection, strategy, observer, want string }{
		{"no candidates", "", `{}`, `{}`, "no selections and no enabled provider"},
		{"unknown balancer", `,"selection":{"balancer":"missing","cost":1}`, `{}`, `{}`, "selection.balancer missing"},
		{"legacy path", `,"path":{"balancer":"auto","cost":1}`, `{}`, `{}`, "path is not supported"},
		{"group and cost", `,"selection":{"balancer":"auto","group":"relay","cost":1}`, group, `{}`, "cost is set by group relay"},
		{"group and regular", `,"selection":{"balancer":"auto","group":"relay","probePolicy":"regular"}`, group, `{}`, "probePolicy is set by group relay"},
		{"group and return block", `,"selection":{"balancer":"auto","group":"relay","returnToCheaper":{"isEnabled":false}}`, group, `{}`, "returnToCheaper is set by group relay"},
		{"group and probe", `,"selection":{"balancer":"auto","group":"relay","probe":""}`, group, `{}`, "probe is set by group relay"},
		{"null cost", `,"selection":{"balancer":"auto","cost":null}`, `{}`, `{}`, "selection.cost must not be null"},
		{"null policy", `,"selection":{"balancer":"auto","probePolicy":null}`, `{}`, `{}`, "selection.probePolicy must not be null"},
		{"null return", `,"selection":{"balancer":"auto","returnToCheaper":null}`, `{}`, `{}`, "selection.returnToCheaper must not be null"},
		{"empty group", `,"selection":{"balancer":"auto","group":"","cost":1}`, `{}`, `{}`, "group needs nonempty name"},
		{"no cost", `,"selection":{"balancer":"auto"}`, `{}`, `{}`, "cost above zero"},
		{"unknown group", `,"selection":{"balancer":"auto","group":"missing"}`, group, `{}`, "unknown group missing"},
		{"groups absent", `,"selection":{"balancer":"auto","group":"relay"}`, `{}`, `{}`, "balancer.groups"},
		{"duplicate group", `,"selection":{"balancer":"auto","group":"relay"}`, `{"groups":[{"name":"relay","cost":1},{"name":"relay","cost":2}]}`, `{}`, "duplicate group relay"},
		{"unknown probe", `,"selection":{"balancer":"auto","cost":1,"probe":"absent"}`, `{}`, `{}`, "unknown probe absent"},
		{"unknown group probe", `,"selection":{"balancer":"auto","group":"relay"}`, `{"groups":[{"name":"relay","cost":1,"probe":"absent"}]}`, `{}`, "unknown probe absent"},
		{"removed isLazy", `,"selection":{"balancer":"auto","cost":1,"isLazy":false}`, `{}`, `{}`, "isLazy"},
		{"removed returnInterval", `,"selection":{"balancer":"auto","cost":1,"returnInterval":"1m"}`, `{}`, `{}`, "returnInterval"},
		{"bad probe policy", `,"selection":{"balancer":"auto","cost":1,"probePolicy":"lazy"}`, `{}`, `{}`, "probePolicy"},
		{"return requires balancer", `,"selection":{"balancer":"auto","cost":1,"returnToCheaper":{"interval":"1m"}}`, `{}`, `{}`, "balancer.returnToCheaper"},
		{"return zero validates disabled", `,"selection":{"balancer":"auto","cost":1,"returnToCheaper":{"isEnabled":false,"interval":"0s"}}`, `{}`, `{}`, "returnToCheaper.interval"},
		{"probeOnly requires candidates", `,"selection":{"balancer":"auto","cost":1,"isProbeOnly":true}`, `{}`, `{}`, "candidates"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := pathConfigError(pathConfig(tc.selection, tc.strategy, tc.observer)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v want %s", err, tc.want)
			}
		})
	}
	if err := pathConfigError(strings.Replace(pathConfig(`,"selection":{"balancer":"auto","cost":1}`, `{}`, `{}`), `"leastCost"`, `"random"`, 1)); err == nil || !strings.Contains(err.Error(), "must name a leastCost") {
		t.Fatal(err)
	}
	if err := pathConfigError(strings.Replace(pathConfig(`,"selection":{"balancer":"auto","cost":1}`, `{}`, `{}`), `"tag":"auto","fallbackTag"`, `"tag":"auto","selector":["a"],"fallbackTag"`, 1)); err == nil || !strings.Contains(err.Error(), "leastCost selector") {
		t.Fatal(err)
	}
}
func TestSelectionDeclarationsKeepOrderAndDurationPrecision(t *testing.T) {
	raw := `{"outbounds":[{"tag":"relay-10","protocol":"freedom","selection":{"balancer":"auto","group":"relay","labels":{"node":"member","location":"DE"}}},{"tag":"ignored","protocol":"freedom"},{"tag":"relay-2","protocol":"freedom","selection":{"balancer":"auto","group":"relay"}},{"tag":"solo","protocol":"freedom","selection":{"balancer":"auto","cost":3,"probePolicy":"onDemand","returnToCheaper":{"interval":"1.000000001s"},"labels":{"node":"solo"},"probe":"test"}}],"routing":{"balancers":[{"tag":"auto","fallbackTag":"ignored","strategy":{"type":"leastCost","settings":{"returnToCheaper":{},"groups":[{"name":"relay","cost":1,"returnToCheaper":{"isEnabled":false},"labels":{"location":"NL"},"probe":"test"}]}}}]},"healthObservatory":{"probes":{"profiles":[{"name":"test","steps":[{"name":"test","destinations":["https://example.test/204"]}]}]}}}`
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Build(); err != nil {
		t.Fatal(err)
	}
	p := c.HealthObservatory.Paths
	if len(p) != 3 || p[0].Tag != "relay-10" || p[1].Tag != "relay-2" || p[2].Tag != "solo" || p[2].ReturnInterval != int64(time.Second)+1 || !p[2].IsLazy || p[2].Labels["node"] != "solo" || p[0].Labels["location"] != "DE" || p[0].Labels["node"] != "member" || p[1].Labels["location"] != "NL" {
		t.Fatalf("selections %v", p)
	}
	built, err := c.RouterConfig.Balancers[0].leastCost.Build()
	if err != nil {
		t.Fatal(err)
	}
	g := built.(*health.SelectorConfig).Groups[0]
	if g.ReturnIsEnabled == nil || *g.ReturnIsEnabled || g.Labels["location"] != "NL" {
		t.Fatal(g)
	}
	// Building twice must preserve order and avoid treating generated selectors as input.
	if _, err := c.Build(); err != nil {
		t.Fatal(err)
	}
	if len(c.HealthObservatory.Paths) != 3 {
		t.Fatal("duplicate generated paths")
	}
}
func TestProviderOnlyLeastCostAllowsNoBaseSelections(t *testing.T) {
	if err := pathConfigError(pathConfig("", `{}`, `{"providers":[{"tag":"fresh","apply":{"balancer":"auto","tagPrefix":"p-"},"urls":["https://x.test/set"]}]}`)); err != nil {
		t.Fatal(err)
	}
	if err := pathConfigError(pathConfig("", `{}`, `{}`)); err == nil {
		t.Fatal("absent provider supplied candidates")
	}
	if err := pathConfigError(pathConfig(`,"selection":{"balancer":"auto","cost":1}`, `{"groups":[{"name":"future","cost":2}]}`, `{}`)); err == nil || !strings.Contains(err.Error(), "has no outbounds") {
		t.Fatalf("present unused group accepted: %v", err)
	}
}

func TestLeastCostMinimumHoldPresence(t *testing.T) {
	for _, value := range []string{`{}`, `{"minHoldTime":"0s"}`, `{"minHoldTime":"1.000000001s"}`} {
		var c Config
		if err := json.Unmarshal([]byte(pathConfig(`,"selection":{"balancer":"auto","cost":1}`, `{"returnToCheaper":`+value+`}`, `{}`)), &c); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Build(); err != nil {
			t.Fatal(err)
		}
		built, err := c.RouterConfig.Balancers[0].leastCost.Build()
		if err != nil {
			t.Fatal(err)
		}
		got := built.(*health.SelectorConfig).MinHoldTime
		if value == `{}` {
			if got != nil {
				t.Fatal("missing minimum hold became explicit")
			}
		} else {
			want := int64(0)
			if strings.Contains(value, "1.000000001") {
				want = int64(time.Second) + 1
			}
			if got == nil || *got != want {
				t.Fatalf("minimum hold %s=%v", value, got)
			}
		}
	}
}
