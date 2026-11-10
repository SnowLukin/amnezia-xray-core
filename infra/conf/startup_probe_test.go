package conf_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/app/router"
	. "github.com/xtls/xray-core/infra/conf"
)

// builtSelector is the leastCost settings the router hands the observatory
// for the balancer auto of leastCostConfig.
func builtSelector(t *testing.T, settings string) *health.SelectorConfig {
	t.Helper()
	var c Config
	if err := json.Unmarshal([]byte(leastCostConfig(`,"healthObservatory":{}`, settings)), &c); err != nil {
		t.Fatal(err)
	}
	built, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	for _, app := range built.App {
		msg, _ := app.GetInstance()
		r, ok := msg.(*router.Config)
		if !ok {
			continue
		}
		inst, err := r.BalancingRule[0].StrategySettings.GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		return inst.(*health.SelectorConfig)
	}
	t.Fatal("no router config")
	return nil
}

// startup.probe is on by its presence. The selector of the balancer goes to
// the observatory with the settings, with the block or without it.
func TestStartupProbeBlock(t *testing.T) {
	on := builtSelector(t, `{"startup":{"probe":{}}}`)
	if !on.GetFeatures().GetStartupProbe() || !slices.Equal(on.OutboundSelector, []string{"a1", "b1"}) {
		t.Fatalf("with the block: %v, selector %v", on.GetFeatures(), on.OutboundSelector)
	}
	off := builtSelector(t, `{"startup":{"outbound":"a1"}}`)
	if off.GetFeatures().StartupProbe != nil || !slices.Equal(off.OutboundSelector, []string{"a1", "b1"}) {
		t.Fatalf("without the block: %v, selector %v", off.GetFeatures(), off.OutboundSelector)
	}
}

func TestStartupProbeBlockIsChecked(t *testing.T) {
	for _, tc := range []struct{ name, probe, want string }{
		{"unknown key", `{"timeout":"5s"}`, "leastCost.settings.startup.probe.timeout: unknown key"},
		{"flag", `{"isEnabled":true}`, "leastCost.settings.startup.probe.isEnabled is not supported"},
		{"null", `null`, "leastCost.settings.startup.probe must not be null"},
		{"not an object", `true`, "leastCost.settings.startup.probe must be an object"},
	} {
		err := loadConfig(leastCostConfig(`,"healthObservatory":{}`, `{"startup":{"probe":`+tc.probe+`}}`))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
}
