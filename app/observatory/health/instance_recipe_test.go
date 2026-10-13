package health_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
)

// These existing integration scenarios use short clocks and carefully chosen
// evidence thresholds. Only this test fixture translates their recipes; the
// JSON presented to the production loader always uses the v2 schema.
func wireGuardScenarioV2(t *testing.T, passive, probe, more, strategy string) (string, string) {
	t.Helper()
	var evidenceRecipe map[string]any
	if err := json.Unmarshal([]byte(passive), &evidenceRecipe); err != nil {
		t.Fatal(err)
	}
	evidence := map[string]any{"partialStalls": map[string]any{"isEnabled": true}}
	for old, name := range map[string]string{"dialTimeout": "dialTimeout", "firstByteTimeout": "firstReply", "stallTimeout": "stalledRequests", "udpReplyTimeout": "udpReplies"} {
		if v, ok := evidenceRecipe[old]; ok {
			evidence[name] = map[string]any{"isEnabled": true, "timeout": v}
		}
	}
	var step map[string]any
	if err := json.Unmarshal([]byte(probe), &step); err != nil {
		t.Fatal(err)
	}
	destination := step["destination"]
	delete(step, "destination")
	step["name"] = "wg"
	step["destinations"] = []any{destination}
	settings := map[string]any{"failover": map[string]any{"downAudit": map[string]any{"isEnabled": true}, "closeSilent": map[string]any{"isEnabled": true}}}
	failover := settings["failover"].(map[string]any)
	if v, ok := evidenceRecipe["stallTimeout"]; ok {
		failover["closeSilent"].(map[string]any)["after"] = v
	}
	if v, ok := evidenceRecipe["shouldKeepPendingOnDown"].(bool); ok {
		failover["closePending"] = map[string]any{"isEnabled": !v}
	}
	if v, ok := evidenceRecipe["shouldKeepSilentOnDown"].(bool); ok {
		failover["closeSilent"].(map[string]any)["isEnabled"] = !v
	}
	observer := map[string]any{
		"evidence":  evidence,
		"probes":    map[string]any{"requests": map[string]any{"isEnabled": true}, "profiles": []any{map[string]any{"name": "wg", "steps": []any{step}}}},
		"host":      map[string]any{"pinning": map[string]any{"isEnabled": true}},
		"resources": map[string]any{"tunnelSleep": map[string]any{"isEnabled": true}},
		"telemetry": map[string]any{"events": map[string]any{"isEnabled": true}},
	}
	if more != "" {
		var recipe struct {
			Sleep struct {
				AfterLastUse   string `json:"afterLastUse"`
				CloseIdleAfter string `json:"closeIdleAfter"`
			} `json:"sleep"`
		}
		if err := json.Unmarshal([]byte("{"+more+"}"), &recipe); err != nil {
			t.Fatal(err)
		}
		resources := observer["resources"].(map[string]any)
		if recipe.Sleep.AfterLastUse != "" {
			resources["tunnelSleep"].(map[string]any)["afterLastUse"] = recipe.Sleep.AfterLastUse
		}
		if recipe.Sleep.CloseIdleAfter != "" {
			resources["closeUnchosenIdle"] = map[string]any{"isEnabled": true, "after": recipe.Sleep.CloseIdleAfter}
		}
	}
	if strategy != "" {
		var recipe struct {
			MinHoldTime string `json:"minHoldTime"`
			Return      struct {
				Interval string `json:"interval"`
			} `json:"returnToCheaper"`
		}
		if err := json.Unmarshal([]byte("{"+strategy+"}"), &recipe); err != nil {
			t.Fatal(err)
		}
		settings["returnToCheaper"] = map[string]any{"isEnabled": true, "interval": recipe.Return.Interval, "minHoldTime": recipe.MinHoldTime}
	}
	rawObserver, err := json.Marshal(observer)
	if err != nil {
		t.Fatal(err)
	}
	rawSettings, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	return string(rawObserver), string(rawSettings)
}

// Fast cadence and detector thresholds are test mechanics, not configuration
// knobs. Apply them to the typed config before the actual core constructor.
func tuneWireGuardScenario(t *testing.T, cfg *core.Config, passive, strategy string) {
	t.Helper()
	var recipe struct {
		SweepInterval        string `json:"sweepInterval"`
		FailuresToSuspect    int32  `json:"failuresToSuspect"`
		MinDistinctTargets   int32  `json:"minDistinctTargets"`
		SuccessBytes         int64  `json:"successBytes"`
		PartialStallMinBytes int64  `json:"partialStallMinBytes"`
	}
	if err := json.Unmarshal([]byte(passive), &recipe); err != nil {
		t.Fatal(err)
	}
	for i, app := range cfg.App {
		value, err := app.GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		switch c := value.(type) {
		case *health.Config:
			if recipe.SweepInterval != "" {
				d, err := time.ParseDuration(recipe.SweepInterval)
				if err != nil {
					t.Fatal(err)
				}
				c.Passive.SweepInterval = int64(d)
			}
			c.Passive.FailuresToSuspect = recipe.FailuresToSuspect
			c.Passive.MinDistinctTargets = recipe.MinDistinctTargets
			c.Passive.SuccessBytes = recipe.SuccessBytes
			c.Passive.PartialStallMinBytes = recipe.PartialStallMinBytes
			c.Schedule.MinProbeInterval = int64(10 * time.Millisecond)
			c.Schedule.SuspectCooldown = int64(time.Second)
			c.Schedule.HostProbes.ReuseWindow = int64(time.Millisecond)
			cfg.App[i] = serial.ToTypedMessage(c)
		case *router.Config:
			if strategy == "" {
				continue
			}
			var recipe struct {
				Return struct {
					HasRandomPhase *bool `json:"hasRandomPhase"`
					ProbesToReturn int32 `json:"probesToReturn"`
				} `json:"returnToCheaper"`
			}
			if err := json.Unmarshal([]byte("{"+strategy+"}"), &recipe); err != nil {
				t.Fatal(err)
			}
			for _, balancer := range c.BalancingRule {
				raw, err := balancer.StrategySettings.GetInstance()
				if err != nil {
					t.Fatal(err)
				}
				selector, ok := raw.(*health.SelectorConfig)
				if !ok {
					continue
				}
				selector.ReturnToCheaper.ProbesToReturn = recipe.Return.ProbesToReturn
				selector.ReturnToCheaper.SkipRandomPhase = recipe.Return.HasRandomPhase != nil && !*recipe.Return.HasRandomPhase
				balancer.StrategySettings = serial.ToTypedMessage(selector)
			}
			cfg.App[i] = serial.ToTypedMessage(c)
		}
	}
}
func tuneInstanceProbeSpacing(t *testing.T, cfg *core.Config) {
	t.Helper()
	for i, app := range cfg.App {
		value, err := app.GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		if c, ok := value.(*health.Config); ok {
			if c.Schedule == nil {
				c.Schedule = &health.ScheduleConfig{}
			}
			c.Schedule.MinProbeInterval = int64(10 * time.Millisecond)
			cfg.App[i] = serial.ToTypedMessage(c)
		}
	}
}
