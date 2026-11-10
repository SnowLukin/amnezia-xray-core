package health_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
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
	evidence := map[string]any{"partialStalls": map[string]any{}}
	for old, name := range map[string]string{"dialTimeout": "dialTimeout", "firstByteTimeout": "firstReply", "stallTimeout": "stalledRequests", "udpReplyTimeout": "udpReplies"} {
		if v, ok := evidenceRecipe[old]; ok {
			evidence[name] = map[string]any{"timeout": v}
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
	settings := map[string]any{"failover": map[string]any{"downAudit": map[string]any{}, "closeSilent": map[string]any{}}}
	failover := settings["failover"].(map[string]any)
	if v, ok := evidenceRecipe["stallTimeout"]; ok {
		failover["closeSilent"].(map[string]any)["after"] = v
	}
	if v, ok := evidenceRecipe["shouldKeepPendingOnDown"].(bool); ok && v {
		failover["closePending"] = map[string]any{"isEnabled": false}
	}
	if v, ok := evidenceRecipe["shouldKeepSilentOnDown"].(bool); ok && v {
		failover["closeSilent"].(map[string]any)["isEnabled"] = false
	}
	observer := map[string]any{
		"evidence":  evidence,
		"probes":    map[string]any{"requests": map[string]any{}, "profiles": []any{map[string]any{"name": "wg", "steps": []any{step}}}},
		"host":      map[string]any{"pinning": map[string]any{}},
		"resources": map[string]any{"tunnelSleep": map[string]any{}},
		"telemetry": map[string]any{"events": map[string]any{}},
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
			resources["closeUnchosenIdle"] = map[string]any{"after": recipe.Sleep.CloseIdleAfter}
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
		settings["returnToCheaper"] = map[string]any{"interval": recipe.Return.Interval, "minHoldTime": recipe.MinHoldTime}
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

// wireGuardKnobs reads the fast cadence and detector thresholds of a
// scenario recipe: test mechanics, not configuration.
func wireGuardKnobs(t *testing.T, passive, strategy string) health.InstanceKnobs {
	t.Helper()
	var recipe struct {
		SweepInterval        string `json:"sweepInterval"`
		FailuresToSuspect    int    `json:"failuresToSuspect"`
		MinDistinctTargets   int    `json:"minDistinctTargets"`
		SuccessBytes         int64  `json:"successBytes"`
		PartialStallMinBytes int64  `json:"partialStallMinBytes"`
	}
	if err := json.Unmarshal([]byte(passive), &recipe); err != nil {
		t.Fatal(err)
	}
	k := health.InstanceKnobs{Failures: recipe.FailuresToSuspect, MinDistinctTargets: recipe.MinDistinctTargets,
		SuccessBytes: recipe.SuccessBytes, PartialMinBytes: recipe.PartialStallMinBytes,
		ProbeSpacing: 10 * time.Millisecond, SuspectCooldown: time.Second, HostReuse: time.Millisecond}
	if recipe.SweepInterval != "" {
		d, err := time.ParseDuration(recipe.SweepInterval)
		if err != nil {
			t.Fatal(err)
		}
		k.Sweep = d
	}
	if strategy != "" {
		var recipe struct {
			Return struct {
				HasRandomPhase *bool `json:"hasRandomPhase"`
				ProbesToReturn int   `json:"probesToReturn"`
			} `json:"returnToCheaper"`
		}
		if err := json.Unmarshal([]byte("{"+strategy+"}"), &recipe); err != nil {
			t.Fatal(err)
		}
		k.ReturnSuccesses = recipe.Return.ProbesToReturn
		k.FixedReturnPhase = recipe.Return.HasRandomPhase != nil && !*recipe.Return.HasRandomPhase
	}
	return k
}
