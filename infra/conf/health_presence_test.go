package conf_test

import (
	"fmt"
	"strings"
	"testing"
)

func TestHealthOptionalBlocksEnableByPresence(t *testing.T) {
	for _, raw := range []string{`{"telemetry":{"quality":{}}}`, `{"resources":{"tunnelSleep":{}}}`, `{"host":{"userPresent":{}}}`, `{"probes":{"requests":{}}}`, `{"evidence":{"partialStalls":{}}}`} {
		cfg, err := buildHealth(t, raw)
		if err != nil {
			t.Fatal(err)
		}
		f := cfg.Features
		if !(f.GetQuality() || f.GetTunnelSleep() || f.GetUserPresent() || f.GetProbeRequests() || f.GetPartialStalls()) {
			t.Fatalf("present block was not enabled: %s", raw)
		}
		for _, flag := range []bool{true, false} {
			bad := strings.Replace(raw, `{}`, fmt.Sprintf(`{"isEnabled":%t}`, flag), 1)
			if _, err := buildHealth(t, bad); err == nil || !strings.Contains(err.Error(), "isEnabled") {
				t.Fatalf("optional enable flag accepted: %s: %v", bad, err)
			}
		}
	}
}

func TestHealthDefaultOnOnlyAllowsDisabling(t *testing.T) {
	for _, detector := range []string{"dialFailures", "dialTimeout", "serverUnverified", "firstReply", "earlyClose", "stalledRequests"} {
		if _, err := buildHealth(t, fmt.Sprintf(`{"evidence":{"%s":{"isEnabled":true}}}`, detector)); err == nil || !strings.Contains(err.Error(), ".isEnabled") {
			t.Fatalf("true accepted for %s: %v", detector, err)
		}
		if _, err := buildHealth(t, fmt.Sprintf(`{"evidence":{"%s":{"isEnabled":false}}}`, detector)); err != nil {
			t.Fatal(err)
		}
	}
	for _, block := range []string{"closePending", "closeSilent"} {
		if err := loadConfig(leastCostConfig(`,"healthObservatory":{}`, fmt.Sprintf(`{"failover":{"%s":{"isEnabled":true}}}`, block))); err == nil {
			t.Fatalf("true accepted for failover.%s", block)
		}
		if err := loadConfig(leastCostConfig(`,"healthObservatory":{}`, fmt.Sprintf(`{"failover":{"%s":{"isEnabled":false}}}`, block))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHealthURLListsAndRequiredProbes(t *testing.T) {
	for _, key := range []string{"connectivity", "labelRequest"} {
		for _, destinations := range []string{`[]`, `[""]`, `["ftp://x.test"]`, `["https://a.test","https://b.test","https://c.test","https://d.test","https://e.test"]`} {
			labels := ""
			if key == "labelRequest" {
				labels = `,"labels":{"asn":"X-Asn"}`
			}
			raw := fmt.Sprintf(`{"network":{"%s":{"outbound":"direct","destinations":%s%s}}}`, key, destinations, labels)
			if _, err := buildHealth(t, raw); err == nil || !strings.Contains(err.Error(), ".destinations") {
				t.Fatalf("invalid destinations accepted: %s: %v", raw, err)
			}
		}
		if _, err := buildHealth(t, fmt.Sprintf(`{"network":{"%s":{"outbound":"direct","destination":"https://x.test"}}}`, key)); err == nil || !strings.Contains(err.Error(), ".destination") {
			t.Fatal("single destination alias accepted", err)
		}
	}
	for _, urls := range []string{`[]`, `[""]`, `["https://a.test","https://b.test","https://c.test","https://d.test","https://e.test"]`} {
		if _, err := buildHealthProvider(t, `{"providers":[{"urls":`+urls+`}]}`); err == nil || !strings.Contains(err.Error(), ".urls") {
			t.Fatal("invalid provider URLs accepted", urls, err)
		}
	}
	for _, flag := range []bool{true, false} {
		if _, err := buildHealthProvider(t, fmt.Sprintf(`{"providers":[{"isEnabled":%t}]}`, flag)); err == nil || !strings.Contains(err.Error(), "providers[0].isEnabled") {
			t.Fatal("provider enable flag accepted", err)
		}
	}
	raw := strings.Replace(leastCostConfig(`,"healthObservatory":{}`, `{}`), `,"probe":"test"`, "", 1)
	if err := loadConfig(raw); err == nil || !strings.Contains(err.Error(), "selection.probe is required") {
		t.Fatal("implicit probe accepted", err)
	}
	if err := loadConfig(leastCostConfig(`,"healthObservatory":{}`, `{"groups":[{"name":"g","cost":1}]}`)); err == nil || !strings.Contains(err.Error(), ".probe is required") {
		t.Fatal("implicit group probe accepted", err)
	}
}

func TestSelectionReturnPresenceAndInheritedDisable(t *testing.T) {
	for _, raw := range []string{`{}`, `{"interval":"1m"}`, `{"isEnabled":false}`} {
		cfg := leastCostConfig(`,"healthObservatory":{}`, `{"returnToCheaper":{}}`)
		cfg = strings.Replace(cfg, `"cost":1,`, `"cost":1,"returnToCheaper":`+raw+`,`, 1)
		if err := loadConfig(cfg); err != nil {
			t.Fatalf("inherited return %s: %v", raw, err)
		}
	}
}
