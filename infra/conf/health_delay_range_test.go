package conf_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/infra/conf"
)

func TestProviderDelayRangeGrammar(t *testing.T) {
	for _, value := range []string{"0s", "30s", "20s..60s", "500ms..1h30m", "0s..0s", "1ns..2ns"} {
		var d conf.HealthDelayRange
		raw, _ := json.Marshal(value)
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatalf("%s: %v", value, err)
		}
	}
	for _, value := range []string{`"-1s"`, `"2s..1s"`, `"0s..-1s"`, `"1s..2s..3s"`, `"1s.."`, `"..2s"`, `"wat"`, `30`, `{}`, `null`} {
		var d conf.HealthDelayRange
		if err := json.Unmarshal([]byte(value), &d); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}

func TestProviderRoadsAndDelayConfig(t *testing.T) {
	// Decode the actual public loader, including its strict nested shape checks.
	raw := `{"healthObservatory":{"network":{"memory":{}},"providers":[{"tag":"p","urls":["https://backend.test/set"],"triggers":{"exhausted":{"delay":"0s..10s"},"minHealthy":{"outbounds":2,"delay":"2s..3s"},"periodic":{"interval":"20m..40m"},"networkChange":{"settleDelay":"30s..90s"},"whenCostAbove":{"cost":10,"after":"15m..25m"}},"apply":{"balancers":["auto","backend"],"tagPrefix":"p-"}}]}}`
	var c conf.Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	built, err := c.HealthObservatory.Build()
	if err != nil {
		t.Fatal(err)
	}
	p := built.(*health.Config).Providers[0]
	if len(p.Apply.Balancers) != 2 || p.Triggers.ExhaustedDelay.Max != int64(10*time.Second) || p.Triggers.GetIntervalMax() != int64(40*time.Minute) || p.Triggers.GetNetworkSettleMax() != int64(90*time.Second) || p.Triggers.WhenCostAbove.GetAfterMax() != int64(25*time.Minute) {
		t.Fatal(p)
	}
	for _, bad := range []string{
		strings.Replace(raw, `"balancers":["auto","backend"]`, `"balancer":"auto"`, 1),
		strings.Replace(raw, `"balancers":["auto","backend"]`, `"balancers":["auto","auto"]`, 1),
		strings.Replace(raw, `"balancers":["auto","backend"]`, `"balancers":[]`, 1),
		strings.Replace(raw, `"balancers":["auto","backend"]`, `"balancers":["a","b","c","d","e"]`, 1),
		strings.Replace(raw, `"periodic":{"interval":"20m..40m"}`, `"periodic":{"interval":"20m","jitterFraction":0.2}`, 1),
		strings.Replace(raw, `"0s..10s"`, `"10s..0s"`, 1),
	} {
		var c conf.Config
		err := json.Unmarshal([]byte(bad), &c)
		if err == nil {
			_, err = c.HealthObservatory.Build()
		}
		if err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}
