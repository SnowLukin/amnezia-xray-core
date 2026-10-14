package conf_test

import (
	"encoding/json"
	"fmt"
	"github.com/xtls/xray-core/app/observatory/health"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	. "github.com/xtls/xray-core/infra/conf"
	"google.golang.org/protobuf/proto"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

func buildHealth(t *testing.T, raw string) (*health.Config, error) {
	t.Helper()
	var c HealthObservatoryConfig
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, err
	}
	m, err := c.Build()
	if err != nil {
		return nil, err
	}
	return m.(*health.Config), nil
}
func leastCostConfig(observer, settings string, selector ...string) string {
	observer = withTestProbeFragment(observer)
	sel := ""
	if len(selector) > 0 {
		raw, _ := json.Marshal(selector)
		sel = `,"selector":` + string(raw)
	}
	return `{"outbounds":[{"tag":"a1","protocol":"freedom","selection":{"balancer":"auto","cost":1,"probe":"test"}},{"tag":"b1","protocol":"freedom","selection":{"balancer":"auto","cost":3,"probe":"test"}},{"tag":"block","protocol":"blackhole"}],"routing":{"balancers":[{"tag":"auto","fallbackTag":"block"` + sel + `,"strategy":{"type":"leastCost","settings":` + settings + `}}]}` + observer + `}`
}
func withTestProbeFragment(raw string) string {
	if raw == "" {
		return raw
	}
	var fields map[string]any
	if json.Unmarshal([]byte("{"+strings.TrimPrefix(raw, ",")+"}"), &fields) != nil {
		return raw
	}
	if h, ok := fields["healthObservatory"].(map[string]any); ok {
		ensureTestProbe(h)
	}
	encoded, _ := json.Marshal(fields)
	return "," + string(encoded[1:len(encoded)-1])
}
func ensureTestProbe(h map[string]any) {
	p, _ := h["probes"].(map[string]any)
	if p == nil {
		p = map[string]any{}
		h["probes"] = p
	}
	profiles, _ := p["profiles"].([]any)
	for _, profile := range profiles {
		if profile.(map[string]any)["name"] == "test" {
			return
		}
	}
	p["profiles"] = append(profiles, map[string]any{"name": "test", "steps": []any{map[string]any{"name": "test", "destinations": []any{"https://example.test/204"}}}})
}
func buildHealthProvider(t *testing.T, raw string) (*health.Config, error) {
	t.Helper()
	var h map[string]any
	if err := json.Unmarshal([]byte(raw), &h); err != nil {
		return nil, err
	}
	for _, item := range h["providers"].([]any) {
		p := item.(map[string]any)
		if p["tag"] == nil {
			p["tag"] = "fresh"
		}
		if p["urls"] == nil {
			p["urls"] = []string{"https://x.test/set"}
		}
		a, _ := p["apply"].(map[string]any)
		if a == nil {
			a = map[string]any{}
			p["apply"] = a
		}
		if a["balancers"] == nil {
			a["balancers"] = []string{"auto"}
		}
		if a["tagPrefix"] == nil {
			a["tagPrefix"] = "p-"
		}
	}
	encoded, _ := json.Marshal(h)
	return buildHealth(t, string(encoded))
}
func loadConfig(raw string) error {
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return err
	}
	_, err := c.Build()
	return err
}
func TestHealthObservatoryV2Shape(t *testing.T) {
	c, err := buildHealth(t, `{"evidence":{"dialTimeout":{"isEnabled":false,"timeout":"6s"},"firstReply":{"timeout":"5s"}},"network":{"connectivity":{"outbound":"direct","timeout":"4s","destinations":["https://example.test/204"]},"witnesses":{"outbounds":["direct"]}},"probes":{"limits":{"hostPerMinute":2},"requests":{"maxOutbounds":4,"timeout":"1m"}},"resources":{"closeUnchosenIdle":{"after":"15s"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	if c.Features.DialTimeout == nil || *c.Features.DialTimeout || !c.Features.GetConnectivity() || !c.Features.GetWitnesses() || !c.Features.GetProbeRequests() || !c.Features.GetCloseUnchosenIdle() {
		t.Fatalf("flags %v", c.Features)
	}
	if c.Passive.DialTimeout != int64(6*time.Second) || c.Connectivity.Timeout != int64(4*time.Second) || c.Schedule.HostProbes.Timeout != int64(time.Minute) || c.Sleep.CloseIdleAfter != int64(15*time.Second) {
		t.Fatalf("duration bridge %v", c)
	}
	if c.Features.TunnelSleep != nil {
		t.Fatal("idle closure implicitly enabled tunnel sleep")
	}
}
func TestHealthProbeStepsJSON(t *testing.T) {
	c, err := buildHealth(t, `{"probes":{"stepProfiles":{},"profiles":[{"name":"full","steps":[{"name":"transport","destinations":["http://10.12.0.1/check"],"minBytes":32768},{"name":"egress","isOptional":true,"destinations":["https://one.test/204","https://two.test/204"],"method":"HEAD","expectedStatuses":[204],"timeout":"3s","progressTimeout":"1s"}]},{"name":"light","steps":[{"name":"light","destinations":["https://example.test/check"],"minBytes":8}]}]}}`)
	if err != nil {
		t.Fatal(err)
	}
	steps := c.Probes[0].Steps
	if len(c.Probes) != 2 || len(steps) != 2 || len(c.Probes[1].Steps) != 1 || steps[0].MinBytes != 32768 || steps[1].Method != "HEAD" || !steps[1].IsOptional || steps[1].Timeout != int64(3*time.Second) || steps[1].ProgressTimeout != int64(time.Second) {
		t.Fatalf("steps %v", c.Probes)
	}
}
func TestHealthProbeStepsJSONRejects(t *testing.T) {
	for name, profile := range map[string]string{
		"empty steps": `{"name":"x","steps":[]}`, "null steps": `{"name":"x","steps":null}`, "five steps": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"]},{"name":"b","destinations":["https://x.test"]},{"name":"c","destinations":["https://x.test"]},{"name":"d","destinations":["https://x.test"]},{"name":"e","destinations":["https://x.test"]}]}`,
		"missing name": `{"name":"x","steps":[{"destinations":["https://x.test"]}]}`, "duplicate name": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"]},{"name":"a","destinations":["https://x.test"]}]}`,
		"no destinations": `{"name":"x","steps":[{"name":"a"}]}`, "five destinations": `{"name":"x","steps":[{"name":"a","destinations":["https://a.test","https://b.test","https://c.test","https://d.test","https://e.test"]}]}`,
		"empty destination": `{"name":"x","steps":[{"name":"a","destinations":[""]}]}`, "bad destination": `{"name":"x","steps":[{"name":"a","destinations":["ftp://x.test"]}]}`,
		"first optional": `{"name":"x","steps":[{"name":"a","isOptional":true,"destinations":["https://x.test"]}]}`, "mandatory after optional": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"]},{"name":"b","isOptional":true,"destinations":["https://x.test"]},{"name":"c","destinations":["https://x.test"]}]}`,
		"bad method": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"method":"POST"}]}`, "HEAD body": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"method":"HEAD","minBytes":1}]}`,
		"zero timeout": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"timeout":"0s"}]}`, "zero progress": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"progressTimeout":"0s"}]}`,
		"negative bytes": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"minBytes":-1}]}`, "invalid status": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"expectedStatuses":[999]}]}`,
		"profile destination": `{"name":"x","destination":"https://x.test","steps":[{"name":"a","destinations":["https://x.test"]}]}`, "profile method": `{"name":"x","method":"GET","steps":[{"name":"a","destinations":["https://x.test"]}]}`, "profile timeout": `{"name":"x","timeout":"1s","steps":[{"name":"a","destinations":["https://x.test"]}]}`, "wrong case": `{"name":"x","STEPS":[{"name":"a","destinations":["https://x.test"]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := buildHealth(t, `{"probes":{"stepProfiles":{},"profiles":[`+profile+`]}}`)
			if err == nil || !strings.Contains(err.Error(), "healthObservatory.probes.profiles") {
				t.Fatalf("accepted invalid profile or wrong path: %v", err)
			}
		})
	}
}
func TestHealthV2UnknownKeysAndNulls(t *testing.T) {
	for _, raw := range []string{`{"passive":{}}`, `{"schedule":{}}`, `{"wake":{}}`, `{"sleep":{}}`, `{"report":{}}`, `{"events":{}}`, `{"udpProbe":{}}`, `{"connectivity":{}}`, `{"subjectSelector":[]}`, `{"probes":[]}`, `{"evidence":{"dialTimeout":{"window":"1s"}}}`, `{"network":{"shouldRememberNetworks":true}}`, `{"telemetry":{"quality":{"unknown":false}}}`, `{"probes":{"profiles":[{"name":"p","destination":"https://x.test"}]}}`, `{"providers":[{"maxBytes":1}]}`, `{"providers":[{"apply":{"isRoutingAccepted":false}}]}`, `{"providers":[{"triggers":{"interval":"1m"}}]}`, `{"providers":[{"limits":{"authRetryDelay":"1h"}}]}`, `{"providers":[{"limits":{"backoff":{}}}]}`, `{"probes":null}`, `{"evidence":{"dialFailures":{"isEnabled":null}}}`, `{"host":{"userPresent":{"freshness":null}}}`, `{"network":{"memory":{"maxNetworks":null}}}`} {
		if _, err := buildHealth(t, raw); err == nil || !strings.Contains(err.Error(), "healthObservatory") {
			t.Errorf("accepted %s, err %v", raw, err)
		}
	}
}
func TestHealthV2PreparedBlocksValidateSettings(t *testing.T) {
	for field, raw := range map[string]string{
		"dialTimeout.timeout": `{"evidence":{"dialTimeout":{"isEnabled":false,"timeout":"0s"}}}`,
		"firstReply.timeout":  `{"evidence":{"firstReply":{"timeout":"0s"}}}`, "stalledRequests.timeout": `{"evidence":{"stalledRequests":{"timeout":"-1s"}}}`,
		"loneFailureProbe.after": `{"evidence":{"loneFailureProbe":{"after":"0s"}}}`, "partialStalls.maxAge": `{"evidence":{"partialStalls":{"maxAge":"0s"}}}`,
		"connectivity.timeout": `{"network":{"connectivity":{"timeout":"0s"}}}`, "requests.timeout": `{"probes":{"requests":{"timeout":"0s"}}}`,
		"candidates.keepVerified": `{"probes":{"candidates":{"keepVerified":0}}}`, "hostPerMinute": `{"probes":{"limits":{"perMinute":4,"hostPerMinute":3}}}`,
		"events.maxBytes": `{"telemetry":{"events":{"maxBytes":0}}}`, "report.interval": `{"telemetry":{"report":{"interval":"0s"}}}`,
		"tunnelSleep.afterLastUse": `{"resources":{"tunnelSleep":{"afterLastUse":"5s"}}}`, "closeUnchosenIdle.after": `{"resources":{"closeUnchosenIdle":{"after":"11m"}}}`,
	} {
		_, err := buildHealth(t, raw)
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("%s: %v must name %s", raw, err, field)
		}
	}
	for _, raw := range []string{`{"resources":{"tunnelSleep":{"afterLastUse":"10s"}}}`, `{"resources":{"closeUnchosenIdle":{"after":"10s"}}}`, `{"resources":{"closeUnchosenIdle":{"after":"10m"}}}`, `{"probes":{"candidates":{"keepVerified":4,"recheckInterval":"1.000000001s"}}}`} {
		if _, err := buildHealth(t, raw); err != nil {
			t.Fatal(err)
		}
	}
}
func TestHealthV2Dependencies(t *testing.T) {
	for field, raw := range map[string]string{
		"connectivity": `{"network":{"connectivity":{}}}`, "witnesses": `{"network":{"witnesses":{}}}`, "labelRequest": `{"network":{"labelRequest":{}}}`, "probes.udp": `{"probes":{"udp":{}}}`,
		"stepProfiles":     `{"probes":{"profiles":[{"name":"p","steps":[{"name":"a","destinations":["https://x.test"]},{"name":"b","destinations":["https://x.test"]}]}]}}`,
		"loneFailureProbe": `{"evidence":{"dialFailures":{"isEnabled":false},"dialTimeout":{"isEnabled":false},"serverUnverified":{"isEnabled":false},"loneFailureProbe":{}}}`,
	} {
		_, err := buildHealth(t, raw)
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Fatalf("%s: %v must name %s", raw, err, field)
		}
	}
	for field, settings := range map[string]string{"networkChanges": `{"returnToCheaper":{"onNetworkChange":{}}}`, "confirmWithProbe:auto": `{"failover":{"downAudit":{},"confirmWithProbe":"never"}}`, "host.pinning": `{"startup":{"pin":{"outbound":"a1"}}}`} {
		if err := loadConfig(leastCostConfig(`,"healthObservatory":{}`, settings)); err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("%s: %v", field, err)
		}
	}
	if err := loadConfig(leastCostConfig(`,"healthObservatory":{"telemetry":{"metrics":{}}}`, `{}`)); err == nil || !strings.Contains(err.Error(), "metrics exporter") {
		t.Fatal(err)
	}
}
func TestHealthV2FeatureLoadMatrix(t *testing.T) {
	features := []string{
		`"evidence":{"partialStalls":{}}`, `"evidence":{"udpReplies":{}}`, `"evidence":{"loneFailureProbe":{}}`,
		`"probes":{"stepProfiles":{}}`, `"probes":{"candidates":{}}`, `"probes":{"requests":{}}`, `"probes":{"udp":{"destinations":["stun.test:3478"]}}`,
		`"network":{"connectivity":{"outbound":"a1","destinations":["https://x.test/204"]}}`, `"network":{"witnesses":{"outbounds":["a1"]}}`, `"network":{"memory":{}}`, `"network":{"hostLabels":{}}`, `"network":{"labelRequest":{"outbound":"a1","labels":{"asn":"X-Asn"},"destinations":["https://x.test/labels"]}}`,
		`"host":{"pinning":{}}`, `"host":{"userPresent":{}}`, `"host":{"memoryPressure":{}}`, `"resources":{"tunnelSleep":{}}`, `"resources":{"closeUnchosenIdle":{}}`,
		`"telemetry":{"events":{}}`, `"telemetry":{"report":{}}`, `"telemetry":{"quality":{}}`, `"telemetry":{"pathLabels":{}}`, `"telemetry":{"probeMeasurements":{}}`,
		`"providers":[{"tag":"p","apply":{"balancers":["auto"],"tagPrefix":"p-"},"urls":["https://x.test/set"]}]`,
	}
	bodies := []map[string]any{}
	for _, feature := range features {
		var body map[string]any
		if err := json.Unmarshal([]byte(`{`+feature+`}`), &body); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
		if err := loadConfig(leastCostConfig(`,"healthObservatory":{`+feature+`}`, `{}`)); err != nil {
			t.Fatalf("single %s: %v", feature, err)
		}
	}
	var merge func(map[string]any, map[string]any)
	merge = func(dst, src map[string]any) {
		for k, v := range src {
			if child, ok := v.(map[string]any); ok {
				next, _ := dst[k].(map[string]any)
				if next == nil {
					next = map[string]any{}
					dst[k] = next
				}
				merge(next, child)
			} else {
				dst[k] = v
			}
		}
	}
	rng := rand.New(rand.NewSource(20261013))
	for i := 0; i < 32; i++ {
		all := map[string]any{}
		for _, body := range bodies {
			if i == 0 || rng.Intn(2) == 0 {
				merge(all, body)
			}
		}
		raw, _ := json.Marshal(all)
		if err := loadConfig(leastCostConfig(`,"healthObservatory":`+string(raw), `{}`)); err != nil {
			t.Fatalf("combination %d %s: %v", i, raw, err)
		}
	}
}
func TestHealthV2StockConfigStillLoads(t *testing.T) {
	for _, raw := range []string{`{}`, `{"outbounds":[{"tag":"direct","protocol":"freedom"}]}`, `{"outbounds":[{"tag":"a","protocol":"freedom","anUpstreamUnknownKey":true}],"routing":{"balancers":[{"tag":"random","selector":["a"],"strategy":{"type":"random","settings":{"unknown":true}}}]}}`} {
		if err := loadConfig(raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := loadConfig(strings.Replace(leastCostConfig(`,"healthObservatory":{}`, `{}`), `,"fallbackTag":"block"`, "", 1)); err == nil || !strings.Contains(err.Error(), "fallbackTag") {
		t.Fatal(err)
	}
	if err := loadConfig(leastCostConfig("", `{}`)); err == nil || !strings.Contains(err.Error(), "healthObservatory") {
		t.Fatal(err)
	}
	for _, settings := range []string{`{"paths":[]}`, `{"shouldSkipDownAudit":false}`, `{"shouldProbeBeforeSwitch":true}`, `{"startOutbound":"a1"}`, `{"returnToCheaper":{"hasRandomPhase":true}}`, `{"groups":{"items":[]}}`} {
		if err := loadConfig(leastCostConfig(`,"healthObservatory":{}`, settings)); err == nil {
			t.Fatalf("accepted alias %s", settings)
		}
	}
	if err := loadConfig(leastCostConfig(`,"healthObservatory":{}`, `{}`, "a1")); err == nil {
		t.Fatal("leastCost selector accepted")
	}
}
func TestHealthProvidersBuild(t *testing.T) {
	c, err := buildHealth(t, `{"providers":[{"tag":"fresh","headers":{"Authorization":"Bearer x"},"via":["auto","direct"],"timeout":"5s","triggers":{"minHealthy":{"outbounds":3},"exhausted":{"isEnabled":false},"whenCostAbove":{"cost":10,"after":"15m"},"networkChange":{"mode":"unknown"}},"limits":{"minInterval":"1m","urgentMinInterval":"30s","maxRefreshesPerHour":2},"apply":{"balancers":["auto"],"tagPrefix":"p-","drainTimeout":"1m"},"urls":["https://api.test/set"]}]}`)
	if err != nil {
		t.Fatal(err)
	}
	p := c.Providers[0]
	if !p.GetIsEnabled() || p.Features.Periodic != nil || p.Features.GetPeriodic() || !p.Features.GetMinHealthy() || p.Features.Exhausted == nil || p.Features.GetExhausted() {
		t.Fatalf("flags %v", p.Features)
	}
	if p.Triggers.Interval != 0 || p.Triggers.MinHealthyOutbounds != 3 || p.Triggers.WhenCostAbove.Cost != 10 || p.Triggers.NetworkChangeMode != health.RefreshOnNetwork_REFRESH_UNKNOWN || p.Limits.MinInterval != int64(time.Minute) || p.Limits.MaxRefreshesPerHour != 2 || p.Headers["Authorization"] != "Bearer x" {
		t.Fatalf("provider %v", p)
	}
}
func TestHealthProviderInitial(t *testing.T) {
	for _, initial := range []string{`null`, `{}`, `{"x":[1]}`, `[]`, `"{}"`, `1`, `{"outbounds":[{"protocol":"nosuch"}],"dns":{}}`, `{"v":"` + strings.Repeat("x", 1<<20) + `"}`} {
		raw := fmt.Sprintf(`{"providers":[{"tag":"a","urls":["https://x.test"],"apply":{"balancers":["auto"],"tagPrefix":"p-"},"initialAnswer":%s}]}`, initial)
		c, err := buildHealth(t, raw)
		if err != nil {
			t.Fatal(err)
		}
		want := initial
		if initial == "null" {
			want = ""
		}
		if got := string(c.Providers[0].InitialAnswer); got != want {
			t.Fatalf("initial %.20s got %.20s", initial, got)
		}
	}
}
func TestHealthProviderMethodBaseVersionAndHeaders(t *testing.T) {
	for method, want := range map[string]string{"get": "GET", "Post": "POST", "": ""} {
		c, err := buildHealthProvider(t, fmt.Sprintf(`{"providers":[{"method":%q,"baseVersion":%q,"apply":{"acceptedHeaders":[]}}]}`, method, strings.Repeat("b", 256)))
		if err != nil || c.Providers[0].Method != want || len(c.Providers[0].BaseVersion) != 256 || !c.Providers[0].Apply.FixedHeaders {
			t.Fatalf("%s: %v %v", method, c, err)
		}
	}
	for _, base := range []string{"", "cfg-12"} {
		c, err := buildHealthProvider(t, fmt.Sprintf(`{"providers":[{"baseVersion":%q}]}`, base))
		if err != nil || c.Providers[0].BaseVersion != base {
			t.Fatal(c, err)
		}
	}
	if _, err := buildHealthProvider(t, fmt.Sprintf(`{"providers":[{"baseVersion":%q}]}`, strings.Repeat("b", 257))); err == nil {
		t.Fatal("oversize baseVersion accepted")
	}
	c, err := buildHealthProvider(t, `{"providers":[{"apply":{"acceptedHeaders":["Authorization","X-Api-Key"]},"triggers":{"leaseRenewal":{"isEnabled":false,"renewAtFraction":0.6,"urgentAtFraction":0.9},"backendSchedule":{"isEnabled":false}}}]}`)
	if err != nil || c.Providers[0].Apply.FixedHeaders || c.Providers[0].Features.GetBackendSchedule() || c.Providers[0].Features.GetLeaseRenewal() {
		t.Fatal(c, err)
	}
}
func TestHealthProvidersReject(t *testing.T) {
	for field, provider := range map[string]string{
		"requires": `{"tag":"a","url":"https://x.test"}`, "credentials": `{"url":"https://user:pw@x.test"}`, "url": `{"url":"ftp://x.test"}`, "https": `{"url":"http://x.test"}`, "mode": `{"triggers":{"networkChange":{"mode":"sometimes"}}}`, "cost": `{"triggers":{"whenCostAbove":{"cost":0}}}`, "cost required": `{"triggers":{"whenCostAbove":{}}}`, "after": `{"triggers":{"whenCostAbove":{"after":"-1s"}}}`, "via": `{"via":[""]}`, "method": `{"method":"PUT"}`, "renewAtFraction": `{"triggers":{"leaseRenewal":{"renewAtFraction":0.99}}}`, "renewal zero": `{"triggers":{"leaseRenewal":{"renewAtFraction":0}}}`, "urgent": `{"triggers":{"leaseRenewal":{"renewAtFraction":0.6,"urgentAtFraction":0.5}}}`, "urgent default": `{"triggers":{"leaseRenewal":{"urgentAtFraction":0.4}}}`, "Host": `{"apply":{"acceptedHeaders":["Host"]}}`, "proxy": `{"apply":{"acceptedHeaders":["proxy-authorization"]}}`, "bad token": `{"apply":{"acceptedHeaders":["X Key"]}}`, "zero limit": `{"limits":{"minInterval":"0s"}}`, "zero quota": `{"limits":{"maxRefreshesPerHour":0}}`, "ceiling": `{"apply":{"maxOutbounds":129}}`,
	} {
		if _, err := buildHealth(t, `{"providers":[`+provider+`]}`); err == nil {
			t.Errorf("accepted %s %s", field, provider)
		}
	}
	for _, providers := range []string{`{"tag":"a","apply":{"tagPrefix":"p-"}},{"tag":"b","apply":{"tagPrefix":"p-x"}}`, `{"tag":"a"},{"tag":"a"}`} {
		if _, err := buildHealth(t, `{"providers":[`+providers+`]}`); err == nil {
			t.Fatal("duplicate/overlap accepted")
		}
	}
}

func configFeatureStatus(t *testing.T, c *core.Config) map[string]bool {
	t.Helper()
	instance, err := core.New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	observer, ok := instance.GetFeature(extension.ObservatoryType()).(*health.Observer)
	if !ok {
		t.Fatal("missing health observer")
	}
	var status struct {
		Features map[string]bool `json:"features"`
	}
	if err := json.Unmarshal(observer.Status(), &status); err != nil {
		t.Fatal(err)
	}
	return status.Features
}
func TestHealthV2JSONAndDirectProtobufDefaults(t *testing.T) {
	var c Config
	if err := json.Unmarshal([]byte(leastCostConfig(`,"healthObservatory":{}`, `{}`)), &c); err != nil {
		t.Fatal(err)
	}
	built, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	jsonFlags := configFeatureStatus(t, built)
	essential := map[string]bool{"dial_failures": true, "dial_timeout": true, "server_unverified": true, "first_reply": true, "early_close": true, "stalled_requests": true, "balancers.auto.probe_before_switch": true, "balancers.auto.close_pending": true, "balancers.auto.close_silent": true}
	// fallbackBalancer and census are enabled by config presence, outside the boolean feature DTO.
	if len(jsonFlags) != (&health.FeatureConfig{}).ProtoReflect().Descriptor().Fields().Len()+(&health.SelectorFeatureConfig{}).ProtoReflect().Descriptor().Fields().Len()+2 {
		t.Fatalf("unexpected feature count %d: %v", len(jsonFlags), jsonFlags)
	}
	for key, value := range jsonFlags {
		if value != essential[key] {
			t.Fatalf("default %s=%v", key, value)
		}
	}
	direct := proto.Clone(built).(*core.Config)
	for i, app := range direct.App {
		if app.Type == "xray.core.app.observatory.health.Config" {
			direct.App[i] = serial.ToTypedMessage(&health.Config{Paths: c.HealthObservatory.Paths})
		}
	}
	if directFlags := configFeatureStatus(t, direct); !reflect.DeepEqual(jsonFlags, directFlags) {
		t.Fatalf("JSON %v direct %v", jsonFlags, directFlags)
	}
}
func TestHealthV2PresenceSurvivesProtobuf(t *testing.T) {
	cfg, err := buildHealth(t, `{"evidence":{"dialFailures":{"isEnabled":false},"dialTimeout":{"isEnabled":false},"serverUnverified":{"isEnabled":false},"firstReply":{"isEnabled":false},"earlyClose":{"isEnabled":false},"stalledRequests":{"isEnabled":false}},"telemetry":{"quality":{}}}`)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var restored health.Config
	if err := proto.Unmarshal(wire, &restored); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(cfg, &restored) || restored.Features.DialFailures == nil || *restored.Features.DialFailures || restored.Features.Events != nil || !restored.Features.GetQuality() {
		t.Fatalf("presence lost: %v", restored.Features)
	}
}
func TestHealthProviderImmediateCostDelayPresence(t *testing.T) {
	for _, after := range []string{``, `,"after":"0s"`, `,"after":"1.000000001s"`} {
		c, err := buildHealthProvider(t, `{"providers":[{"triggers":{"whenCostAbove":{"cost":2`+after+`}}}]}`)
		if err != nil {
			t.Fatal(err)
		}
		value := c.Providers[0].Triggers.WhenCostAbove.After
		if after == "" {
			if value != nil {
				t.Fatal("missing after must retain default presence")
			}
		} else {
			want := int64(0)
			if strings.Contains(after, "1.000000001") {
				want = int64(time.Second) + 1
			}
			if value == nil || *value != want {
				t.Fatalf("%s: %v", after, value)
			}
		}
	}
}

func TestHealthV2CompleteProfileLoads(t *testing.T) {
	observer := `{"evidence":{"partialStalls":{},"udpReplies":{},"loneFailureProbe":{}},"probes":{"stepProfiles":{},"profiles":[{"name":"full","steps":[{"name":"transport","destinations":["https://x.test/bytes"],"minBytes":32768},{"name":"egress","destinations":["https://x.test/204"],"isOptional":true}]}],"candidates":{},"requests":{},"udp":{"destinations":["stun.test:3478"]}},"network":{"connectivity":{"outbound":"b1","destinations":["https://x.test/204"]},"witnesses":{"outbounds":["b1"]},"memory":{},"hostLabels":{},"labelRequest":{"outbound":"b1","labels":{"asn":"X-Asn"},"destinations":["https://x.test/labels"]}},"host":{"pinning":{},"userPresent":{},"memoryPressure":{}},"resources":{"tunnelSleep":{},"closeUnchosenIdle":{}},"telemetry":{"events":{},"report":{"quietSampleFraction":0.05,"sessionLabels":{"labels":{"cohort":"v2"}}},"quality":{},"pathLabels":{},"probeMeasurements":{},"metrics":{}},"providers":[{"tag":"p","triggers":{"periodic":{},"minHealthy":{},"whenCostAbove":{"cost":2,"after":"0s"},"maxAge":{},"networkChange":{}},"apply":{"balancers":["auto"],"tagPrefix":"p-"},"urls":["https://x.test/set"]}]}`
	settings := `{"failover":{"downAudit":{}},"groups":[{"name":"relay","cost":1,"probe":"full","probePolicy":"onDemand","returnToCheaper":{"interval":"10m"},"labels":{"location":"NL"}}],"returnToCheaper":{"onNetworkChange":{"scope":"all"}},"startup":{"pin":{"outbound":"a1","timeout":"0s"}},"networkChanges":{}}`
	raw := leastCostConfig(`,"metrics":{"tag":"metrics"},"healthObservatory":`+observer, settings)
	raw = strings.Replace(raw, `"selection":{"balancer":"auto","cost":1,"probe":"test"}`, `"selection":{"balancer":"auto","group":"relay","labels":{"node":"n1"}}`, 1)
	raw = strings.Replace(raw, `{"tag":"block","protocol":"blackhole"}`, `{"tag":"candidate","protocol":"freedom","selection":{"balancer":"auto","cost":2,"probe":"test","isProbeOnly":true}},{"tag":"block","protocol":"blackhole"}`, 1)
	if err := loadConfig(raw); err != nil {
		t.Fatal(err)
	}
}
func TestHealthV2LabelsValidateEvenWhileDisabled(t *testing.T) {
	for _, labels := range []map[string]string{{"Upper": "x"}, {strings.Repeat("a", 25): "x"}, {"node": ""}, {"node": strings.Repeat("x", 33)}, {"node": "bad value"}, {"node": "value/with/slash"}, {"node": "é"}} {
		raw, _ := json.Marshal(labels)
		if err := loadConfig(strings.Replace(leastCostConfig(`,"healthObservatory":{}`, `{}`), `"cost":1`, `"cost":1,"labels":`+string(raw), 1)); err == nil {
			t.Fatalf("invalid selection labels accepted %s", raw)
		}
		if _, err := buildHealth(t, `{"telemetry":{"report":{"sessionLabels":{"isEnabled":false,"labels":`+string(raw)+`}}}}`); err == nil {
			t.Fatalf("invalid session labels accepted %s", raw)
		}
	}
	labels := map[string]string{}
	for i := 0; i < 9; i++ {
		labels[fmt.Sprintf("k%d", i)] = "value"
	}
	raw, _ := json.Marshal(labels)
	if _, err := buildHealth(t, `{"telemetry":{"report":{"sessionLabels":{"labels":`+string(raw)+`}}}}`); err == nil {
		t.Fatal("nine session labels accepted")
	}
	if _, err := buildHealth(t, `{"network":{"labelRequest":{"labels":{"Invalid":"X-Asn"}}}}`); err == nil {
		t.Fatal("invalid IP label name accepted")
	}
	if _, err := buildHealth(t, `{"network":{"labelRequest":{"labels":{"asn":"X Bad"}}}}`); err == nil {
		t.Fatal("invalid IP label header accepted")
	}
}

func TestHealthV2ImmediateDelaysRetainPresence(t *testing.T) {
	for _, value := range []string{``, `,"settleDelay":"0s"`, `,"settleDelay":"1.000000001s"`} {
		c, err := buildHealthProvider(t, `{"providers":[{"triggers":{"networkChange":{`+strings.TrimPrefix(value, ",")+`}}}]}`)
		if err != nil {
			t.Fatal(err)
		}
		got := c.Providers[0].Triggers.NetworkSettleDelay
		if value == "" {
			if got != nil {
				t.Fatal("missing settleDelay became explicit")
			}
		} else {
			want := int64(0)
			if strings.Contains(value, "1.000000001") {
				want = int64(time.Second) + 1
			}
			if got == nil || *got != want {
				t.Fatalf("settleDelay %s=%v", value, got)
			}
		}
	}
	if _, err := buildHealthProvider(t, `{"providers":[{"triggers":{"networkChange":{"settleDelay":"-1s"}}}]}`); err == nil {
		t.Fatal("negative settle delay accepted")
	}
	if err := loadConfig(leastCostConfig(`,"healthObservatory":{}`, `{"returnToCheaper":{"minHoldTime":"0s"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := loadConfig(leastCostConfig(`,"healthObservatory":{}`, `{"returnToCheaper":{"minHoldTime":"-1s"}}`)); err == nil {
		t.Fatal("negative minimum hold accepted")
	}
}

func TestHealthV2AdaptiveRootNullAndCaseReject(t *testing.T) {
	for _, raw := range []string{`{"healthObservatory":null}`, `{"HealthObservatory":{}}`, `{"outbounds":[{"protocol":"freedom","selection":null}]}`, `{"outbounds":[{"protocol":"freedom","Selection":{"balancer":"auto","cost":1}}]}`} {
		if err := loadConfig(raw); err == nil {
			t.Fatalf("adaptive null/case accepted %s", raw)
		}
	}
}

func TestHealthV2AdaptiveMetadataBounds(t *testing.T) {
	for _, length := range []int{128, 129} {
		tag := strings.Repeat("t", length)
		raw := strings.Replace(leastCostConfig(`,"healthObservatory":{}`, `{}`), `"tag":"a1"`, `"tag":"`+tag+`"`, 1)
		if err := loadConfig(raw); (err == nil) != (length == 128) {
			t.Fatalf("selected tag%d: %v", length, err)
		}
		group := strings.Repeat("g", length)
		settings := `{"groups":[{"name":"` + group + `","cost":1,"probe":"test"}]}`
		raw = strings.Replace(leastCostConfig(`,"healthObservatory":{}`, settings), `"balancer":"auto","cost":1,"probe":"test"`, `"balancer":"auto","group":"`+group+`"`, 1)
		if err := loadConfig(raw); (err == nil) != (length == 128) {
			t.Fatalf("group%d: %v", length, err)
		}
		if _, err := buildHealthProvider(t, `{"providers":[{"tag":"`+tag+`"}]}`); (err == nil) != (length == 128) {
			t.Fatalf("provider tag%d: %v", length, err)
		}
	}
	stock := `{"outbounds":[{"tag":"` + strings.Repeat("u", 256) + `","protocol":"freedom"}]}`
	if err := loadConfig(stock); err != nil {
		t.Fatal("stock tag was constrained", err)
	}
}
