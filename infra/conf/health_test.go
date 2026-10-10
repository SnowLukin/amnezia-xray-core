package conf_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	. "github.com/xtls/xray-core/infra/conf"
	"google.golang.org/protobuf/proto"
)

func TestHealthObservatoryConfig(t *testing.T) {
	creator := func() Buildable { return new(HealthObservatoryConfig) }
	runMultiTestCase(t, []TestCase{
		{
			Input: `{

				"passive": {"dialTimeout": "6s", "failuresToSuspect": 3, "shouldKeepPendingOnDown": true, "shouldKeepSilentOnDown": true},
				"connectivity": {"outbound": "direct", "destination": "https://example.com/204",
				                 "timeout": "4s", "maxPause": "90s",
				                 "recheckBackoff": {"initialDelay": "2s", "multiplier": 2, "maxDelay": "30s"}}
			}`,
			Parser: loadJSON(creator),
			Output: &health.Config{

				Passive: &health.PassiveConfig{DialTimeout: int64(6 * time.Second), FailuresToSuspect: 3,
					ShouldKeepPendingOnDown: true, ShouldKeepSilentOnDown: true},
				Connectivity: &health.ConnectivityConfig{
					Outbound:       "direct",
					Destination:    "https://example.com/204",
					Timeout:        int64(4 * time.Second),
					MaxPause:       int64(90 * time.Second),
					RecheckBackoff: &health.Backoff{InitialDelay: int64(2 * time.Second), Multiplier: 2, MaxDelay: int64(30 * time.Second)},
				},
			},
		},
	})
}

func TestHealthProbeStepsJSON(t *testing.T) {
	cfg, err := buildHealth(t, `{"probes":[
		{"name":"full","steps":[
			{"name":"transport","destinations":["http://10.12.0.1/check"],"minBytes":32768},
			{"name":"egress","isOptional":true,"destinations":["https://one.test/204","https://two.test/204"],
			 "method":"HEAD","expectedStatuses":[204],"timeout":"3s","progressTimeout":"1s"}]},
		{"name":"light","destination":"https://legacy.test/check","minBytes":8}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Probes) != 2 || len(cfg.Probes[0].Steps) != 2 || len(cfg.Probes[1].Steps) != 0 {
		t.Fatalf("profiles %+v", cfg.Probes)
	}
	steps := cfg.Probes[0].Steps
	if steps[0].Name != "transport" || steps[0].MinBytes != 32768 || steps[0].Timeout != 0 ||
		steps[1].Name != "egress" || !steps[1].IsOptional || len(steps[1].Destinations) != 2 ||
		steps[1].Method != "HEAD" || steps[1].ExpectedStatuses[0] != 204 ||
		steps[1].Timeout != int64(3*time.Second) || steps[1].ProgressTimeout != int64(time.Second) {
		t.Fatalf("steps %+v", steps)
	}
	if cfg.Probes[1].Destination != "https://legacy.test/check" || cfg.Probes[1].MinBytes != 8 {
		t.Fatalf("legacy profile %+v", cfg.Probes[1])
	}
}

func TestHealthProbeStepsJSONRejects(t *testing.T) {
	for name, raw := range map[string]string{
		"empty steps": `{"name":"x","steps":[]}`,
		"null steps":  `{"name":"x","steps":null}`,
		"five steps": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"]},
			{"name":"b","destinations":["https://x.test"]},{"name":"c","destinations":["https://x.test"]},
			{"name":"d","destinations":["https://x.test"]},{"name":"e","destinations":["https://x.test"]}]}`,
		"no destinations": `{"name":"x","steps":[{"name":"a"}]}`,
		"five destinations": `{"name":"x","steps":[{"name":"a","destinations":["https://a.test","https://b.test",
			"https://c.test","https://d.test","https://e.test"]}]}`,
		"empty destination": `{"name":"x","steps":[{"name":"a","destinations":[""]}]}`,
		"bad destination":   `{"name":"x","steps":[{"name":"a","destinations":["ftp://x.test"]}]}`,
		"missing name":      `{"name":"x","steps":[{"destinations":["https://x.test"]}]}`,
		"duplicate name": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"]},
			{"name":"a","destinations":["https://x.test"]}]}`,
		"first optional": `{"name":"x","steps":[{"name":"a","isOptional":true,"destinations":["https://x.test"]}]}`,
		"mandatory after optional": `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"]},
			{"name":"b","isOptional":true,"destinations":["https://x.test"]},
			{"name":"c","destinations":["https://x.test"]}]}`,
		"steps and destination":       `{"name":"x","destination":"https://x.test","steps":[{"name":"a","destinations":["https://x.test"]}]}`,
		"steps and empty destination": `{"name":"x","destination":"","steps":[{"name":"a","destinations":["https://x.test"]}]}`,
		"bad method":                  `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"method":"POST"}]}`,
		"HEAD body":                   `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"method":"HEAD","minBytes":1}]}`,
		"zero timeout":                `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"timeout":"0s"}]}`,
		"zero progress timeout":       `{"name":"x","steps":[{"name":"a","destinations":["https://x.test"],"progressTimeout":"0s"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := buildHealth(t, `{"probes":[`+raw+`]}`)
			if err == nil || !strings.Contains(err.Error(), "healthObservatory") {
				t.Fatalf("error %v, want healthObservatory rejection", err)
			}
		})
	}
}

func TestHealthProbeStepsRejectProfileZeroTimeouts(t *testing.T) {
	for field, value := range map[string]string{
		"timeout":         `"timeout":"0s"`,
		"progressTimeout": `"progressTimeout":"0s"`,
	} {
		raw := `{"probes":[{"name":"full",` + value + `,"steps":[{"name":"transport","destinations":["https://x.test/204"]}]}]}`
		_, err := buildHealth(t, raw)
		if err == nil || !strings.Contains(err.Error(), "probes[]."+field) {
			t.Errorf("%s: error %v must name probes[].%s", raw, err, field)
		}
	}
}

func TestHealthProbeDurationErrorBeforeMissingName(t *testing.T) {
	for _, tc := range []struct {
		raw, field string
	}{
		{`{"probes":[{"timeout":"0s"}]}`, "probes[].timeout"},
		{`{"probes":[{"progressTimeout":"0s"}]}`, "probes[].progressTimeout"},
		{`{"probes":[{"timeout":"0s","steps":[{"name":"transport","destinations":["https://x.test/204"]}]}]}`, "probes[].timeout"},
		{`{"probes":[{"progressTimeout":"0s","steps":[{"name":"transport","destinations":["https://x.test/204"]}]}]}`, "probes[].progressTimeout"},
	} {
		_, err := buildHealth(t, tc.raw)
		if err == nil || !strings.Contains(err.Error(), tc.field) {
			t.Errorf("%s: error %v must name %s", tc.raw, err, tc.field)
		}
	}
}

func TestHealthProbeStepsJSONPresence(t *testing.T) {
	step := `{"name":"transport","destinations":["https://x.test/204"]}`
	for name, tc := range map[string]struct {
		profile string
		field   string
	}{
		"capitalized null steps":   {`"Steps":null`, "steps"},
		"mixed case null steps":    {`"sTePs":null`, "steps"},
		"capitalized destination":  {`"steps":[` + step + `],"Destination":null`, "destination"},
		"empty profile method":     {`"steps":[` + step + `],"method":""`, "method"},
		"empty statuses":           {`"steps":[` + step + `],"expectedStatuses":[]`, "expectedStatuses"},
		"zero min bytes":           {`"steps":[` + step + `],"minBytes":0`, "minBytes"},
		"profile timeout":          {`"steps":[` + step + `],"timeout":"1s"`, "timeout"},
		"profile progress timeout": {`"steps":[` + step + `],"progressTimeout":"1s"`, "progressTimeout"},
		"capitalized method":       {`"steps":[` + step + `],"METHOD":""`, "method"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := buildHealth(t, `{"probes":[{"name":"full",`+tc.profile+`}]}`)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.field)) {
				t.Fatalf("error %v must name %s", err, tc.field)
			}
		})
	}
	cfg, err := buildHealth(t, `{"probes":[{"name":"full","STEPS":[`+step+`]}]}`)
	if err != nil || len(cfg.GetProbes()[0].GetSteps()) != 1 {
		t.Fatalf("capitalized valid steps: config %v, error %v", cfg, err)
	}
}

func leastCostConfig(health, settings string, selector ...string) string {
	sel := ""
	if len(selector) > 0 {
		raw, _ := json.Marshal(selector)
		sel = `,"selector":` + string(raw)
	}
	return `{
		"outbounds": [{"tag": "a1", "protocol": "freedom", "path":{"balancer":"auto","cost":1}},
		              {"tag": "b1", "protocol": "freedom", "path":{"balancer":"auto","cost":3}}],
		"routing": {"balancers": [{"tag": "auto"` + sel + `,
			"strategy": {"type": "leastCost", "settings": ` + settings + `}}]}` + health + `
	}`
}

func TestLeastCostConfigChecks(t *testing.T) {
	observed := `, "healthObservatory": {}`
	for _, tc := range []struct {
		name, input string
		ok          bool
	}{
		{"valid", leastCostConfig(observed, `{}`), true},
		{"no observatory", leastCostConfig("", `{}`), false},
		{"explicit selector", leastCostConfig(observed, `{}`, "a"), false},
		{"removed costs", leastCostConfig(observed, `{"costs":[{"match":"a","cost":1}]}`), false},
		{"removed defaultCost", leastCostConfig(observed, `{"defaultCost":1}`), false},
		{"removed lazyOutbounds", leastCostConfig(observed, `{"lazyOutbounds":["a"]}`), false},
	} {
		c := new(Config)
		err := json.Unmarshal([]byte(tc.input), c)
		if err == nil {
			_, err = c.Build()
		}
		if (err == nil) != tc.ok {
			t.Errorf("%s: build error %v, want ok %v", tc.name, err, tc.ok)
		}
	}
}

func TestHealthObservatoryConfigRejects(t *testing.T) {
	for _, input := range []string{
		`{"subjectSelector":[]}`,
		`{ "connectivity": {"outbound": "direct"}}`,
		`{ "connectivity": {"outbound": "direct", "destination": "ftp://x/"}}`,
		`{ "probes": [{"destination": "https://x/", "method": "POST"}]}`,
		`{ "probes": [{"destination": "https://x/", "method": "HEAD", "minBytes": 1}]}`,
		`{ "connectivity": {"outbound": "direct", "destination": "https://x/", "witnesses": [""]}}`,
		`{ "schedule": {"probeJitterFraction": 1}}`,
	} {
		c := new(HealthObservatoryConfig)
		err := json.Unmarshal([]byte(input), c)
		if err == nil {
			_, err = c.Build()
		}
		if err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
}

func buildHealth(t *testing.T, raw string) (*health.Config, error) {
	t.Helper()
	c := &HealthObservatoryConfig{}
	if err := json.Unmarshal([]byte(raw), c); err != nil {
		t.Fatal(err)
	}
	m, err := c.Build()
	if err != nil {
		return nil, err
	}
	return m.(*health.Config), nil
}

func TestHealthHostProbesConfig(t *testing.T) {
	cfg, err := buildHealth(t, `{ "schedule": {"hostProbes": {"probesPerMinute": 2, "reuseWindow": "30s",
		"maxOutbounds": 4, "timeout": "1m"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	want := &health.HostProbesConfig{ProbesPerMinute: 2, ReuseWindow: int64(30 * time.Second), MaxOutbounds: 4, Timeout: int64(time.Minute)}
	if got := cfg.GetSchedule().GetHostProbes(); !proto.Equal(got, want) {
		t.Fatalf("hostProbes %v, want %v", got, want)
	}
}

func TestHealthReportInterval(t *testing.T) {
	for raw, want := range map[string]int64{
		`{ "report": {}}`:                  0,
		`{ "report": {"interval": "0s"}}`:  -1,
		`{ "report": {"interval": "90m"}}`: int64(90 * time.Minute),
	} {
		cfg, err := buildHealth(t, raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.GetReport().GetInterval(); got != want {
			t.Errorf("%s: interval %d, want %d", raw, got, want)
		}
	}
}

func TestHealthDurationPresence(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		get  func(*health.Config) int64
		want int64
	}{
		{"missing window", `{"passive":{}}`, func(c *health.Config) int64 { return c.GetPassive().GetWindow() }, 0},
		{"zero window", `{"passive":{"window":"0s"}}`, func(c *health.Config) int64 { return c.GetPassive().GetWindow() }, -1},
		{"missing cost delay", `{"providers":[{"tag":"p","url":"https://x.test/","apply":{"balancer":"b","tagPrefix":"p-"},"triggers":{"whenCostAbove":{"cost":2}}}]}`, func(c *health.Config) int64 { return c.GetProviders()[0].GetTriggers().GetWhenCostAbove().GetAfter() }, 0},
		{"zero cost delay", `{"providers":[{"tag":"p","url":"https://x.test/","apply":{"balancer":"b","tagPrefix":"p-"},"triggers":{"whenCostAbove":{"cost":2,"after":"0s"}}}]}`, func(c *health.Config) int64 { return c.GetProviders()[0].GetTriggers().GetWhenCostAbove().GetAfter() }, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildHealth(t, tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if value := tc.get(got); value != tc.want {
				t.Fatalf("duration %d, want %d", value, tc.want)
			}
		})
	}
	for key, raw := range map[string]string{
		"passive.dialTimeout":         `{"passive":{"dialTimeout":"0s"}}`,
		"probes[].timeout":            `{"probes":[{"timeout":"0s"}]}`,
		"connectivity.timeout":        `{"connectivity":{"outbound":"direct","destination":"https://x.test/","timeout":"0s"}}`,
		"schedule.hostProbes.timeout": `{"schedule":{"hostProbes":{"timeout":"0s"}}}`,
		"provider p timeout":          `{"providers":[{"tag":"p","url":"https://x.test/","timeout":"0s","apply":{"balancer":"b","tagPrefix":"p-"}}]}`,
	} {
		if _, err := buildHealth(t, raw); err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("%s: error %v must name %s", raw, err, key)
		}
	}
	config := leastCostConfig(`, "healthObservatory": {}`, `{"returnToCheaper":{"interval":"0s"}}`)
	c := new(Config)
	if err := json.Unmarshal([]byte(config), c); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Build(); err == nil || !strings.Contains(err.Error(), "returnToCheaper.interval") {
		t.Fatalf("leastCost: error %v must name returnToCheaper.interval", err)
	}
}

func TestHealthSleepAfter(t *testing.T) {
	for raw, want := range map[string]int64{
		`{}`:                                  0,
		`{ "sleep": {}}`:                      0,
		`{ "sleep": {"afterLastUse": "0s"}}`:  -1,
		`{ "sleep": {"afterLastUse": "10s"}}`: int64(10 * time.Second),
		`{ "sleep": {"afterLastUse": "15m"}}`: int64(15 * time.Minute),
	} {
		cfg, err := buildHealth(t, raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.GetSleep().GetAfterLastUse(); got != want {
			t.Errorf("%s: after %d, want %d", raw, got, want)
		}
	}
	for _, raw := range []string{
		`{ "sleep": {"afterLastUse": "5s"}}`,
		`{ "sleep": {"afterLastUse": "-1m"}}`,
	} {
		if _, err := buildHealth(t, raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

// sleep.closeIdleAfter is off unless set: missing and "0s" leave zero.
func TestHealthSleepCloseIdleAfter(t *testing.T) {
	for raw, want := range map[string]int64{
		`{}`:                                    0,
		`{ "sleep": {"afterLastUse": "1m"}}`:    0,
		`{ "sleep": {"closeIdleAfter": "0s"}}`:  0,
		`{ "sleep": {"closeIdleAfter": "10s"}}`: int64(10 * time.Second),
		`{ "sleep": {"closeIdleAfter": "2m"}}`:  int64(2 * time.Minute),
		`{ "sleep": {"closeIdleAfter": "10m"}}`: int64(10 * time.Minute),
	} {
		cfg, err := buildHealth(t, raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.GetSleep().GetCloseIdleAfter(); got != want {
			t.Errorf("%s: closeIdleAfter %d, want %d", raw, got, want)
		}
	}
	for _, raw := range []string{
		`{ "sleep": {"closeIdleAfter": "9s"}}`,
		`{ "sleep": {"closeIdleAfter": "11m"}}`,
		`{ "sleep": {"closeIdleAfter": "-1m"}}`,
	} {
		if _, err := buildHealth(t, raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

func TestHealthProvidersBuild(t *testing.T) {
	cfg, err := buildHealth(t, `{

	  "providers": [{
	    "tag": "fresh", "url": "https://api.example/set", "headers": {"Authorization": "Bearer x"},
	    "via": ["auto", "direct"], "timeout": "5s",
	    "triggers": {"interval": "0s", "jitterFraction": 0, "minHealthyOutbounds": 3, "shouldRefreshWhenExhausted": false,
	                 "whenCostAbove": {"cost": 10, "after": "15m"}, "networkChangeMode": "unknown"},
	    "limits": {"minInterval": "1m", "maxRefreshesPerHour": 0, "backoff": {"initialDelay": "10s"}},
	    "apply": {"balancer": "auto", "tagPrefix": "p-", "drainTimeout": "1m"}
	  }]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Providers[0]
	tr := p.Triggers
	if tr.Interval != -1 || tr.JitterFraction != -1 || tr.MinHealthyOutbounds != 3 || !tr.SkipRefreshWhenExhausted || tr.MaxAge != 0 {
		t.Fatalf("triggers %+v: explicit zero must turn off, missing must keep the default", tr)
	}
	if tr.WhenCostAbove.Cost != 10 || tr.WhenCostAbove.After != int64(15*time.Minute) || tr.NetworkChangeMode != health.RefreshOnNetwork_REFRESH_UNKNOWN {
		t.Fatalf("triggers %+v", tr)
	}
	if l := p.Limits; l.MinInterval != int64(time.Minute) || l.MaxRefreshesPerHour != -1 || l.ExhaustedMinInterval != 0 || l.Backoff.InitialDelay != int64(10*time.Second) {
		t.Fatalf("limits %+v", l)
	}
	if p.Apply.TagPrefix != "p-" || p.Headers["Authorization"] != "Bearer x" || len(p.Via) != 2 {
		t.Fatalf("provider %+v", p)
	}
}

// The method is GET or POST in any case; left out it stays empty, which the
// observatory takes for POST.
func TestHealthProviderMethod(t *testing.T) {
	for method, want := range map[string]string{`"get"`: "GET", `"Post"`: "POST", `""`: ""} {
		cfg, err := buildHealth(t, `{ "providers": [{"tag": "a", "url": "https://x.test/",
		  "method": `+method+`, "apply": {"balancer": "auto", "tagPrefix": "p-"}}]}`)
		if err != nil || cfg.Providers[0].Method != want {
			t.Errorf("method %s: %v, %+v; want %q", method, err, cfg, want)
		}
	}
}

// Shares of a lease: an explicit renewAt or jitter of 0 turns it off, which
// the observatory reads from a negative value; missing keeps the default.
func TestHealthProviderExpiry(t *testing.T) {
	for expiry, want := range map[string]string{
		``:               "<nil>",
		`, "expiry": {}`: "&{0 0 0}",
		`, "expiry": {"renewAtFraction": 0.6, "urgentAtFraction": 0.9, "jitterFraction": 0.2}`: "&{0.6 0.9 0.2}",
		`, "expiry": {"renewAtFraction": 0, "jitterFraction": 0}`:                              "&{-1 0 -1}",
		`, "expiry": {"renewAtFraction": 0, "urgentAtFraction": 0.3}`:                          "&{-1 0.3 0}",
		`, "expiry": {"urgentAtFraction": 0.95}`:                                               "&{0 0.95 0}",
	} {
		cfg, err := buildHealth(t, `{ "providers": [{"tag": "a", "url": "https://x.test/",
		  "triggers": {"interval": "1h"`+expiry+`}, "apply": {"balancer": "auto", "tagPrefix": "p-"}}]}`)
		if err != nil {
			t.Errorf("%s: %v", expiry, err)
			continue
		}
		e := cfg.Providers[0].Triggers.Expiry
		got := "<nil>"
		if e != nil {
			got = fmt.Sprintf("&{%v %v %v}", e.RenewAtFraction, e.UrgentAtFraction, e.JitterFraction)
		}
		if got != want {
			t.Errorf("%s: %s, want %s", expiry, got, want)
		}
	}
}

// apply.acceptedHeaders names what an answer may set: missing keeps the default,
// an explicit [] allows nothing. limits.authRetry "0s" waits for new
// headers, which the observatory reads from a negative value.
func TestHealthProviderHeadersAndAuthRetry(t *testing.T) {
	for _, tc := range []struct{ apply, limits, want string }{
		{``, ``, "[] false 0"},
		{`, "acceptedHeaders": []`, `, "limits": {"authRetryDelay": "0s"}`, "[] true -1"},
		{`, "acceptedHeaders": ["Authorization", "x-api-key"]`, `, "limits": {"authRetryDelay": "2h"}`,
			fmt.Sprint("[Authorization x-api-key] false ", int64(2*time.Hour))},
	} {
		cfg, err := buildHealth(t, `{ "providers": [{"tag": "a", "url": "https://x.test/"`+tc.limits+`,
		  "apply": {"balancer": "auto", "tagPrefix": "p-"`+tc.apply+`}}]}`)
		if err != nil {
			t.Errorf("%s %s: %v", tc.apply, tc.limits, err)
			continue
		}
		p := cfg.Providers[0]
		if got := fmt.Sprint(p.Apply.AcceptedHeaders, " ", p.Apply.FixedHeaders, " ", p.GetLimits().GetAuthRetryDelay()); got != tc.want {
			t.Errorf("%s %s: %s, want %s", tc.apply, tc.limits, got, tc.want)
		}
	}
}

// initial is passed on as it is: a bad saved set must not keep the core
// from starting, so the observatory checks all of it and reports it.
func TestHealthProviderInitial(t *testing.T) {
	provider := func(initial string) string {
		return `{ "providers": [{"tag": "a", "url": "https://x.test/",
		  "apply": {"balancer": "auto", "tagPrefix": "p-"}` + initial + `}]}`
	}
	for initial, want := range map[string]string{
		``:                              "",
		`, "initialAnswer": null`:       "",
		`, "initialAnswer": {}`:         "{}",
		`, "initialAnswer": {"x": [1]}`: `{"x": [1]}`,
		`, "initialAnswer": {"outbounds": [{"tag": "x1", "protocol": "nosuch"}], "dns": {}}`: `{"outbounds": [{"tag": "x1", "protocol": "nosuch"}], "dns": {}}`,
		`, "initialAnswer": []`:   `[]`,
		`, "initialAnswer": "{}"`: `"{}"`,
		`, "initialAnswer": 1`:    `1`,
		`, "initialAnswer": {"v": "` + strings.Repeat("x", 1<<20) + `"}`: `{"v": "` + strings.Repeat("x", 1<<20) + `"}`,
	} {
		cfg, err := buildHealth(t, provider(initial))
		if err != nil {
			t.Errorf("%.40s: %v", initial, err)
		} else if got := string(cfg.Providers[0].InitialAnswer); got != want {
			t.Errorf("%.40s: %.40q, want %.40q", initial, got, want)
		}
	}
}

func TestHealthProviderBaseVersion(t *testing.T) {
	provider := func(base string) string {
		return `{ "providers": [{"tag": "a", "url": "https://x.test/",
		  "apply": {"balancer": "auto", "tagPrefix": "p-"}, "baseVersion": "` + base + `"}]}`
	}
	for _, base := range []string{"", "cfg-12", strings.Repeat("b", 256)} {
		cfg, err := buildHealth(t, provider(base))
		if err != nil {
			t.Errorf("%.20s: %v", base, err)
		} else if got := cfg.Providers[0].BaseVersion; got != base {
			t.Errorf("%.20s: %.20s", base, got)
		}
	}
	if _, err := buildHealth(t, provider(strings.Repeat("b", 257))); err == nil || !strings.Contains(err.Error(), "baseVersion") {
		t.Errorf("257 bytes: %v, want a refusal", err)
	}
}

func TestHealthProvidersReject(t *testing.T) {
	cases := map[string]string{
		"no apply":         `{"tag": "a", "url": "https://x.test/"}`,
		"no prefix":        `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto"}}`,
		"token in url":     `{"tag": "a", "url": "https://user:pw@x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"}}`,
		"not http":         `{"tag": "a", "url": "ftp://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"}}`,
		"plain http":       `{"tag": "a", "url": "http://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"}}`,
		"bad network mode": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"}, "triggers": {"networkChangeMode": "sometimes"}}`,
		"overlap": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"}},
		            {"tag": "b", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-x"}}`,
		"zero cost trigger": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"whenCostAbove": {"cost": 0, "after": "15m"}}}`,
		"empty via": `{"tag": "a", "url": "https://x.test/", "via": ["auto", ""], "apply": {"balancer": "auto", "tagPrefix": "p-"}}`,
		"put":       `{"tag": "a", "url": "https://x.test/", "method": "PUT", "apply": {"balancer": "auto", "tagPrefix": "p-"}}`,
		"renewal past the lease": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"renewAtFraction": 0.99}}}`,
		"negative renewal": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"renewAtFraction": -0.5}}}`,
		"urgent before renewal": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"renewAtFraction": 0.6, "urgentAtFraction": 0.5}}}`,
		"urgent before the default renewal": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"urgentAtFraction": 0.4}}}`,
		"wide renewal spread": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"jitterFraction": 0.6}}}`,
		"rotated host header": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-",
		            "acceptedHeaders": ["Authorization", "Host"]}}`,
		"rotated proxy header": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-",
		            "acceptedHeaders": ["proxy-authorization"]}}`,
		"rotated header not a token": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-",
		            "acceptedHeaders": ["X Key"]}}`,
		"short auth retry": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "limits": {"authRetryDelay": "1m"}}`,
		"long auth retry": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "limits": {"authRetryDelay": "25h"}}`,
	}
	for name, providers := range cases {
		if _, err := buildHealth(t, `{ "providers": [`+providers+`]}`); err == nil {
			t.Errorf("%s: accepted", name)
		} else if !strings.Contains(err.Error(), "healthObservatory") {
			t.Errorf("%s: error %v does not name the section", name, err)
		}
	}
}

func TestHealthProvidersNeedTheirBalancer(t *testing.T) {
	provider := func(balancer, prefix string) string {
		return `, "healthObservatory": { "providers": [{"tag": "fresh",
			"url": "https://x.test/", "apply": {"balancer": "` + balancer + `", "tagPrefix": "` + prefix + `"}}]}`
	}
	random := `{
		"outbounds": [{"tag": "a1", "protocol": "freedom"}],
		"routing": {"balancers": [{"tag": "auto", "selector": ["a", "p-"], "strategy": {"type": "random"}}]}` +
		provider("auto", "p-") + `}`
	for _, tc := range []struct {
		name, input string
		ok          bool
	}{
		{"provider picked up", leastCostConfig(provider("auto", "p-"), `{}`), true},
		{"prefix does not choose membership", leastCostConfig(provider("auto", "p-x"), `{}`), true},
		{"no such balancer", leastCostConfig(provider("main", "p-"), `{}`), false},
		{"explicit leastCost selector", leastCostConfig(provider("auto", "p-"), `{}`, "a", "p-x"), false},
		{"balancer without leastCost", random, false},
	} {
		c := new(Config)
		err := json.Unmarshal([]byte(tc.input), c)
		if err == nil {
			_, err = c.Build()
		}
		if (err == nil) != tc.ok {
			t.Errorf("%s: build error %v, want ok %v", tc.name, err, tc.ok)
		}
	}
}
