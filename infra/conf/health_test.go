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
				"subjectSelector": ["a", "b"],
				"passive": {"dialTimeout": "6s", "failures": 3, "keepPendingOnDown": true, "keepSilentOnDown": true},
				"connectivity": {"outbound": "direct", "destination": "https://example.com/204",
				                 "timeout": "4s", "maxPause": "90s",
				                 "recheck": {"initial": "2s", "multiplier": 2, "max": "30s"}}
			}`,
			Parser: loadJSON(creator),
			Output: &health.Config{
				SubjectSelector: []string{"a", "b"},
				Passive: &health.PassiveConfig{DialTimeout: int64(6 * time.Second), Failures: 3,
					KeepPendingOnDown: true, KeepSilentOnDown: true},
				Connectivity: &health.ConnectivityConfig{
					Outbound:    "direct",
					Destination: "https://example.com/204",
					Timeout:     int64(4 * time.Second),
					MaxPause:    int64(90 * time.Second),
					Recheck:     &health.Backoff{Initial: int64(2 * time.Second), Multiplier: 2, Max: int64(30 * time.Second)},
				},
			},
		},
	})
}

func leastCostConfig(health, settings string, selector ...string) string {
	sel, _ := json.Marshal(selector)
	return `{
		"outbounds": [{"tag": "a1", "protocol": "freedom"}, {"tag": "b1", "protocol": "freedom"}],
		"routing": {"balancers": [{"tag": "auto", "selector": ` + string(sel) + `,
			"strategy": {"type": "leastCost", "settings": ` + settings + `}}]}` + health + `
	}`
}

func TestLeastCostConfigChecks(t *testing.T) {
	observed := `, "healthObservatory": {"subjectSelector": ["a", "b"]}`
	for _, tc := range []struct {
		name, input string
		ok          bool
	}{
		{"valid", leastCostConfig(observed, `{"costs": [{"match": "a", "value": 1}]}`, "a", "b"), true},
		{"no observatory", leastCostConfig("", `{}`, "a"), false},
		{"selector not observed", leastCostConfig(`, "healthObservatory": {"subjectSelector": ["a"]}`, `{}`, "a", "b"), false},
		{"zero cost", leastCostConfig(observed, `{"costs": [{"match": "a", "value": 0}]}`, "a"), false},
		{"broken regexp", leastCostConfig(observed, `{"costs": [{"regexp": true, "match": "a(", "value": 1}]}`, "a"), false},
	} {
		c := new(Config)
		if err := json.Unmarshal([]byte(tc.input), c); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		_, err := c.Build()
		if (err == nil) != tc.ok {
			t.Errorf("%s: build error %v, want ok %v", tc.name, err, tc.ok)
		}
	}
}

func TestHealthObservatoryConfigRejects(t *testing.T) {
	for _, input := range []string{
		`{}`,
		`{"subjectSelector": ["a"], "connectivity": {"outbound": "direct"}}`,
		`{"subjectSelector": ["a"], "connectivity": {"outbound": "direct", "destination": "ftp://x/"}}`,
		`{"subjectSelector": ["a"], "probes": [{"destination": "https://x/", "method": "POST"}]}`,
		`{"subjectSelector": ["a"], "probes": [{"destination": "https://x/", "method": "HEAD", "minBytes": 1}]}`,
		`{"subjectSelector": ["a"], "connectivity": {"outbound": "direct", "destination": "https://x/", "witnesses": [""]}}`,
		`{"subjectSelector": ["a"], "schedule": {"spacingJitter": 1}}`,
	} {
		c := new(HealthObservatoryConfig)
		if err := json.Unmarshal([]byte(input), c); err != nil {
			t.Fatalf("%s: %v", input, err)
		}
		if _, err := c.Build(); err == nil {
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
	cfg, err := buildHealth(t, `{"subjectSelector": ["a"], "schedule": {"hostProbes": {"perMinute": 2, "reuse": "30s",
		"maxOutbounds": 4, "timeout": "1m"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	want := &health.HostProbesConfig{PerMinute: 2, Reuse: int64(30 * time.Second), MaxOutbounds: 4, Timeout: int64(time.Minute)}
	if got := cfg.GetSchedule().GetHostProbes(); !proto.Equal(got, want) {
		t.Fatalf("hostProbes %v, want %v", got, want)
	}
}

func TestHealthReportInterval(t *testing.T) {
	for raw, want := range map[string]int64{
		`{"subjectSelector": ["a"], "report": {}}`:                  0,
		`{"subjectSelector": ["a"], "report": {"interval": "0s"}}`:  -1,
		`{"subjectSelector": ["a"], "report": {"interval": "90m"}}`: int64(90 * time.Minute),
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

func TestHealthSleepAfter(t *testing.T) {
	for raw, want := range map[string]int64{
		`{"subjectSelector": ["a"]}`:                            0,
		`{"subjectSelector": ["a"], "sleep": {}}`:               0,
		`{"subjectSelector": ["a"], "sleep": {"after": "0s"}}`:  -1,
		`{"subjectSelector": ["a"], "sleep": {"after": "10s"}}`: int64(10 * time.Second),
		`{"subjectSelector": ["a"], "sleep": {"after": "15m"}}`: int64(15 * time.Minute),
	} {
		cfg, err := buildHealth(t, raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.GetSleep().GetAfter(); got != want {
			t.Errorf("%s: after %d, want %d", raw, got, want)
		}
	}
	for _, raw := range []string{
		`{"subjectSelector": ["a"], "sleep": {"after": "5s"}}`,
		`{"subjectSelector": ["a"], "sleep": {"after": "-1m"}}`,
	} {
		if _, err := buildHealth(t, raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

func TestHealthProvidersBuild(t *testing.T) {
	cfg, err := buildHealth(t, `{
	  "subjectSelector": ["a"],
	  "providers": [{
	    "tag": "fresh", "url": "https://api.example/set", "headers": {"Authorization": "Bearer x"},
	    "via": ["auto", "direct"], "timeout": "5s",
	    "triggers": {"interval": "0s", "jitter": 0, "minHealthy": 3, "whenExhausted": false,
	                 "whenCostAbove": {"cost": 10, "for": "15m"}, "onNetworkChange": "unknown"},
	    "limits": {"minInterval": "1m", "maxPerHour": 0, "backoff": {"initial": "10s"}},
	    "apply": {"balancer": "auto", "tagPrefix": "p-", "drain": "1m"}
	  }]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Providers[0]
	tr := p.Triggers
	if tr.Interval != -1 || tr.Jitter != -1 || tr.MinHealthy != 3 || !tr.SkipWhenExhausted || tr.MaxAge != 0 {
		t.Fatalf("triggers %+v: explicit zero must turn off, missing must keep the default", tr)
	}
	if tr.CostAbove.Cost != 10 || tr.CostAbove.After != int64(15*time.Minute) || tr.OnNetworkChange != health.RefreshOnNetwork_REFRESH_UNKNOWN {
		t.Fatalf("triggers %+v", tr)
	}
	if l := p.Limits; l.MinInterval != int64(time.Minute) || l.MaxPerHour != -1 || l.ExhaustedMinInterval != 0 || l.Backoff.Initial != int64(10*time.Second) {
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
		cfg, err := buildHealth(t, `{"subjectSelector": ["a"], "providers": [{"tag": "a", "url": "https://x.test/",
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
		`, "expiry": {"renewAt": 0.6, "urgentAt": 0.9, "jitter": 0.2}`: "&{0.6 0.9 0.2}",
		`, "expiry": {"renewAt": 0, "jitter": 0}`:                      "&{-1 0 -1}",
		`, "expiry": {"renewAt": 0, "urgentAt": 0.3}`:                  "&{-1 0.3 0}",
		`, "expiry": {"urgentAt": 0.95}`:                               "&{0 0.95 0}",
	} {
		cfg, err := buildHealth(t, `{"subjectSelector": ["a"], "providers": [{"tag": "a", "url": "https://x.test/",
		  "triggers": {"interval": "1h"`+expiry+`}, "apply": {"balancer": "auto", "tagPrefix": "p-"}}]}`)
		if err != nil {
			t.Errorf("%s: %v", expiry, err)
			continue
		}
		e := cfg.Providers[0].Triggers.Expiry
		got := "<nil>"
		if e != nil {
			got = fmt.Sprintf("&{%v %v %v}", e.RenewAt, e.UrgentAt, e.Jitter)
		}
		if got != want {
			t.Errorf("%s: %s, want %s", expiry, got, want)
		}
	}
}

// apply.headers names what an answer may set: missing keeps the default,
// an explicit [] allows nothing. limits.authRetry "0s" waits for new
// headers, which the observatory reads from a negative value.
func TestHealthProviderHeadersAndAuthRetry(t *testing.T) {
	for _, tc := range []struct{ apply, limits, want string }{
		{``, ``, "[] false 0"},
		{`, "headers": []`, `, "limits": {"authRetry": "0s"}`, "[] true -1"},
		{`, "headers": ["Authorization", "x-api-key"]`, `, "limits": {"authRetry": "2h"}`,
			fmt.Sprint("[Authorization x-api-key] false ", int64(2*time.Hour))},
	} {
		cfg, err := buildHealth(t, `{"subjectSelector": ["a"], "providers": [{"tag": "a", "url": "https://x.test/"`+tc.limits+`,
		  "apply": {"balancer": "auto", "tagPrefix": "p-"`+tc.apply+`}}]}`)
		if err != nil {
			t.Errorf("%s %s: %v", tc.apply, tc.limits, err)
			continue
		}
		p := cfg.Providers[0]
		if got := fmt.Sprint(p.Apply.Headers, " ", p.Apply.FixedHeaders, " ", p.GetLimits().GetAuthRetry()); got != tc.want {
			t.Errorf("%s %s: %s, want %s", tc.apply, tc.limits, got, tc.want)
		}
	}
}

// initial is passed on as it is: a bad saved set must not keep the core
// from starting, so the observatory checks all of it and reports it.
func TestHealthProviderInitial(t *testing.T) {
	provider := func(initial string) string {
		return `{"subjectSelector": ["a"], "providers": [{"tag": "a", "url": "https://x.test/",
		  "apply": {"balancer": "auto", "tagPrefix": "p-"}` + initial + `}]}`
	}
	for initial, want := range map[string]string{
		``:                        "",
		`, "initial": null`:       "",
		`, "initial": {}`:         "{}",
		`, "initial": {"x": [1]}`: `{"x": [1]}`,
		`, "initial": {"outbounds": [{"tag": "x1", "protocol": "nosuch"}], "dns": {}}`: `{"outbounds": [{"tag": "x1", "protocol": "nosuch"}], "dns": {}}`,
		`, "initial": []`:   `[]`,
		`, "initial": "{}"`: `"{}"`,
		`, "initial": 1`:    `1`,
		`, "initial": {"v": "` + strings.Repeat("x", 1<<20) + `"}`: `{"v": "` + strings.Repeat("x", 1<<20) + `"}`,
	} {
		cfg, err := buildHealth(t, provider(initial))
		if err != nil {
			t.Errorf("%.40s: %v", initial, err)
		} else if got := string(cfg.Providers[0].Initial); got != want {
			t.Errorf("%.40s: %.40q, want %.40q", initial, got, want)
		}
	}
}

func TestHealthProviderBaseVersion(t *testing.T) {
	provider := func(base string) string {
		return `{"subjectSelector": ["a"], "providers": [{"tag": "a", "url": "https://x.test/",
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
		"bad network mode": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"}, "triggers": {"onNetworkChange": "sometimes"}}`,
		"overlap": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"}},
		            {"tag": "b", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-x"}}`,
		"zero cost trigger": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"whenCostAbove": {"cost": 0, "for": "15m"}}}`,
		"empty via": `{"tag": "a", "url": "https://x.test/", "via": ["auto", ""], "apply": {"balancer": "auto", "tagPrefix": "p-"}}`,
		"put":       `{"tag": "a", "url": "https://x.test/", "method": "PUT", "apply": {"balancer": "auto", "tagPrefix": "p-"}}`,
		"renewal past the lease": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"renewAt": 0.99}}}`,
		"negative renewal": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"renewAt": -0.5}}}`,
		"urgent before renewal": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"renewAt": 0.6, "urgentAt": 0.5}}}`,
		"urgent before the default renewal": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"urgentAt": 0.4}}}`,
		"wide renewal spread": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "triggers": {"expiry": {"jitter": 0.6}}}`,
		"rotated host header": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-",
		            "headers": ["Authorization", "Host"]}}`,
		"rotated proxy header": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-",
		            "headers": ["proxy-authorization"]}}`,
		"rotated header not a token": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-",
		            "headers": ["X Key"]}}`,
		"short auth retry": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "limits": {"authRetry": "1m"}}`,
		"long auth retry": `{"tag": "a", "url": "https://x.test/", "apply": {"balancer": "auto", "tagPrefix": "p-"},
		            "limits": {"authRetry": "25h"}}`,
	}
	for name, providers := range cases {
		if _, err := buildHealth(t, `{"subjectSelector": ["a"], "providers": [`+providers+`]}`); err == nil {
			t.Errorf("%s: accepted", name)
		} else if !strings.Contains(err.Error(), "healthObservatory") {
			t.Errorf("%s: error %v does not name the section", name, err)
		}
	}
}

func TestHealthProvidersNeedTheirBalancer(t *testing.T) {
	provider := func(balancer, prefix string) string {
		return `, "healthObservatory": {"subjectSelector": ["a"], "providers": [{"tag": "fresh",
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
		{"prefix observed and picked up", leastCostConfig(provider("auto", "p-"), `{}`, "a", "p-"), true},
		{"selector wider than the observed prefix", leastCostConfig(provider("auto", "p-x"), `{}`, "a", "p-"), false},
		{"no such balancer", leastCostConfig(provider("main", "p-"), `{}`, "a", "p-"), false},
		{"balancer misses the prefix", leastCostConfig(provider("auto", "p-"), `{}`, "a", "p-x"), false},
		{"balancer without leastCost", random, false},
	} {
		c := new(Config)
		if err := json.Unmarshal([]byte(tc.input), c); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		_, err := c.Build()
		if (err == nil) != tc.ok {
			t.Errorf("%s: build error %v, want ok %v", tc.name, err, tc.ok)
		}
	}
}
