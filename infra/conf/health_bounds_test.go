package conf_test

import (
	"strings"
	"testing"
)

func TestHealthBoundsRejectWithRange(t *testing.T) {
	udp := func(key, value string) string {
		return `{"probes":{"udp":{"destinations":["stun.test:3478"],"` + key + `":` + value + `}}}`
	}
	step := func(key, value string) string {
		return `{"probes":{"profiles":[{"name":"p","steps":[{"name":"a","destinations":["https://x.test"],"` + key + `":` + value + `}]}]}}`
	}
	for raw, want := range map[string]string{
		`{"probes":{"limits":{"perMinute":61}}}`:              "probes.limits.perMinute must be from 1 to 60",
		`{"probes":{"limits":{"perServerNamePerMinute":11}}}`: "probes.limits.perServerNamePerMinute must be from 1 to 10",
		`{"probes":{"limits":{"maxConcurrent":9}}}`:           "probes.limits.maxConcurrent must be from 1 to 8",
		`{"probes":{"requests":{"maxOutbounds":17}}}`:         "probes.requests.maxOutbounds must be from 1 to 16",
		`{"probes":{"requests":{"timeout":"11m"}}}`:           "probes.requests.timeout must be from 1ns to 10m0s",
		udp("requests", "10000000"):                           "probes.udp.requests must be from 1 to 32",
		udp("interval", `"3s"`):                               "probes.udp.interval must be from 1ns to 2s",
		udp("timeout", `"11s"`):                               "probes.udp.timeout must be from 1ns to 10s",
		udp("perHour", "61"):                                  "probes.udp.perHour must be from 1 to 60",
		step("minBytes", "1048577"):                           "minBytes must be from 0 to 1048576",
		step("timeout", `"31s"`):                              "timeout must be from 1ns to 30s",
		step("progressTimeout", `"31s"`):                      "progressTimeout must be from 1ns to 30s",
		`{"network":{"connectivity":{"outbound":"direct","destinations":["https://x.test/204"],"timeout":"31s"}}}`:                             "network.connectivity.timeout must be from 1ns to 30s",
		`{"network":{"memory":{"maxNetworks":65}}}`:                                                                                            "network.memory.maxNetworks must be from 1 to 64",
		`{"network":{"labelRequest":{"outbound":"direct","labels":{"asn":"X-Asn"},"destinations":["https://x.test/l"],"requestsPerHour":61}}}`: "network.labelRequest.requestsPerHour must be from 1 to 60",
		`{"telemetry":{"events":{"maxEvents":4097}}}`:                                                                                          "telemetry.events.maxEvents must be from 1 to 4096",
		`{"telemetry":{"events":{"maxBytes":262145}}}`:                                                                                         "telemetry.events.maxBytes must be from 1 to 262144",
		`{"providers":[{"limits":{"maxRefreshesPerHour":600}}]}`:                                                                               "limits.maxRefreshesPerHour must be from 1 to 60",
		`{"providers":[{"apply":{"drainTimeout":"31m"}}]}`:                                                                                     "apply.drainTimeout must be from 1ns to 30m0s",
		`{"providers":[{"triggers":{"periodic":{"interval":"0s"}}}]}`:                                                                          "triggers.periodic.interval must be greater than 0s",
		`{"providers":[{"triggers":{"periodic":{"interval":"0s..1h"}}}]}`:                                                                      "triggers.periodic.interval must be greater than 0s",
	} {
		var err error
		if strings.Contains(raw, "providers") {
			_, err = buildHealthProvider(t, raw)
		} else {
			_, err = buildHealth(t, raw)
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", raw, err, want)
		}
	}
	if _, err := buildHealth(t, `{"probes":{"limits":{"perMinute":0}}}`); err == nil || strings.Contains(err.Error(), "maximum 0") {
		t.Errorf("count without a ceiling: %v", err)
	}
}

func TestHealthBoundsAcceptEdges(t *testing.T) {
	for _, raw := range []string{
		`{"probes":{"limits":{"perMinute":60,"perServerNamePerMinute":10,"maxConcurrent":8},"requests":{"maxOutbounds":16,"timeout":"10m"},"udp":{"destinations":["stun.test:3478"],"requests":32,"interval":"2s","timeout":"10s","perHour":60}}}`,
		`{"probes":{"profiles":[{"name":"p","steps":[{"name":"a","destinations":["https://x.test"],"minBytes":1048576,"timeout":"30s","progressTimeout":"30s"}]}]}}`,
		`{"network":{"connectivity":{"outbound":"direct","destinations":["https://x.test/204"],"timeout":"30s"},"memory":{"maxNetworks":64}},"telemetry":{"events":{"maxEvents":4096,"maxBytes":262144}}}`,
	} {
		if _, err := buildHealth(t, raw); err != nil {
			t.Errorf("%s: %v", raw, err)
		}
	}
	if _, err := buildHealthProvider(t, `{"providers":[{"limits":{"maxRefreshesPerHour":60},"apply":{"drainTimeout":"30m"},"triggers":{"periodic":{"interval":"1ns..2h"}}}]}`); err != nil {
		t.Error(err)
	}
}

func TestHealthTagReferences(t *testing.T) {
	provider := func(fields string) string {
		return `"providers":[{"tag":"fresh","urls":["https://x.test/set"],"apply":{"balancers":["auto"],"tagPrefix":"p-"}` + fields + `}]`
	}
	withDirect := func(raw string) string {
		return strings.Replace(raw, `{"tag":"block","protocol":"blackhole"}`, `{"tag":"block","protocol":"blackhole"},{"tag":"direct","protocol":"freedom"}`, 1)
	}
	for name, c := range map[string]struct{ raw, want string }{
		"fallbackTag":                      {strings.Replace(leastCostConfig(`,"healthObservatory":{}`, `{}`), `"fallbackTag":"block"`, `"fallbackTag":"blok"`, 1), "fallbackTag: unknown outbound blok"},
		"connectivity":                     {leastCostConfig(`,"healthObservatory":{"network":{"connectivity":{"outbound":"drect","destinations":["https://x.test/204"]}}}`, `{}`), "connectivity.outbound: unknown outbound drect"},
		"labelRequest":                     {leastCostConfig(`,"healthObservatory":{"network":{"labelRequest":{"outbound":"drect","labels":{"asn":"X-Asn"},"destinations":["https://x.test/l"]}}}`, `{}`), "labelRequest.outbound: unknown outbound drect"},
		"via":                              {leastCostConfig(`,"healthObservatory":{`+provider(`,"via":["auto","drect"]`)+`}`, `{}`), "via: unknown outbound or balancer drect"},
		"witnesses":                        {leastCostConfig(`,"healthObservatory":{"network":{"witnesses":{"outbounds":["dir"]}}}`, `{}`), "witnesses.outbounds: dir matches no outbound"},
		"prefix captures block":            {strings.Replace(leastCostConfig(`,"healthObservatory":{`+provider(``)+`}`, `{}`), `"tagPrefix":"p-"`, `"tagPrefix":"b"`, 1), "outbound block is under apply.tagPrefix b"},
		"prefix captures via":              {withDirect(strings.Replace(leastCostConfig(`,"healthObservatory":{`+provider(`,"via":["direct"]`)+`}`, `{}`), `"tagPrefix":"p-"`, `"tagPrefix":"d"`, 1)), "outbound direct is under apply.tagPrefix d"},
		"prefix captures another balancer": {strings.Replace(strings.Replace(leastCostConfig(`,"healthObservatory":{`+provider(``)+`}`, `{}`), `"tagPrefix":"p-"`, `"tagPrefix":"b1"`, 1), `{"tag":"b1","protocol":"freedom","selection":{"balancer":"auto"`, `{"tag":"b1","protocol":"freedom","selection":{"balancer":"other"`, 1), "outbound b1 is under apply.tagPrefix b1"},
	} {
		if strings.Contains(name, "another balancer") {
			c.raw = strings.Replace(c.raw, `"balancers":[{"tag":"auto"`, `"balancers":[{"tag":"other","fallbackTag":"block","strategy":{"type":"leastCost"}},{"tag":"auto"`, 1)
		}
		if err := loadConfig(c.raw); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
	for name, raw := range map[string]string{
		"via balancer and outbound": withDirect(leastCostConfig(`,"healthObservatory":{`+provider(`,"via":["auto","direct"]`)+`,"network":{"connectivity":{"outbound":"direct","destinations":["https://x.test/204"]},"witnesses":{"outbounds":["dir","p-"]}}}`, `{}`)),
		"prefix over own selection": strings.Replace(leastCostConfig(`,"healthObservatory":{`+provider(``)+`}`, `{}`), `"tagPrefix":"p-"`, `"tagPrefix":"a"`, 1),
	} {
		if err := loadConfig(raw); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestHealthNetworkModesNeedWhatTheyUse(t *testing.T) {
	settings := `{"returnToCheaper":{"onNetworkChange":{}},"networkChanges":{}}`
	for name, c := range map[string]struct{ observer, settings, want string }{
		"forget":           {`{"network":{"memory":{}}}`, `{"returnToCheaper":{"onNetworkChange":{}},"networkChanges":{"action":"forget"}}`, "onNetworkChange requires networkChanges.action:recheck"},
		"scope unknown":    {`{}`, settings, "onNetworkChange.scope:unknown requires healthObservatory.network.memory"},
		"provider unknown": {`{"providers":[{"tag":"fresh","urls":["https://x.test/set"],"apply":{"balancers":["auto"],"tagPrefix":"p-"},"triggers":{"networkChange":{}}}]}`, `{}`, "networkChange.mode:unknownNotCheapest requires healthObservatory.network.memory"},
	} {
		if err := loadConfig(leastCostConfig(`,"healthObservatory":`+c.observer, c.settings)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
	for name, c := range map[string]struct{ observer, settings string }{
		"scope all":       {`{}`, `{"returnToCheaper":{"onNetworkChange":{"scope":"all"}},"networkChanges":{}}`},
		"memory":          {`{"network":{"memory":{}}}`, settings},
		"provider any":    {`{"providers":[{"tag":"fresh","urls":["https://x.test/set"],"apply":{"balancers":["auto"],"tagPrefix":"p-"},"triggers":{"networkChange":{"mode":"any"}}}]}`, `{}`},
		"provider memory": {`{"network":{"memory":{}},"providers":[{"tag":"fresh","urls":["https://x.test/set"],"apply":{"balancers":["auto"],"tagPrefix":"p-"},"triggers":{"networkChange":{"mode":"unknown"}}}]}`, `{}`},
	} {
		if err := loadConfig(leastCostConfig(`,"healthObservatory":`+c.observer, c.settings)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
