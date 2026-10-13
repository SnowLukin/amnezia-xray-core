package health

import (
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func TestProviderReturnToCheaperBlockPresence(t *testing.T) {
	for _, tc := range []struct {
		name, raw   string
		wantPresent bool
		wantOff     bool
		interval    time.Duration
		wantErr     bool
	}{
		{name: "omitted", raw: `{}`},
		{name: "empty block", raw: `{"returnToCheaper":{}}`, wantPresent: true},
		{name: "interval", raw: `{"returnToCheaper":{"interval":"2m"}}`, wantPresent: true, interval: 2 * time.Minute},
		{name: "retired true", raw: `{"returnToCheaper":{"isEnabled":true}}`, wantErr: true},
		{name: "retired false", raw: `{"returnToCheaper":{"isEnabled":false}}`, wantPresent: true, wantOff: true},
		{name: "boolean", raw: `{"returnToCheaper":true}`, wantErr: true},
		{name: "null", raw: `{"returnToCheaper":null}`, wantErr: true},
		{name: "zero interval", raw: `{"returnToCheaper":{"interval":"0s"}}`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var obj map[string]json.RawMessage
			if err := json.Unmarshal([]byte(tc.raw), &obj); err != nil {
				t.Fatal(err)
			}
			interval, present, err := answerReturn(obj, "selection")
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v, expected error=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if (present != nil) != tc.wantPresent || (present != nil && *present == tc.wantOff) || time.Duration(interval) != tc.interval {
				t.Fatalf("presence=%v interval=%s", present, time.Duration(interval))
			}
		})
	}
}

func TestProviderPresenceRuleAppliesToSelectionAndGroup(t *testing.T) {
	for _, group := range []bool{false, true} {
		for _, block := range []string{`{}`, `{"interval":"2m"}`, `{"isEnabled":true}`, `{"isEnabled":false}`} {
			where := `"cost":1,"returnToCheaper":` + block
			extra := ""
			if group {
				where = `"group":"g"`
				extra = `,"groups":[{"name":"g","cost":1,"returnToCheaper":` + block + `}]`
			}
			a, err := parseAnswer([]byte(fmt.Sprintf(`{"outbounds":[{"tag":"x","protocol":"vless","selection":{"balancer":"auto",%s}}]%s}`, where, extra)))
			if err == nil {
				_, err = checkHeads(newProviderSettings(v2ProviderConfig()), a)
			}
			wantErr := block == `{"isEnabled":true}`
			if (err != nil) != wantErr {
				t.Fatalf("group=%v block=%s: %v", group, block, err)
			}
		}
	}
}

func TestProviderMirrorsNormalizationAndBounds(t *testing.T) {
	pc := v2ProviderConfig()
	pc.Urls = []string{"https://first.test/set", "https://second.test/set"}
	cfg := newProviderSettings(pc)
	pc.Urls[0] = "https://mutated.test/set"
	if cfg.url != "https://first.test/set" || len(cfg.urls) != 2 || cfg.urls[0] != cfg.url {
		t.Fatalf("mirror identity was not stable and copied: %+v", cfg)
	}
	for _, mirrors := range [][]string{
		{"https://one.test/set"},
		{"https://one.test/set", "https://two.test/set", "https://three.test/set", "https://four.test/set"},
		{"https://one.test/set", "https://two.test/set", "https://three.test/set", "https://four.test/set", "https://five.test/set"},
		{"https://secret:password@one.test/set"},
		{"https://one.test/set#fragment"},
		{"ftp://one.test/set"},
		{""},
	} {
		pc := v2ProviderConfig()
		pc.Urls = mirrors
		err := validateProviderConfigurations(&Config{Providers: []*ProviderConfig{pc}})
		wantOK := len(mirrors) == 4 || (len(mirrors) == 1 && mirrors[0] == "https://one.test/set")
		if (err == nil) != wantOK {
			t.Fatalf("mirrors=%v error=%v", mirrors, err)
		}
	}
}

func TestProviderAnswerRequiresExplicitProbeProfile(t *testing.T) {
	s := newSettings(&Config{Probes: []*ProbeProfile{{Name: "check", Destination: "http://127.0.0.1/probe"}}, Features: &FeatureConfig{StepProfiles: proto.Bool(true)}})
	for _, tc := range []struct {
		name    string
		path    *PathDefinition
		group   *PathGroup
		wantErr bool
	}{
		{name: "missing standalone", path: &PathDefinition{Tag: "x"}, wantErr: true},
		{name: "unknown standalone", path: &PathDefinition{Tag: "x", Probe: "missing"}, wantErr: true},
		{name: "configured standalone", path: &PathDefinition{Tag: "x", Probe: "check"}},
		{name: "missing group", path: &PathDefinition{Tag: "x", Group: "g"}, group: &PathGroup{Name: "g"}, wantErr: true},
		{name: "configured group", path: &PathDefinition{Tag: "x", Group: "g"}, group: &PathGroup{Name: "g", Probe: "check"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &providerAnswer{paths: []*PathDefinition{tc.path}}
			if tc.group != nil {
				a.groups = []*PathGroup{tc.group}
			}
			err := validateAnswerProbes(s, a)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v expected error=%v", err, tc.wantErr)
			}
		})
	}
}
