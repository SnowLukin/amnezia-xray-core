package health

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common/net"
)

// Protocol 1 knows its keys: the routing of an answer changes on the fly with
// its domain strategy, and a section of the config the core reads only at
// start refuses the answer instead of asking for a restart.
func TestAnswerKeysOfProtocol1(t *testing.T) {
	a, err := parseAnswer([]byte(`{"protocol": 1, "outbounds": [{"tag": "x"}],
		"routing": {"domainStrategy": "IPIfNonMatch", "rules": []}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !a.withStrategy || !strings.Contains(string(a.routing), "IPIfNonMatch") {
		t.Fatalf("strategy taken %v, routing %s", a.withStrategy, a.routing)
	}
	if a, _ := parseAnswer([]byte(`{"outbounds": [{"tag": "x"}], "routing": {"rules": []}}`)); a.withStrategy {
		t.Fatal("routing without a strategy asks to change it")
	}
	for body, want := range map[string]string{
		`{"outbounds": [{"tag": "x"}], "dns": {"servers": ["1.1.1.1"]}}`:     `invalid answer: unknown key "dns"`,
		`{"outbounds": [{"tag": "x"}], "routing": {"domainStrategy": "Ip"}}`: `invalid answer: routing: unknown domainStrategy "Ip"`,
	} {
		if _, err := parseAnswer([]byte(body)); err == nil || !isAnswerError(err) || err.Error() != want {
			t.Errorf("%s: %v, want %s", body, err, want)
		}
	}
}

// An answer, and the initial set alike, takes no section of the config but
// outbounds and routing. A section of the host never comes from a backend,
// and one the core reads only at start would need a restart: no answer may
// name such a section yet, so each refuses the answer and nothing asks for
// a restart. The keys are those of the config file; version there is a
// section, here the name of the set, which an object cannot be.
func TestAnswerTakesNoSectionOfTheConfig(t *testing.T) {
	sections := []string{"transport", "log", "dns", "inbounds", "policy", "api", "metrics", "stats", "reverse",
		"fakeDns", "observatory", "burstObservatory", "geodata"}
	for name, parse := range map[string]func([]byte) (*providerAnswer, error){"answer": parseAnswer, "initial": parseInitial} {
		for _, key := range sections {
			want := `invalid answer: unknown key "` + key + `"`
			if _, err := parse([]byte(`{"outbounds": [{"tag": "x"}], "` + key + `": {}}`)); err == nil || err.Error() != want {
				t.Errorf("%s with %s: %v, want %s", name, key, err, want)
			}
		}
		for body, want := range map[string]string{
			`{"outbounds": [{"tag": "x"}], "healthObservatory": {}}`:      "invalid answer: healthObservatory is not accepted in an answer",
			`{"outbounds": [{"tag": "x"}], "version": {"min": "25.1.1"}}`: "invalid answer: version: ",
		} {
			if _, err := parse([]byte(body)); err == nil || !isAnswerError(err) || !strings.HasPrefix(err.Error(), want) {
				t.Errorf("%s %s: %v, want %s", name, body, err, want)
			}
		}
	}
}

// testRouting is a tiny stand-in for the routing section: rules go to the
// outbound in "to" or the balancer in "via"; balancers choose by prefix.
func testRouting(raw json.RawMessage) (*router.Config, error) {
	var in struct {
		Rules []struct {
			To      string `json:"to"`
			Via     string `json:"via"`
			Webhook string `json:"webhook"`
		} `json:"rules"`
		Balancers []struct {
			Tag      string   `json:"tag"`
			Selector []string `json:"selector"`
			Strategy string   `json:"strategy"`
		} `json:"balancers"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err
	}
	rc := &router.Config{}
	for _, r := range in.Rules {
		rule := &router.RoutingRule{Networks: []net.Network{net.Network_TCP}, TargetTag: &router.RoutingRule_Tag{Tag: r.To}}
		if r.Via != "" {
			rule.TargetTag = &router.RoutingRule_BalancingTag{BalancingTag: r.Via}
		}
		if r.Webhook != "" {
			rule.Webhook = &router.WebhookConfig{Url: r.Webhook}
		}
		rc.Rule = append(rc.Rule, rule)
	}
	for _, b := range in.Balancers {
		rc.BalancingRule = append(rc.BalancingRule, &router.BalancingRule{Tag: b.Tag, OutboundSelector: b.Selector, Strategy: b.Strategy})
	}
	return rc, nil
}

func routingAnswer(t *testing.T, routing string) *providerAnswer {
	t.Helper()
	a, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}], "routing": ` + routing + `}`))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestApplyRoutingWithTheSet(t *testing.T) {
	own := `{"tag": "auto", "strategy": "leastcost"}`
	for _, tc := range []struct {
		name, routing string
		applied       bool
		err           string
	}{
		{"valid", `{"rules": [{"via": "auto"}, {"to": "p-x1"}], "balancers": [` + own + `]}`, true, ""},
		{"rule to an unknown outbound", `{"rules": [{"to": "nowhere"}], "balancers": [` + own + `]}`, false, "unknown outbound"},
		{"rule to an unknown balancer", `{"rules": [{"via": "other"}], "balancers": [` + own + `]}`, false, "unknown balancer"},
		{"balancer of the provider dropped", `{"rules": [{"to": "p-x1"}], "balancers": []}`, false, "drops"},
		{"leastCost selector in routing", `{"rules": [], "balancers": [` + own + `,
			{"tag": "b", "selector": ["z"], "strategy": "leastcost"}]}`, false, "selector is not supported"},
		{"rule with a webhook", `{"rules": [{"via": "auto", "webhook": "https://hook.test/"}], "balancers": [` + own + `]}`,
			false, "webhook"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newApplyFixture(t, time.Minute)
			f.rt.cfg.routing = true
			f.o.parseRouting = testRouting
			var replaced *router.Config
			f.o.replaceRules = func(rc *router.Config, _ bool) error { replaced = rc; return nil }
			_, err := f.apply(t, routingAnswer(t, tc.routing))
			if tc.applied {
				if err != nil || replaced == nil || len(replaced.Rule) != 2 {
					t.Fatalf("valid routing: err %v, replaced %v", err, replaced)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("error %v, want one about %q", err, tc.err)
			}
			if replaced != nil || f.m.tags() != "" {
				t.Fatalf("a broken routing changed something: replaced %v, outbounds %q", replaced != nil, f.m.tags())
			}
		})
	}
}

// IsRoutingAccepted off is the choice of the client, not a change the host has to
// restart for: the set applies without it.
func TestRoutingIgnoredWhenOff(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	f.o.parseRouting = testRouting
	called := false
	f.o.replaceRules = func(*router.Config, bool) error { called = true; return nil }
	if _, err := f.apply(t, routingAnswer(t, `{"rules": []}`)); err != nil {
		t.Fatal(err)
	}
	raw, _ := f.o.DrainEvents()
	var evs []Event
	_ = json.Unmarshal(raw, &evs)
	var apply map[string]any
	for _, e := range evs {
		if e.Type == "provider_restart_needed" {
			t.Fatalf("routing off asks for a restart: %s", raw)
		}
		if e.Type == "provider_apply" {
			apply = e.Fields
		}
	}
	if called || apply["is_ok"] != true || apply["is_changed"] != true || apply["is_routing_ignored"] != true {
		t.Fatalf("routing applied %v; apply %v", called, apply)
	}
}

// The strategy of the answer goes to the router only when the answer has one.
func TestApplyRoutingTakesTheStrategyOfTheAnswer(t *testing.T) {
	own := `{"tag": "auto", "strategy": "leastcost"}`
	for _, tc := range []struct {
		routing string
		want    bool
	}{
		{`{"rules": [], "balancers": [` + own + `]}`, false},
		{`{"rules": [], "balancers": [` + own + `], "domainStrategy": "IPOnDemand"}`, true},
	} {
		f := newApplyFixture(t, time.Minute)
		f.rt.cfg.routing = true
		f.o.parseRouting = testRouting
		var called, withStrategy bool
		f.o.replaceRules = func(_ *router.Config, ws bool) error { called, withStrategy = true, ws; return nil }
		if _, err := f.apply(t, routingAnswer(t, tc.routing)); err != nil || !called || withStrategy != tc.want {
			t.Fatalf("%s: err %v, applied %v, with the strategy %v", tc.routing, err, called, withStrategy)
		}
	}
}

// Balancers the new routing does not have stop being judged and forget what
// they remembered per network.
func TestRoutingForgetsBalancersItNoLongerHas(t *testing.T) {
	own := `{"tag": "auto", "strategy": "leastcost"}`
	for _, refused := range []bool{false, true} {
		f := newApplyFixture(t, time.Minute)
		f.rt.cfg.routing = true
		f.o.parseRouting = testRouting
		f.o.replaceRules = func(*router.Config, bool) error {
			if refused {
				return errors.New("refused")
			}
			return nil
		}
		f.o.e.addSelector("old", newSelectorSettings(&SelectorConfig{}))
		f.o.e.networks["home"] = map[string]string{"auto": "p-x1", "old": "p-x1"}
		f.o.e.networks["work"] = map[string]string{"old": "p-x1"}
		f.o.e.netOrder = []string{"home", "work"}
		if _, err := f.apply(t, routingAnswer(t, `{"rules": [{"via": "auto"}], "balancers": [`+own+`]}`)); err != nil {
			t.Fatal(err)
		}
		_, kept := f.o.e.selectors["old"]
		if kept != refused {
			t.Fatalf("routing refused %v: balancer old kept %v", refused, kept)
		}
		if refused {
			continue
		}
		if _, ok := f.o.e.networks["home"]["old"]; ok || f.o.e.networks["home"]["auto"] != "p-x1" {
			t.Fatalf("networks after the balancer left: %v", f.o.e.networks)
		}
		if _, ok := f.o.e.networks["work"]; ok || len(f.o.e.netOrder) != 1 {
			t.Fatalf("a network that remembered only the gone balancer is kept: %v %v", f.o.e.networks, f.o.e.netOrder)
		}
	}
}
