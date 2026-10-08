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

func TestAnswerSectionsTheCoreCannotChangeAskForARestart(t *testing.T) {
	a, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x"}], "dns": {"servers": ["1.1.1.1"]},
		"routing": {"domainStrategy": "IPIfNonMatch", "rules": []}}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(a.restart, ",") != "dns,routing.domainStrategy" || string(a.routing) != `{"rules":[]}` {
		t.Fatalf("restart %v, routing %s", a.restart, a.routing)
	}
	b, _ := parseAnswer([]byte(`{"outbounds": [{"tag": "x"}], "dns": {"servers": ["8.8.8.8"]}}`))
	c, _ := parseAnswer([]byte(`{"outbounds": [{"tag": "x"}]}`))
	if b.version == c.version {
		t.Fatal("a set whose dns changed is named like the one without dns")
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
	own := `{"tag": "auto", "selector": ["p-"], "strategy": "leastcost"}`
	for _, tc := range []struct {
		name, routing string
		applied       bool
		err           string
	}{
		{"valid", `{"rules": [{"via": "auto"}, {"to": "p-x1"}], "balancers": [` + own + `]}`, true, ""},
		{"rule to an unknown outbound", `{"rules": [{"to": "nowhere"}], "balancers": [` + own + `]}`, false, "unknown outbound"},
		{"rule to an unknown balancer", `{"rules": [{"via": "other"}], "balancers": [` + own + `]}`, false, "unknown balancer"},
		{"balancer of the provider dropped", `{"rules": [{"to": "p-x1"}], "balancers": []}`, false, "drops"},
		{"balancer choosing what nothing observes", `{"rules": [], "balancers": [` + own + `,
			{"tag": "b", "selector": ["z"], "strategy": "leastcost"}]}`, false, "nothing observes"},
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

func TestRoutingNotAllowedAsksForARestart(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	f.o.parseRouting = testRouting
	called := false
	f.o.replaceRules = func(*router.Config, bool) error { called = true; return nil }
	if _, err := f.apply(t, routingAnswer(t, `{"rules": []}`)); err != nil {
		t.Fatal(err)
	}
	raw, _ := f.o.DrainEvents()
	if called || !strings.Contains(string(raw), `"provider_restart_needed"`) || !strings.Contains(string(raw), `"routing"`) {
		t.Fatalf("routing applied %v; events %s", called, raw)
	}
}

// Balancers the new routing does not have stop being judged and forget what
// they remembered per network.
func TestRoutingForgetsBalancersItNoLongerHas(t *testing.T) {
	own := `{"tag": "auto", "selector": ["p-"], "strategy": "leastcost"}`
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
