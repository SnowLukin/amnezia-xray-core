package health

import (
	"encoding/json"
	"fmt"
	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"strings"
	"testing"
	"time"
)

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

func TestAnswerKeysOfProtocol1(t *testing.T) {
	for _, parse := range []func([]byte) (*providerAnswer, error){parseAnswer, parseInitial} {
		for _, key := range []string{"routing", "dns", "inbounds", "healthObservatory", "api", "policy", "transport", "log", "metrics", "stats", "reverse", "fakeDns", "observatory", "burstObservatory", "geodata"} {
			_, err := parse([]byte(`{"protocol":1,"outbounds":[{"tag":"x","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":1}}],"` + key + `":{}}`))
			if err == nil || !isAnswerError(err) {
				t.Fatalf("accepted forbidden %s", key)
			}
		}
	}
}
func TestProviderRoutingRejectionKeepsWholePreviousSet(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	original := f.m.GetHandler("p-x1")
	saved, seq := f.providerSet(t, 0)
	var builds int
	f.o.parseOutbounds = func(json.RawMessage) ([]*core.OutboundHandlerConfig, error) { builds++; return nil, nil }
	_, err := parseAnswer([]byte(`{"protocol":1,"version":"rejected","outbounds":[{"tag":"x2","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":1}}],"routing":{"rules":[]}}`))
	if err == nil || !strings.Contains(err.Error(), "routing") {
		t.Fatalf("routing refusal: %v", err)
	}
	again, next := f.providerSet(t, 0)
	if builds != 0 || original != f.m.GetHandler("p-x1") || f.m.GetHandler("p-x2") != nil || seq != next || fmt.Sprint(saved) != fmt.Sprint(again) {
		t.Fatal("routing refusal changed previous working set")
	}
}
