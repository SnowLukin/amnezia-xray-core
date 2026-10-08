package health_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
)

func TestProviderRoutingAppliesInRunningCore(t *testing.T) {
	srv := &setServer{}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	cfg, err := core.LoadConfig("json", strings.NewReader(`{
	  "log": {"loglevel": "none"},
	  "outbounds": [
	    {"tag": "block", "protocol": "blackhole"},
	    {"tag": "direct", "protocol": "freedom"}
	  ],
	  "routing": {"rules": [{"domain": ["full:a.test"], "balancerTag": "auto"}],
	    "balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost", "settings": {}}, "fallbackTag": "block"}]},
	  "healthObservatory": {
	    "subjectSelector": ["p-"],
	    "providers": [{
	      "tag": "fresh", "url": "`+ts.URL+`/set", "allowPlainHttp": true, "via": ["direct"],
	      "triggers": {"interval": "0s", "minHealthy": 0, "whenExhausted": false, "maxAge": "0s", "onNetworkChange": "never"},
	      "limits": {"exhaustedMinInterval": "10ms", "backoff": {"initial": "50ms"}},
	      "apply": {"balancer": "auto", "tagPrefix": "p-", "protocols": ["freedom"], "routing": true}
	    }]
	  }
	}`))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	obs := health.FromInstance(inst)
	r := inst.GetFeature(routing.RouterType()).(routing.Router)
	pick := func(domain string) string {
		ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
			Target: net.TCPDestination(net.DomainAddress(domain), 80),
		}})
		route, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
		if err != nil {
			return "error: " + err.Error()
		}
		return route.GetOutboundTag()
	}
	w := &eventWaiter{t: t, o: obs}

	srv.set(`{"outbounds": [{"tag": "x1", "protocol": "freedom"}],
	  "routing": {"rules": [{"domain": ["full:blocked.test"], "outboundTag": "block"},
	                        {"domain": ["full:a.test"], "balancerTag": "auto"}],
	    "balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost", "settings": {}}, "fallbackTag": "block"}]},
	  "dns": {"servers": ["1.1.1.1"]}}`)
	obs.RequestRefresh("fresh")
	ev := w.until("provider_apply", nil)
	if ev.Fields["ok"] != true || ev.Fields["rules"] != 2.0 {
		t.Fatalf("apply %v", ev.Fields)
	}
	if got := pick("blocked.test"); got != "block" {
		t.Fatalf("a rule from the provider routes blocked.test to %q, want block", got)
	}
	if got := pick("a.test"); got != "p-x1" {
		t.Fatalf("the balancer rebuilt from the answer routes a.test to %q, want p-x1", got)
	}
	restart := ""
	for _, e := range w.all {
		if e.Type == "provider_restart_needed" {
			restart = strings.Join(toStrings(e.Fields["sections"]), ",")
		}
	}
	if restart != "dns" {
		t.Fatalf("restart needed for %q, want dns", restart)
	}

	srv.set(`{"outbounds": [{"tag": "x1", "protocol": "freedom"}],
	  "routing": {"rules": [{"domain": ["full:a.test"], "outboundTag": "nowhere"}],
	    "balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost", "settings": {}}}]}}`)
	obs.RequestRefresh("fresh")
	ev = w.until("provider_apply", nil)
	if ev.Fields["ok"] != false || pick("blocked.test") != "block" {
		t.Fatalf("a routing to an unknown outbound: apply %v, blocked.test now goes to %q", ev.Fields, pick("blocked.test"))
	}
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		s, _ := it.(string)
		out = append(out, s)
	}
	return out
}
