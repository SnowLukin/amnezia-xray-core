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
	  "dns": {"hosts": {"lan.test": "10.1.2.3"}},
	  "outbounds": [
	    {"tag": "block", "protocol": "blackhole"},
	    {"tag": "direct", "protocol": "freedom"}
	  ],
	  "routing": {"rules": [{"domain": ["full:a.test"], "balancerTag": "auto"}],
	    "balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost", "settings": {}}, "fallbackTag": "block"}]},
	  "healthObservatory": {
	    "subjectSelector": ["p-"],
	    "providers": [{
	      "tag": "fresh", "url": "`+ts.URL+`/set", "isPlainHttpAllowed": true, "via": ["direct"],
	      "triggers": {"interval": "0s", "minHealthyOutbounds": 0, "shouldRefreshWhenExhausted": false, "maxAge": "0s", "networkChangeMode": "never"},
	      "limits": {"exhaustedMinInterval": "10ms", "backoff": {"initialDelay": "50ms"}},
	      "apply": {"balancer": "auto", "tagPrefix": "p-", "allowedProtocols": ["freedom"], "isRoutingAccepted": true}
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
	if got := pick("lan.test"); got == "block" {
		t.Fatal("setup: lan.test matches an IP rule before the answer")
	}

	routingWith := func(strategy string) string {
		return `"routing": {"domainStrategy": "` + strategy + `",
	    "rules": [{"domain": ["full:blocked.test"], "outboundTag": "block"},
	              {"ip": ["10.1.2.0/24"], "outboundTag": "block"},
	              {"domain": ["full:a.test"], "balancerTag": "auto"}],
	    "balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost", "settings": {}}, "fallbackTag": "block"}]}`
	}
	srv.set(`{"protocol": 1, "outbounds": [{"tag": "x1", "protocol": "freedom"}], ` + routingWith("IPIfNonMatch") + `}`)
	obs.RequestRefresh("fresh")
	ev := w.until("provider_apply", nil)
	if ev.Fields["is_ok"] != true || ev.Fields["rules"] != 3.0 {
		t.Fatalf("apply %v", ev.Fields)
	}
	if got := pick("blocked.test"); got != "block" {
		t.Fatalf("a rule from the provider routes blocked.test to %q, want block", got)
	}
	if got := pick("a.test"); got != "p-x1" {
		t.Fatalf("the balancer rebuilt from the answer routes a.test to %q, want p-x1", got)
	}
	// The domain strategy of the answer works without a restart: the name
	// resolves into the range of an IP rule.
	if got := pick("lan.test"); got != "block" {
		t.Fatalf("with IPIfNonMatch from the answer lan.test goes to %q, want block", got)
	}

	// A section the core reads only at start refuses the whole answer.
	from := len(w.all)
	srv.set(`{"protocol": 1, "outbounds": [{"tag": "x2", "protocol": "freedom"}], ` + routingWith("AsIs") + `,
	  "dns": {"servers": ["1.1.1.1"]}}`)
	obs.RequestRefresh("fresh")
	ev = w.until("provider_apply", nil)
	if ev.Fields["is_ok"] != false || !strings.Contains(ev.Fields["error"].(string), `unknown key "dns"`) {
		t.Fatalf("an answer with dns: apply %v", ev.Fields)
	}
	if len(w.since(from, "provider_restart_needed")) != 0 {
		t.Fatal("a refused answer asked for a restart")
	}
	if got := pick("lan.test"); got != "block" || pick("a.test") != "p-x1" {
		t.Fatalf("a refused answer changed the routing: lan.test to %q, a.test to %q", got, pick("a.test"))
	}

	srv.set(`{"outbounds": [{"tag": "x1", "protocol": "freedom"}],
	  "routing": {"rules": [{"domain": ["full:a.test"], "outboundTag": "nowhere"}],
	    "balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost", "settings": {}}}]}}`)
	obs.RequestRefresh("fresh")
	// The refused answer with dns is asked for again after the short
	// backoff, and its refusal may come first: the apply of this answer is
	// the first one about something else.
	ev = w.until("provider_apply", func(e health.Event) bool {
		reason, _ := e.Fields["error"].(string)
		return !strings.Contains(reason, `"dns"`)
	})
	if reason, _ := ev.Fields["error"].(string); ev.Fields["is_ok"] != false || !strings.Contains(reason, "unknown outbound nowhere") ||
		pick("blocked.test") != "block" {
		t.Fatalf("a routing to an unknown outbound: apply %v, blocked.test now goes to %q", ev.Fields, pick("blocked.test"))
	}
}
