package health_test

import (
	"context"
	"strings"
	"testing"

	routing_session "github.com/xtls/xray-core/features/routing/session"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
)

// The host restarts the core on the outbound that worked last, out of the set
// it kept: the core takes it from the initial answer, and starts as without
// it when the set no longer has it.
func TestStartupOutboundFromProviderSet(t *testing.T) {
	for _, tc := range []struct{ start, want string }{{"p-slot", "p-slot"}, {"p-gone", "p-old"}} {
		t.Run(tc.start, func(t *testing.T) {
			cfg, err := core.LoadConfig("json", strings.NewReader(`{"log":{"loglevel":"none"},
				"outbounds":[{"tag":"direct","protocol":"freedom"},{"tag":"drop","protocol":"blackhole"}],
				"routing":{"rules":[{"domain":["full:base.test"],"balancerTag":"auto"}],"balancers":[{"tag":"auto","fallbackTag":"drop",
				  "strategy":{"type":"leastCost","settings":{"startup":{"outbound":"`+tc.start+`"}}}}]},
				"healthObservatory":{"probes":{"profiles":[{"name":"check","steps":[{"name":"transport","destinations":["http://127.0.0.1/probe"]}]}]},"telemetry":{"events":{}},
				"providers":[{"tag":"fresh","urls":["http://127.0.0.1:1/set"],"isPlainHttpAllowed":true,"via":["direct"],
				  "triggers":{"exhausted":{"isEnabled":false},"leaseRenewal":{"isEnabled":false}},
				  "apply":{"balancers":["auto"],"tagPrefix":"p-","allowedProtocols":["freedom"]},
				  "initialAnswer":{"protocol":1,"version":"kept","outbounds":[
				    {"tag":"old","protocol":"freedom","selection":{"balancer":"auto","probe":"check","cost":1}},
				    {"tag":"slot","protocol":"freedom","selection":{"balancer":"auto","probe":"check","cost":2}}]}}]}}`))
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
			router := inst.GetFeature(routing.RouterType()).(routing.Router)
			ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("base.test"), 80)}})
			route, err := router.PickRoute(routing_session.AsRoutingContext(ctx))
			if err != nil || route.GetOutboundTag() != tc.want {
				t.Fatalf("route %v, %v; want %s", route, err, tc.want)
			}
		})
	}
}
