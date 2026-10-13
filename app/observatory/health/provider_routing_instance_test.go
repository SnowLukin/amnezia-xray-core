package health_test

import (
	"context"
	"encoding/json"
	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProviderRoutingRejectedInRunningCore(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"protocol":1,"version":"rejected","outbounds":[{"tag":"new","protocol":"freedom","selection":{"balancer":"auto","cost":1}}],"routing":{"rules":[]}}`))
	}))
	defer ts.Close()
	cfg, err := core.LoadConfig("json", strings.NewReader(`{"log":{"loglevel":"none"},"outbounds":[{"tag":"direct","protocol":"freedom"},{"tag":"drop","protocol":"blackhole"}],"routing":{"rules":[{"domain":["full:base.test"],"balancerTag":"auto"}],"balancers":[{"tag":"auto","strategy":{"type":"leastCost"},"fallbackTag":"drop"}]},"healthObservatory":{"telemetry":{"events":{"isEnabled":true}},"providers":[{"isEnabled":true,"tag":"fresh","url":"`+ts.URL+`","isPlainHttpAllowed":true,"via":["direct"],"triggers":{"exhausted":{"isEnabled":false},"leaseRenewal":{"isEnabled":false}},"apply":{"balancer":"auto","tagPrefix":"p-","allowedProtocols":["freedom"]},"initialAnswer":{"protocol":1,"version":"working","outbounds":[{"tag":"old","protocol":"freedom","selection":{"balancer":"auto","cost":2}}]}}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = inst.Start(); err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	o := health.FromInstance(inst)
	before, seq, err := o.ExportProviderSet("fresh", 0)
	if err != nil {
		t.Fatal(err)
	}
	manager := inst.GetFeature(outbound.ManagerType()).(outbound.Manager)
	old := manager.GetHandler("p-old")
	router := inst.GetFeature(routing.RouterType()).(routing.Router)
	route, err := router.PickRoute(routing_session.AsRoutingContext(session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("base.test"), 80)}})))
	if err != nil || route.GetOutboundTag() != "p-old" {
		t.Fatalf("route %v,%v", route, err)
	}
	o.RequestProviderRefresh("fresh")
	w := &eventWaiter{t: t, o: o}
	w.until("provider_apply", func(ev health.Event) bool { return ev.Fields["is_ok"] == false })
	after, next, err := o.ExportProviderSet("fresh", 0)
	if err != nil || seq != next || string(before) != string(after) || old != manager.GetHandler("p-old") || manager.GetHandler("p-new") != nil {
		t.Fatal("routing refusal changed working set")
	}
	var status struct {
		Providers []struct {
			LastError string `json:"last_error"`
		} `json:"providers"`
	}
	if json.Unmarshal(o.Status(), &status) != nil || !strings.Contains(status.Providers[0].LastError, "routing") {
		t.Fatal(string(o.Status()))
	}
}
