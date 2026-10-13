package health_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
)

type probeView struct {
	ID        uint64 `json:"id"`
	IsDone    bool   `json:"is_done"`
	End       string `json:"end"`
	Outbounds []struct {
		Outbound string `json:"outbound"`
		Result   string `json:"result"`
		Source   string `json:"source"`
	} `json:"outbounds"`
}

// A request of the host probes through the outbounds for real and returns
// at once; the result arrives in ProbeResult and in Status.
func TestRequestProbeInRunningCore(t *testing.T) {
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	cfg, err := core.LoadConfig("json", strings.NewReader(`{
	  "log": {"loglevel": "none"},
	  "outbounds": [
	    {"tag": "drop", "protocol": "blackhole"},
	    {"tag": "a1", "protocol": "freedom", "selection": {"balancer": "auto", "cost": 1, "probe": "host"}},
	    {"tag": "b1", "protocol": "blackhole", "selection": {"balancer": "auto", "cost": 3, "probe": "host"}}
	  ],
	  "routing": {"rules": [{"domain": ["full:a.test"], "balancerTag": "auto"}],
	    "balancers": [{"tag": "auto", "fallbackTag": "drop",
	      "strategy": {"type": "leastCost", "settings": {}}}]},
	  "healthObservatory": {
	    "probes": {"requests":{},"profiles":[{"name":"host","steps":[{"name":"transport","destinations":["`+ts.URL+`/204"],"timeout":"2s","progressTimeout":"500ms"}]}]}
	  }
	}`))
	if err != nil {
		t.Fatal(err)
	}
	tuneInstanceProbeSpacing(t, cfg)
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := health.TuneInstanceScenario(inst, cfg); err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	obs := health.FromInstance(inst)

	if _, err := obs.RequestProbe(health.ProbeRequest{Balancer: "auto"}); err == nil || !strings.Contains(err.Error(), "no candidates yet") {
		t.Fatalf("request before any connection: %v", err)
	}
	r := inst.GetFeature(routing.RouterType()).(routing.Router)
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
		Target: net.TCPDestination(net.DomainAddress("a.test"), 80),
	}})
	if route, err := r.PickRoute(routing_session.AsRoutingContext(ctx)); err != nil || route.GetOutboundTag() != "a1" {
		t.Fatalf("route %v, %v", route, err)
	}

	ticket, err := obs.RequestProbe(health.ProbeRequest{Balancer: "auto"})
	if err != nil || ticket.ID != 1 || strings.Join(ticket.Outbounds, ",") != "a1,b1" {
		t.Fatalf("ticket %+v, %v", ticket, err)
	}
	var status struct {
		Run           string      `json:"run"`
		ProbeRequests []probeView `json:"probe_requests"`
	}
	if err := json.Unmarshal(obs.Status(), &status); err != nil || status.Run != ticket.Run {
		t.Fatalf("status run %q, ticket run %q, %v", status.Run, ticket.Run, err)
	}
	var v probeView
	for deadline := time.Now().Add(10 * time.Second); !v.IsDone && time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		raw, done, _ := obs.ProbeResult(ticket.Run, ticket.ID)
		if err := json.Unmarshal(raw, &v); err != nil || v.IsDone != done {
			t.Fatalf("result %s, done %v, %v", raw, done, err)
		}
	}
	if !v.IsDone || v.End != "done" || len(v.Outbounds) != 2 ||
		v.Outbounds[0].Result != "ok" || v.Outbounds[0].Source != "probe" ||
		v.Outbounds[1].Result != "failed" || v.Outbounds[1].Source != "probe" {
		t.Fatalf("request %+v", v)
	}
	if hits.Load() != 1 {
		t.Fatalf("the probe destination saw %d requests, want the probe of a1", hits.Load())
	}
	if err := json.Unmarshal(obs.Status(), &status); err != nil || len(status.ProbeRequests) != 1 || !status.ProbeRequests[0].IsDone {
		t.Fatalf("status probe_requests %+v, %v", status.ProbeRequests, err)
	}
	if raw, _, _ := obs.ProbeResult(ticket.Run, ticket.ID+1); raw != nil {
		t.Fatalf("result of a request never made: %s", raw)
	}
}
