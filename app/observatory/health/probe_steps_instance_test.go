package health_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	_ "github.com/xtls/xray-core/main/distro/all"
)

func TestJSONProbeStepsThroughRequestProbe(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mandatory  int
		wantResult string
		wantState  string
		wantPaths  string
	}{
		{name: "mandatory failure stops", mandatory: http.StatusServiceUnavailable, wantResult: "failed", wantState: "down", wantPaths: "/transport"},
		{name: "optional failure continues", mandatory: http.StatusNoContent, wantResult: "ok", wantState: "degraded", wantPaths: "/transport,/egress,/egress_ip"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var paths []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				paths = append(paths, r.URL.Path)
				mu.Unlock()
				switch r.URL.Path {
				case "/transport":
					if tc.mandatory != http.StatusNoContent {
						w.WriteHeader(tc.mandatory)
						return
					}
					w.Write([]byte(strings.Repeat("t", 4096)))
				case "/egress":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "/egress_ip":
					w.Write([]byte(strings.Repeat("i", 4096)))
				}
			}))
			defer srv.Close()
			config := fmt.Sprintf(`{
				"log":{"loglevel":"none"},
				"outbounds":[
					{"tag":"drop","protocol":"blackhole"},
					{"tag":"a1","protocol":"freedom","selection":{"balancer":"auto","cost":1,"probe":"full"}}
				],
				"routing":{"rules":[{"domain":["full:a.test"],"balancerTag":"auto"}],
					"balancers":[{"tag":"auto","fallbackTag":"drop","strategy":{"type":"leastCost","settings":{}}}]},
				"healthObservatory":{"probes":{"requests":{"isEnabled":true},"stepProfiles":{"isEnabled":true},"profiles":[{"name":"full","steps":[
					{"name":"transport","destinations":[%q],"minBytes":4096,"timeout":"2s","progressTimeout":"500ms"},
					{"name":"egress","isOptional":true,"destinations":[%q],"timeout":"2s","progressTimeout":"500ms"},
					{"name":"egress_ip","isOptional":true,"destinations":[%q],"minBytes":4096,"timeout":"2s","progressTimeout":"500ms"}
				]}]}}
			}`, srv.URL+"/transport", srv.URL+"/egress", srv.URL+"/egress_ip")
			cfg, err := core.LoadConfig("json", strings.NewReader(config))
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
			r := inst.GetFeature(routing.RouterType()).(routing.Router)
			ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
				Target: net.TCPDestination(net.DomainAddress("a.test"), 80),
			}})
			if route, err := r.PickRoute(routing_session.AsRoutingContext(ctx)); err != nil || route.GetOutboundTag() != "a1" {
				t.Fatalf("initial route %v, %v", route, err)
			}
			ticket, err := obs.RequestProbe(health.ProbeRequest{Balancer: "auto", Outbounds: []string{"a1"}})
			if err != nil {
				t.Fatal(err)
			}
			var view probeView
			for deadline := time.Now().Add(5 * time.Second); !view.IsDone && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
				raw, _, _ := obs.ProbeResult(ticket.Run, ticket.ID)
				if err := json.Unmarshal(raw, &view); err != nil {
					t.Fatal(err)
				}
			}
			var status struct {
				Paths []struct {
					Outbound   string `json:"outbound"`
					State      string `json:"state"`
					ProbeBytes int64  `json:"probe_bytes"`
				} `json:"paths"`
			}
			if err := json.Unmarshal(obs.Status(), &status); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			gotPaths := strings.Join(paths, ",")
			mu.Unlock()
			if !view.IsDone || len(view.Outbounds) != 1 || view.Outbounds[0].Result != tc.wantResult || gotPaths != tc.wantPaths {
				t.Fatalf("request %+v, paths %s, want %s", view, gotPaths, tc.wantPaths)
			}
			for _, path := range status.Paths {
				if path.Outbound == "a1" {
					if path.State != tc.wantState {
						t.Fatalf("state %s, want %s", path.State, tc.wantState)
					}
					if tc.mandatory == http.StatusNoContent && path.ProbeBytes < 8192 {
						t.Fatalf("probe bytes %d, want traffic from both successful steps", path.ProbeBytes)
					}
					return
				}
			}
			t.Fatal("a1 missing from status")
		})
	}
}
