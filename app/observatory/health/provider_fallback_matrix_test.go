package health_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/core"
	"golang.org/x/net/proxy"
)

func TestProviderFailuresKeepBaseUserRoute(t *testing.T) {
	for _, mode := range []string{"absent", "unreachable", "500", "invalid", "slow", "huge"} {
		t.Run(mode, func(t *testing.T) {
			var requests atomic.Int64
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch mode {
				case "500":
					w.WriteHeader(http.StatusInternalServerError)
				case "invalid":
					_, _ = io.WriteString(w, `{"outbounds": [`)
				case "slow":
					select {
					case <-r.Context().Done():
					case <-release:
					}
				case "huge":
					_, _ = io.WriteString(w, strings.Repeat("x", 64<<10))
				default:
					t.Errorf("unexpected provider request in %s mode", mode)
				}
			}))
			defer server.Close()
			defer close(release)
			url := server.URL
			if mode == "unreachable" {
				server.Close()
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			socksPort := listener.Addr().(*net.TCPAddr).Port
			_ = listener.Close()
			providers := "[]"
			if mode != "absent" {
				providers = fmt.Sprintf(`[{"tag":"fresh","urls":[%q],"isPlainHttpAllowed":true,"via":["direct"],"timeout":"100ms",
				  "triggers":{"exhausted":{"isEnabled":false},"leaseRenewal":{},"backendSchedule":{"isEnabled":false}},
				  "limits":{"minInterval":"10ms","urgentMinInterval":"10ms","maxRefreshesPerHour":2},
				  "apply":{"balancers":["auto"],"tagPrefix":"p-","allowedProtocols":["freedom"],"maxOutbounds":1}}]`, url)
			}
			document := fmt.Sprintf(`{
			  "log":{"loglevel":"none"},
			  "inbounds":[{"listen":"127.0.0.1","port":%d,"protocol":"socks"}],
			  "outbounds":[{"tag":"direct","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]}},
			    {"tag":"p-base","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]},"selection":{"balancer":"auto","probe":"check","cost":1}}],
			  "routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{}},"fallbackTag":"direct"}],"rules":[{"type":"field","network":"tcp","balancerTag":"auto"}]},
			  "healthObservatory":{"probes":{"profiles":[{"name":"check","steps":[{"name":"transport","destinations":["http://127.0.0.1/probe"]}]}]},"telemetry":{"events":{}},"providers":%s}
			}`, socksPort, providers)
			cfg, err := core.LoadConfig("json", strings.NewReader(document))
			if err != nil {
				t.Fatal(err)
			}
			instance, err := core.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := instance.Start(); err != nil {
				t.Fatal(err)
			}
			defer instance.Close()
			observer := health.FromInstance(instance)
			if observer == nil {
				t.Fatal("no observer")
			}
			// A legacy host may report a change without network labels and
			// never call UserPresent or collect reports.
			observer.NetworkChanged("")
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "base route works") }))
			defer target.Close()
			dialer, err := proxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort), nil, &net.Dialer{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			transport := &http.Transport{DialContext: dialer.(proxy.ContextDialer).DialContext, DisableKeepAlives: true}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: time.Second}
			assertUserRoute := func() {
				t.Helper()
				resp, err := client.Get(target.URL)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if err != nil || string(body) != "base route works" {
					t.Fatalf("user route body %q, error %v", body, err)
				}
			}
			assertUserRoute()
			accepted := observer.RequestProviderRefresh("fresh")
			if accepted != (mode != "absent") {
				t.Fatalf("refresh accepted = %v in %s mode", accepted, mode)
			}
			assertUserRoute()
			if mode != "absent" {
				waitRefreshed(t, observer)
				for range 50 {
					observer.RequestProviderRefresh("fresh")
				}
			}
			assertUserRoute()
			var status struct {
				Providers []struct{ Members, Refreshes, Failures int } `json:"providers"`
				Balancers []struct {
					Active string `json:"active"`
				} `json:"balancers"`
			}
			if err := json.Unmarshal(observer.Status(), &status); err != nil {
				t.Fatal(err)
			}
			if len(status.Balancers) != 1 || status.Balancers[0].Active != "p-base" {
				t.Fatalf("base route changed: %s", observer.Status())
			}
			if mode != "absent" && (len(status.Providers) != 1 || status.Providers[0].Members != 1 || status.Providers[0].Failures != 1 || status.Providers[0].Refreshes != 1) {
				t.Fatalf("failed provider did not keep bounded base state: %s", observer.Status())
			}
			wantRequests := int64(1)
			if mode == "absent" || mode == "unreachable" {
				wantRequests = 0
			}
			if got := requests.Load(); got != wantRequests {
				t.Fatalf("provider requests = %d, want %d", got, wantRequests)
			}
		})
	}
}
