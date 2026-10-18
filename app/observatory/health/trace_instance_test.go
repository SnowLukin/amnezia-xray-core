package health_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/core"
	"golang.org/x/net/proxy"
)

// Real user connections through an observed outbound end up in the trace
// with their timings and outcome, and without where they went.
func TestTraceRecordsRealConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	socksPort := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	cfg, err := core.LoadConfig("json", strings.NewReader(fmt.Sprintf(`{
	  "log":{"loglevel":"none"},
	  "inbounds":[{"listen":"127.0.0.1","port":%d,"protocol":"socks"}],
	  "outbounds":[{"tag":"direct","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]}},
	    {"tag":"a1","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]},"selection":{"balancer":"auto","probe":"check","cost":1}}],
	  "routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{}},"fallbackTag":"direct"}],"rules":[{"type":"field","network":"tcp","balancerTag":"auto"}]},
	  "healthObservatory":{"probes":{"profiles":[{"name":"check","steps":[{"name":"transport","destinations":["http://127.0.0.1/probe"]}]}]},"telemetry":{"trace":{}}}
	}`, socksPort)))
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
	o := health.FromInstance(instance)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "works") }))
	defer target.Close()
	dialer, _ := proxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort), nil, &net.Dialer{Timeout: time.Second})
	transport := &http.Transport{DialContext: dialer.(proxy.ContextDialer).DialContext, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	resp, err := client.Get(target.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var conn map[string]any
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && conn == nil; time.Sleep(20 * time.Millisecond) {
		d, err := o.ExportTrace(0)
		if err != nil {
			t.Fatal(err)
		}
		_, port, _ := net.SplitHostPort(strings.TrimPrefix(target.URL, "http://"))
		if strings.Contains(string(d.Records), "127.0.0.1") || strings.Contains(string(d.Records), ":"+port) {
			t.Fatalf("destination leaked: %s", d.Records)
		}
		var recs []map[string]any
		_ = json.Unmarshal(d.Records, &recs)
		for _, r := range recs {
			if r["type"] == "conn" {
				conn = r
			}
		}
	}
	if conn == nil || conn["path"] != "a1" || conn["outcome"] != "ok" || conn["net"] != "tcp" || conn["down"] != "<1k" ||
		!strings.HasPrefix(conn["target"].(string), "h:") || conn["port"] != "other" {
		t.Fatalf("connection record %+v", conn)
	}
}
