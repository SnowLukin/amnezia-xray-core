package health_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	gonet "net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf/serial"
)

func measurementInstance(t *testing.T, destination string) (*health.Observer, *core.Instance) {
	t.Helper()
	raw := fmt.Sprintf(`{"log":{"loglevel":"none"},"outbounds":[{"tag":"a1","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]},"selection":{"balancer":"auto","cost":1,"labels":{"node":"n1"}}},{"tag":"direct","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]}},{"tag":"block","protocol":"blackhole"}],"routing":{"rules":[{"type":"field","network":"tcp","balancerTag":"auto"}],"balancers":[{"tag":"auto","fallbackTag":"block","strategy":{"type":"leastCost","settings":{"networkChanges":{"isEnabled":true,"action":"forget"}}}}]},"healthObservatory":{"telemetry":{"events":{"isEnabled":true},"quality":{"isEnabled":true},"pathLabels":{"isEnabled":true},"report":{"isEnabled":true,"quietSampleFraction":1,"sessionLabels":{"isEnabled":true,"labels":{"cohort":"test"}}}},"network":{"hostLabels":{"isEnabled":true},"labelRequest":{"isEnabled":true,"outbound":"direct","destination":%q,"timeout":"1s","labels":{"asn":"X-Network-Asn","country":"X-Network-Country","region":"X-Network-Region"}}}}}`, destination)
	cfg, err := serial.LoadJSONConfig(strings.NewReader(raw))
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
	t.Cleanup(func() { inst.Close() })
	return health.FromInstance(inst), inst
}

func measurementTraffic(t *testing.T, inst *core.Instance, endpoint string) {
	t.Helper()

	tr := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (gonet.Conn, error) {
		dest, err := net.ParseDestination(network + ":" + addr)
		if err != nil {
			return nil, err
		}
		return core.Dial(ctx, inst, dest)
	}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 2 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func measurementWait(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("measurement request did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMeasurementHTTPDirectAndCache(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("X-Network-Asn", "8359")
		w.Header().Set("X-Network-Country", "RU")
		w.Header().Set("X-Network-Region", "RU-MOW")
		w.Header().Set("X-Unlisted", "203.0.113.42")
		w.WriteHeader(204)
	}))
	defer srv.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(strings.Repeat("working", 8192))) }))
	defer target.Close()
	o, inst := measurementInstance(t, srv.URL)
	o.NetworkChangedWithLabels("home", `{"type":"wifi"}`)
	time.Sleep(20 * time.Millisecond)
	if requests.Load() != 0 {
		t.Fatal("idle query")
	}
	measurementTraffic(t, inst, target.URL)
	measurementWait(t, func() bool { return strings.Contains(string(o.Status()), `"asn":"8359"`) })
	o.NetworkChangedWithLabels("mobile", `{"type":"cellular"}`)
	measurementTraffic(t, inst, target.URL)
	measurementWait(t, func() bool { return requests.Load() == 2 && strings.Contains(string(o.Status()), `"asn":"8359"`) })
	o.NetworkChangedWithLabels("home", `{"type":"wifi"}`)
	measurementTraffic(t, inst, target.URL)
	time.Sleep(25 * time.Millisecond)
	if requests.Load() != 2 {
		t.Fatal("return requested labels again", requests.Load())
	}
	status := string(o.Status())
	if strings.Contains(status, "203.0.113.42") {
		t.Fatal("unlisted header retained")
	}
	report := o.PeekReport()
	if !strings.Contains(string(report), `"network":{"host":{"type":"wifi"},"ip":{"asn":"8359"`) {
		t.Fatal("report network missing", string(report))
	}
	var value map[string]any
	if json.Unmarshal(report, &value) != nil {
		t.Fatal("invalid report")
	}
	events, _ := o.DrainEvents()
	t.Logf("requests=%d events=%s", requests.Load(), events)
	if strings.Contains(string(report), `"direct":`) {
		t.Fatal("request became quality user traffic")
	}
}

func TestMeasurementHTTPFailureClasses(t *testing.T) {
	for _, kind := range []string{"no_labels", "status_503", "invalid", "redirect"} {
		t.Run(kind, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch kind {
				case "status_503":
					w.WriteHeader(503)
				case "redirect":
					w.Header().Set("Location", "/other")
					w.WriteHeader(302)
				case "invalid":
					w.Header().Set("X-Network-Asn", "bad value")
					w.WriteHeader(200)
				default:
					w.WriteHeader(204)
				}
			}))
			defer srv.Close()
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(strings.Repeat("working", 8192))) }))
			defer target.Close()
			o, inst := measurementInstance(t, srv.URL)
			o.NetworkChanged("home")
			measurementTraffic(t, inst, target.URL)
			var all string
			measurementWait(t, func() bool {
				ev, _ := o.DrainEvents()
				all += string(ev)
				return strings.Contains(all, "network_labels")
			})
			expected := kind
			if kind == "invalid" {
				expected = "no_labels"
			}
			if kind == "redirect" {
				expected = "status_302"
			}
			if !strings.Contains(all, `"error":"`+expected+`"`) || requests.Load() != 1 || strings.Contains(string(o.Status()), `"ip":`) {
				t.Fatal("failure result", all)
			}
		})
	}
}
