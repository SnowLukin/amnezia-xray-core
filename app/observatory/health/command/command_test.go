package command_test

import (
	"context"
	"encoding/json"
	gonet "net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/app/observatory/health/command"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	_ "github.com/xtls/xray-core/main/distro/all"
)

func service(t *testing.T, config string) (command.HealthServiceServer, *core.Instance) {
	t.Helper()
	cfg, err := core.LoadConfig("json", strings.NewReader(config))
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
	t.Cleanup(func() { inst.Close() })
	s, err := core.CreateObject(inst, &command.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return s.(command.HealthServiceServer), inst
}

func TestGetReport(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"outbounds": [{"tag": "x1", "protocol": "freedom"}]}`))
	}))
	defer ts.Close()
	s, inst := service(t, `{
	  "log": {"loglevel": "none"},
	  "outbounds": [{"tag": "direct", "protocol": "freedom"}, {"tag": "block", "protocol": "blackhole"}],
	  "routing": {"balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost"}, "fallbackTag": "block"}]},
	  "healthObservatory": {"subjectSelector": ["p-"], "providers": [{
	    "tag": "fresh", "url": "`+ts.URL+`", "via": ["direct"], "allowPlainHttp": true,
	    "triggers": {"interval": "0s", "minHealthy": 0, "whenExhausted": false, "maxAge": "0s", "onNetworkChange": "never"},
	    "apply": {"balancer": "auto", "tagPrefix": "p-", "protocols": ["freedom"]}}]}
	}`)
	ctx := context.Background()
	get := func(r *command.GetReportRequest) map[string]any {
		t.Helper()
		resp, err := s.GetReport(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Json == "" {
			return nil
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(resp.Json), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if r := get(&command.GetReportRequest{}); r != nil {
		t.Fatalf("snapshot before any decision: %v", r)
	}
	// A provider set applied is a decision.
	health.FromInstance(inst).RequestRefresh("fresh")
	var snap map[string]any
	for deadline := time.Now().Add(5 * time.Second); snap == nil && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
		snap = get(&command.GetReportRequest{})
	}
	if snap == nil || snap["report_reason"] != "snapshot" || snap["provider_applied"] != 1.0 {
		t.Fatalf("snapshot %v", snap)
	}
	if r := get(&command.GetReportRequest{Take: true}); r != nil {
		t.Fatalf("a report is due before the interval: %v", r)
	}
	final := get(&command.GetReportRequest{Final: true})
	if final == nil || final["report_reason"] != "stop" || final["provider_applied"] != 1.0 {
		t.Fatalf("final report %v", final)
	}
	if r := get(&command.GetReportRequest{Take: true, Final: true}); r != nil {
		t.Fatalf("a second final report: %v", r)
	}
}

func TestProbe(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	// b1 reaches a server that accepts and never answers.
	silent, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	go func() {
		var held []gonet.Conn
		for {
			c, err := silent.Accept()
			if err != nil {
				for _, c := range held {
					c.Close()
				}
				return
			}
			held = append(held, c)
		}
	}()
	s, inst := service(t, `{
	  "log": {"loglevel": "none"},
	  "outbounds": [{"tag": "drop", "protocol": "blackhole"}, {"tag": "a1", "protocol": "freedom"},
	    {"tag": "b1", "protocol": "freedom", "settings": {"redirect": "`+silent.Addr().String()+`"}}],
	  "routing": {"rules": [{"domain": ["full:a.test"], "balancerTag": "auto"}],
	    "balancers": [{"tag": "auto", "selector": ["a", "b"], "strategy": {"type": "leastCost"}, "fallbackTag": "drop"}]},
	  "healthObservatory": {"subjectSelector": ["a", "b"], "schedule": {"spacing": "10ms"},
	    "probes": [{"destination": "`+ts.URL+`/204", "timeout": "3s", "progressTimeout": "1s"}]}
	}`)
	r := inst.GetFeature(routing.RouterType()).(routing.Router)
	if _, err := r.PickRoute(routing_session.AsRoutingContext(session.ContextWithOutbounds(context.Background(),
		[]*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("a.test"), 80)}}))); err != nil {
		t.Fatal(err)
	}
	type result struct {
		ID        uint64 `json:"id"`
		Done      bool   `json:"done"`
		Outbounds []struct {
			Result string `json:"result"`
		} `json:"outbounds"`
	}
	probe := func(ctx context.Context, req *command.ProbeRequest) (*command.ProbeResponse, result, time.Duration) {
		t.Helper()
		start := time.Now()
		resp, err := s.Probe(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		var out result
		if err := json.Unmarshal([]byte(resp.Json), &out); err != nil {
			t.Fatal(err)
		}
		return resp, out, time.Since(start)
	}
	ctx := context.Background()
	// The probe of b1 fails after a second without progress.
	resp, res, took := probe(ctx, &command.ProbeRequest{Balancer: "auto", Outbounds: []string{"b1"}})
	if resp.Id != 1 || resp.Run == "" || res.Done || took > 100*time.Millisecond {
		t.Fatalf("without a wait: %+v %+v after %v", resp, res, took)
	}
	short, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
	resp, res, took = probe(short, &command.ProbeRequest{Balancer: "auto", Outbounds: []string{"b1"}, WaitMs: 10_000})
	cancel()
	if resp.Id != 1 || res.Done || took >= 600*time.Millisecond {
		t.Fatalf("a wait cut by the deadline: %+v %+v after %v", resp, res, took)
	}
	resp, res, _ = probe(ctx, &command.ProbeRequest{Balancer: "auto", Outbounds: []string{"b1"}, WaitMs: 10_000})
	if resp.Id != 1 || !res.Done || res.Outbounds[0].Result != "failed" {
		t.Fatalf("with a wait: %+v %+v", resp, res)
	}
	resp, res, _ = probe(ctx, &command.ProbeRequest{Balancer: "auto", WaitMs: 10_000})
	if resp.Id != 2 || !res.Done || len(res.Outbounds) != 2 || res.Outbounds[0].Result != "ok" || res.Outbounds[1].Result != "down" {
		t.Fatalf("a request for the balancer: %+v %+v", resp, res)
	}
	if _, err := s.Probe(ctx, &command.ProbeRequest{Balancer: "auto", Outbounds: []string{"x1"}}); err == nil {
		t.Fatal("a probe of an outbound the balancer does not have")
	}
}

func TestProbeWithoutObservatory(t *testing.T) {
	s, _ := service(t, `{"log": {"loglevel": "none"}, "outbounds": [{"tag": "direct", "protocol": "freedom"}]}`)
	if _, err := s.Probe(context.Background(), &command.ProbeRequest{Balancer: "auto"}); err == nil {
		t.Fatal("a probe without a health observatory")
	}
}

func TestGetReportWithoutObservatory(t *testing.T) {
	s, _ := service(t, `{"log": {"loglevel": "none"}, "outbounds": [{"tag": "direct", "protocol": "freedom"}]}`)
	if _, err := s.GetReport(context.Background(), &command.GetReportRequest{}); err == nil {
		t.Fatal("a report without a health observatory")
	}
}

func TestSetProviderHeaders(t *testing.T) {
	s, inst := service(t, `{
	  "log": {"loglevel": "none"},
	  "outbounds": [{"tag": "direct", "protocol": "freedom"}, {"tag": "block", "protocol": "blackhole"}],
	  "routing": {"balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost"}, "fallbackTag": "block"}]},
	  "healthObservatory": {"subjectSelector": ["p-"], "providers": [{
	    "tag": "fresh", "url": "https://provider.test/set", "headers": {"Authorization": "Bearer config"},
	    "triggers": {"interval": "0s", "minHealthy": 0, "whenExhausted": false, "maxAge": "0s", "onNetworkChange": "never"},
	    "apply": {"balancer": "auto", "tagPrefix": "p-"}}]}
	}`)
	ctx := context.Background()
	if _, err := s.SetProviderHeaders(ctx, &command.SetProviderHeadersRequest{Provider: "fresh",
		Headers: map[string]string{"authorization": "Bearer host-secret"}}); err != nil {
		t.Fatal(err)
	}
	var st struct {
		Providers []struct {
			Headers []string `json:"headers"`
		} `json:"providers"`
	}
	status := health.FromInstance(inst).Status()
	if err := json.Unmarshal(status, &st); err != nil || len(st.Providers) != 1 || strings.Join(st.Providers[0].Headers, ",") != "Authorization" {
		t.Fatalf("status %s: %v", status, err)
	}
	if strings.Contains(string(status), "host-secret") {
		t.Fatal("the status shows the value")
	}
	for name, req := range map[string]*command.SetProviderHeadersRequest{
		"no such provider": {Provider: "other", Headers: map[string]string{"Authorization": "Bearer host-secret"}},
		"bad name":         {Provider: "fresh", Headers: map[string]string{"Host": "provider.test"}},
		"bad value":        {Provider: "fresh", Headers: map[string]string{"Authorization": "Bearer host-secret\r\n"}},
	} {
		_, err := s.SetProviderHeaders(ctx, req)
		if err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.Contains(err.Error(), "host-secret") {
			t.Errorf("%s: the error quotes the value: %v", name, err)
		}
	}
}

func TestSetProviderHeadersWithoutObservatory(t *testing.T) {
	s, _ := service(t, `{"log": {"loglevel": "none"}, "outbounds": [{"tag": "direct", "protocol": "freedom"}]}`)
	if _, err := s.SetProviderHeaders(context.Background(), &command.SetProviderHeadersRequest{Provider: "fresh"}); err == nil {
		t.Fatal("headers without a health observatory")
	}
}
