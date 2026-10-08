package command_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/app/observatory/health/command"
	"github.com/xtls/xray-core/core"
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

func TestGetReportWithoutObservatory(t *testing.T) {
	s, _ := service(t, `{"log": {"loglevel": "none"}, "outbounds": [{"tag": "direct", "protocol": "freedom"}]}`)
	if _, err := s.GetReport(context.Background(), &command.GetReportRequest{}); err == nil {
		t.Fatal("a report without a health observatory")
	}
}
