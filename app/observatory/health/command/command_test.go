package command_test

import (
	"context"
	"encoding/json"
	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/app/observatory/health/command"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	_ "github.com/xtls/xray-core/main/distro/all"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
	if err = inst.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inst.Close() })
	s, err := core.CreateObject(inst, &command.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return s.(command.HealthServiceServer), inst
}
func requireCode(t *testing.T, r *command.Result, err error, code string) {
	t.Helper()
	if err != nil || r == nil || r.Success || r.Error == nil || r.Error.Code != code {
		t.Fatalf("result %+v error %v, want %s", r, err, code)
	}
}
func decodeData(t *testing.T, r *command.Result, err error, out any) {
	t.Helper()
	if err != nil || !r.Success {
		t.Fatalf("result %+v error %v", r, err)
	}
	if out != nil && json.Unmarshal([]byte(r.Data), out) != nil {
		t.Fatalf("invalid data %s", r.Data)
	}
}

func TestExplicitCommandMethods(t *testing.T) {
	descriptor := command.File_app_observatory_health_command_command_proto.Services().ByName("HealthService")
	for _, name := range []string{"GetStatus", "GetReport", "Probe", "Refresh", "GetProviderSet"} {
		if descriptor.Methods().ByName(protoreflect.Name(name)) != nil {
			t.Fatalf("legacy alias %s remains", name)
		}
	}
	for _, name := range []string{"Status", "PeekReport", "TakeReport", "Release", "NetworkLost", "SetMemoryLow", "ProbeResult", "RequestProbe", "RequestProviderRefresh", "ExportProviderSet"} {
		if descriptor.Methods().ByName(protoreflect.Name(name)) == nil {
			t.Fatalf("missing method %s", name)
		}
	}
}
func TestJSONCommandsWithoutObservatory(t *testing.T) {
	s, _ := service(t, `{"log":{"loglevel":"none"},"outbounds":[{"tag":"direct","protocol":"freedom"}]}`)
	ctx := context.Background()
	calls := []func() (*command.Result, error){
		func() (*command.Result, error) { return s.Status(ctx, &command.StatusRequest{}) },
		func() (*command.Result, error) { return s.DrainEvents(ctx, &command.DrainEventsRequest{}) },
		func() (*command.Result, error) { return s.PeekReport(ctx, &command.PeekReportRequest{}) },
		func() (*command.Result, error) { return s.TakeReport(ctx, &command.TakeReportRequest{IsFinal: true}) },
		func() (*command.Result, error) { return s.RequestProbe(ctx, &command.RequestProbeRequest{}) },
		func() (*command.Result, error) {
			return s.ProbeResult(ctx, &command.ProbeResultRequest{Run: "old", Id: 1})
		},
		func() (*command.Result, error) { return s.ExportProviderSet(ctx, &command.ExportProviderSetRequest{}) },
		func() (*command.Result, error) {
			return s.SetProviderHeaders(ctx, &command.SetProviderHeadersRequest{})
		},
	}
	for _, call := range calls {
		r, err := call()
		requireCode(t, r, err, "not_running")
	}
}
func TestDisabledCommandsAndLocalStatus(t *testing.T) {
	s, _ := service(t, `{"log":{"loglevel":"none"},"outbounds":[{"tag":"direct","protocol":"freedom"}],"healthObservatory":{}}`)
	ctx := context.Background()
	calls := []func() (*command.Result, error){
		func() (*command.Result, error) { return s.DrainEvents(ctx, &command.DrainEventsRequest{}) },
		func() (*command.Result, error) { return s.PeekReport(ctx, &command.PeekReportRequest{}) },
		func() (*command.Result, error) { return s.TakeReport(ctx, &command.TakeReportRequest{IsFinal: true}) },
		func() (*command.Result, error) { return s.RequestProbe(ctx, &command.RequestProbeRequest{}) },
		func() (*command.Result, error) { return s.UserPresent(ctx, &command.UserPresentRequest{}) },
		func() (*command.Result, error) { return s.SetMemoryLow(ctx, &command.SetMemoryLowRequest{IsLow: true}) },
		func() (*command.Result, error) {
			return s.Pin(ctx, &command.PinRequest{Balancer: "auto", Outbound: "x"})
		},
		func() (*command.Result, error) {
			return s.RequestProviderRefresh(ctx, &command.RequestProviderRefreshRequest{})
		},
	}
	for _, call := range calls {
		r, err := call()
		requireCode(t, r, err, "feature_disabled")
	}
	var status struct {
		IsLocalOnly bool            `json:"is_local_only"`
		Features    map[string]bool `json:"features"`
	}
	r, err := s.Status(ctx, &command.StatusRequest{})
	decodeData(t, r, err, &status)
	if !status.IsLocalOnly || len(status.Features) < 29 || status.Features["quality"] || !status.Features["dial_failures"] {
		t.Fatalf("status %+v", status)
	}
}
func TestProbeRunAndWait(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer ts.Close()
	s, inst := service(t, `{"log":{"loglevel":"none"},"outbounds":[{"tag":"drop","protocol":"blackhole"},{"tag":"a1","protocol":"freedom","selection":{"balancer":"auto","cost":1,"probe":"check"}}],"routing":{"rules":[{"domain":["full:a.test"],"balancerTag":"auto"}],"balancers":[{"tag":"auto","strategy":{"type":"leastCost"},"fallbackTag":"drop"}]},"healthObservatory":{"probes":{"requests":{},"profiles":[{"name":"check","steps":[{"name":"transport","destinations":["`+ts.URL+`/204"]}]}]}}}`)
	router := inst.GetFeature(routing.RouterType()).(routing.Router)
	_, err := router.PickRoute(routing_session.AsRoutingContext(session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("a.test"), 80)}})))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r, err := s.RequestProbe(ctx, &command.RequestProbeRequest{Balancer: "auto", Outbounds: []string{"a1"}, WaitMs: 2000})
	var ticket health.ProbeTicket
	decodeData(t, r, err, &ticket)
	if ticket.ID == 0 || ticket.Run == "" {
		t.Fatalf("ticket %+v", ticket)
	}
	var view struct {
		IsDone    bool `json:"is_done"`
		Outbounds []struct {
			Result string `json:"result"`
		} `json:"outbounds"`
	}
	r, err = s.ProbeResult(ctx, &command.ProbeResultRequest{Run: ticket.Run, Id: ticket.ID})
	decodeData(t, r, err, &view)
	if !view.IsDone || len(view.Outbounds) != 1 || view.Outbounds[0].Result != "ok" {
		t.Fatalf("probe %+v", view)
	}
	r, err = s.ProbeResult(ctx, &command.ProbeResultRequest{Run: "previous-run", Id: ticket.ID})
	requireCode(t, r, err, "stale_run")
	r, err = s.RequestProbe(ctx, &command.RequestProbeRequest{Balancer: "auto", IsUdp: true})
	requireCode(t, r, err, "feature_disabled")
	r, err = s.RequestProbe(ctx, &command.RequestProbeRequest{Balancer: "auto", Outbounds: []string{"unknown"}})
	requireCode(t, r, err, "unknown_outbound")
}
func TestReportAndProviderExportObject(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"protocol":1,"version":"v1","outbounds":[{"tag":"x1","protocol":"freedom","selection":{"balancer":"auto","cost":1,"probe":"check"}}],"headers":{"Authorization":"Bearer secret"}}`))
	}))
	defer ts.Close()
	s, inst := service(t, `{"log":{"loglevel":"none"},"outbounds":[{"tag":"direct","protocol":"freedom"},{"tag":"drop","protocol":"blackhole"}],"routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost"},"fallbackTag":"drop"}]},"healthObservatory":{"probes":{"profiles":[{"name":"check","steps":[{"name":"transport","destinations":["http://127.0.0.1:1/check"]}]}]},"telemetry":{"report":{}},"providers":[{"tag":"fresh","urls":["`+ts.URL+`"],"isPlainHttpAllowed":true,"via":["direct"],"triggers":{"exhausted":{"isEnabled":false},"leaseRenewal":{"isEnabled":false},"backendSchedule":{"isEnabled":false}},"apply":{"balancer":"auto","tagPrefix":"p-","allowedProtocols":["freedom"]}}]}}`)
	selector, selectErr := health.FromInstance(inst).OutboundSelector("auto", nil)
	if selectErr != nil {
		t.Fatal(selectErr)
	}
	selector.Select(nil)
	ctx := context.Background()
	r, err := s.RequestProviderRefresh(ctx, &command.RequestProviderRefreshRequest{Provider: "fresh"})
	var refresh health.RefreshData
	decodeData(t, r, err, &refresh)
	if !refresh.IsQueued {
		t.Fatalf("refresh %+v", refresh)
	}
	var data health.ProviderSetData
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		r, err = s.ExportProviderSet(ctx, &command.ExportProviderSetRequest{Provider: "fresh"})
		if r.Data != "" {
			decodeData(t, r, err, &data)
			break
		}
	}
	var set map[string]any
	if json.Unmarshal(data.Set, &set) != nil || data.Seq == 0 || set["version"] != "v1" {
		t.Fatalf("set %s", r.Data)
	}
	r, err = s.ExportProviderSet(ctx, &command.ExportProviderSetRequest{Provider: "fresh", After: data.Seq})
	decodeData(t, r, err, nil)
	if r.Data != "" {
		t.Fatalf("unchanged export %s", r.Data)
	}
	r, err = s.PeekReport(ctx, &command.PeekReportRequest{})
	var snapshot map[string]any
	decodeData(t, r, err, &snapshot)
	if snapshot["report_reason"] != "snapshot" || snapshot["provider_applied"] != 1.0 {
		t.Fatalf("snapshot %s", r.Data)
	}
	r, err = s.TakeReport(ctx, &command.TakeReportRequest{IsFinal: true})
	var final map[string]any
	decodeData(t, r, err, &final)
	if final["report_reason"] != "stop" {
		t.Fatalf("final %s", r.Data)
	}
	r, err = s.SetProviderHeaders(ctx, &command.SetProviderHeadersRequest{Provider: "fresh", Headers: map[string]string{"Authorization": "secret\r\n"}})
	requireCode(t, r, err, "invalid_argument")
	if strings.Contains(r.Error.Message, "secret") {
		t.Fatal("credential in command error")
	}
}

func TestDirectProviderLimitsCannotDisableAdmission(t *testing.T) {
	config := func(pc *health.ProviderConfig) *core.Config {
		t.Helper()
		cfg, err := core.LoadConfig("json", strings.NewReader(`{"log":{"loglevel":"none"},"outbounds":[{"tag":"direct","protocol":"freedom"}],"healthObservatory":{}}`))
		if err != nil {
			t.Fatal(err)
		}
		for i, app := range cfg.App {
			value, err := app.GetInstance()
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := value.(*health.Config); ok {
				cfg.App[i] = serial.ToTypedMessage(&health.Config{Providers: []*health.ProviderConfig{pc}})
			}
		}
		return cfg
	}
	for _, field := range []string{"minInterval", "urgentMinInterval", "maxRefreshesPerHour"} {
		pc := &health.ProviderConfig{IsEnabled: proto.Bool(true), Tag: "fresh", Url: "https://provider.test/set", Apply: &health.ProviderApply{Balancer: "auto", TagPrefix: "p-"}, Limits: &health.ProviderLimits{}}
		switch field {
		case "minInterval":
			pc.Limits.MinInterval = -1
		case "urgentMinInterval":
			pc.Limits.ExhaustedMinInterval = -1
		case "maxRefreshesPerHour":
			pc.Limits.MaxRefreshesPerHour = -1
		}
		inst, err := core.New(config(pc))
		if inst != nil {
			inst.Close()
		}
		if err == nil || !strings.Contains(err.Error(), field) || !strings.Contains(err.Error(), "positive or omitted") {
			t.Fatalf("direct protobuf %s=-1: %v", field, err)
		}
	}
	pc := &health.ProviderConfig{IsEnabled: proto.Bool(true), Tag: "fresh", Url: "https://provider.test/set", Apply: &health.ProviderApply{Balancer: "auto", TagPrefix: "p-"}, Limits: &health.ProviderLimits{}}
	inst, err := core.New(config(pc))
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	o := health.FromInstance(inst)
	selector, err := o.OutboundSelector("auto", nil)
	if err != nil {
		t.Fatal(err)
	}
	selector.Select(nil)
	o.RequestProviderRefresh("fresh")
	data, err := o.ProviderRefreshInfo("fresh")
	if err != nil || !data.IsQueued {
		t.Fatalf("default admission %+v,%v", data, err)
	}
}
