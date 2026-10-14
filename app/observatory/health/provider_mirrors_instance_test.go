package health_test

import (
	"fmt"
	"github.com/xtls/xray-core/core"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
)

func TestProviderMirrorsTryInOrderBeforeNextVia(t *testing.T) {
	broken := &replyServer{status: http.StatusBadGateway}
	first := httptest.NewServer(broken)
	defer first.Close()
	working := &setServer{body: `{"outbounds":[{"tag":"x1","protocol":"freedom","selection":{"balancer":"auto","probe":"check","cost":1}}]}`}
	second := httptest.NewServer(working)
	defer second.Close()
	obs := twoViaCore(t, first.URL, fmt.Sprintf(`,"urls":[%q,%q,%q,%q]`, first.URL+"/one", second.URL, first.URL+"/three", first.URL+"/four"))
	w := &eventWaiter{t: t, o: obs}
	if !obs.RequestProviderRefresh("fresh") {
		t.Fatal("manual refresh refused")
	}
	w.until("provider_apply", func(e health.Event) bool { return e.Fields["is_ok"] == true })
	waitRefreshed(t, obs)
	fetches := w.since(0, "provider_fetch")
	if broken.requests() != 1 || working.requests() != 1 || len(fetches) != 2 {
		t.Fatalf("requests first=%d second=%d, events=%+v", broken.requests(), working.requests(), fetches)
	}
	for i, event := range fetches {
		if event.Fields["via"] != "direct" || event.Fields["mirror"] != float64(i+1) {
			t.Fatalf("ordered mirror[%d]: %+v", i, event.Fields)
		}
		if strings.Contains(fmt.Sprint(event.Fields), first.URL) || strings.Contains(fmt.Sprint(event.Fields), second.URL) {
			t.Fatal("provider endpoint leaked into event")
		}
	}
}

func TestProviderMirrorsRespectAuthoritativeRejection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		header map[string]string
	}{
		{name: "busy", status: http.StatusTooManyRequests, body: `{"protocol":1,"code":"rate_limited"}`},
		{name: "unauthorized", status: http.StatusUnauthorized, body: `{"protocol":1,"code":"unauthorized"}`},
		{name: "forbidden", status: http.StatusForbidden, body: `{"protocol":1,"code":"forbidden"}`},
		{name: "retry after", status: http.StatusServiceUnavailable, header: map[string]string{"Retry-After": "60"}},
		{name: "schema", status: http.StatusOK, body: `{"protocol":2,"outbounds":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusal := &replyServer{status: tc.status, body: tc.body, header: tc.header}
			first := httptest.NewServer(refusal)
			defer first.Close()
			working := &setServer{body: `{"outbounds":[]}`}
			second := httptest.NewServer(working)
			defer second.Close()
			obs := twoViaCore(t, first.URL, fmt.Sprintf(`,"urls":[%q,%q]`, first.URL, second.URL))
			if !obs.RequestProviderRefresh("fresh") {
				t.Fatal("manual refresh refused")
			}
			waitRefreshed(t, obs)
			if refusal.requests() != 1 || working.requests() != 0 {
				t.Fatalf("authoritative refusal bypassed: first=%d second=%d", refusal.requests(), working.requests())
			}
		})
	}
}

func TestProviderMirrorsLowMemoryMakesOnlyOneAttempt(t *testing.T) {
	broken := &replyServer{status: http.StatusBadGateway}
	first := httptest.NewServer(broken)
	defer first.Close()
	working := &setServer{body: `{"outbounds":[]}`}
	second := httptest.NewServer(working)
	defer second.Close()
	cfg, err := core.LoadConfig("json", strings.NewReader(fmt.Sprintf(`{
		"log":{"loglevel":"none"},"outbounds":[{"tag":"direct","protocol":"freedom"}],
		"routing":{"balancers":[{"tag":"auto","fallbackTag":"direct","strategy":{"type":"leastCost"}}]},
		"healthObservatory":{"host":{"memoryPressure":{}},"telemetry":{"events":{}},
		"providers":[{"tag":"fresh","urls":[%q,%q],"isPlainHttpAllowed":true,"via":["direct","direct"],
		"apply":{"balancers":["auto"],"tagPrefix":"p-","allowedProtocols":["freedom"]}}]}}`, first.URL, second.URL)))
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
	obs := health.FromInstance(inst)
	health.ConfigureProviderScenarioForTest(obs)
	health.ConfigureProviderLongCooldownForTest(obs)
	obs.SetMemoryLow(true)
	selectProviderTraffic(t, obs)
	waitRefreshed(t, obs)
	if broken.requests() != 1 || working.requests() != 0 {
		t.Fatalf("low memory made extra request: first=%d second=%d", broken.requests(), working.requests())
	}
}
