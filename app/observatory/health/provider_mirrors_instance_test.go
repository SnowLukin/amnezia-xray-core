package health_test

import (
	"fmt"
	"github.com/xtls/xray-core/core"
	gonet "net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
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
	if obs.RequestProviderRefresh("fresh") == 0 {
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
		{name: "retry after", status: http.StatusServiceUnavailable, body: `{"protocol":1,"code":"unavailable"}`, header: map[string]string{"Retry-After": "60"}},
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
			if obs.RequestProviderRefresh("fresh") == 0 {
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

// A 503 with Retry-After but no body of the protocol may come from a proxy
// in front of a mirror: the next mirror may reach the backend.
func TestProviderRetryAfterWithoutProtocolBodyTriesTheNextMirror(t *testing.T) {
	proxy := &replyServer{status: http.StatusServiceUnavailable, header: map[string]string{"Retry-After": "60"}}
	first := httptest.NewServer(proxy)
	defer first.Close()
	working := &setServer{body: `{"outbounds":[{"tag":"x1","protocol":"freedom","selection":{"balancer":"auto","probe":"check","cost":1}}]}`}
	second := httptest.NewServer(working)
	defer second.Close()
	obs := twoViaCore(t, first.URL, fmt.Sprintf(`,"urls":[%q,%q]`, first.URL, second.URL))
	w := &eventWaiter{t: t, o: obs}
	if obs.RequestProviderRefresh("fresh") == 0 {
		t.Fatal("manual refresh refused")
	}
	w.until("provider_apply", func(e health.Event) bool { return e.Fields["is_ok"] == true })
	if proxy.requests() != 1 || working.requests() != 1 {
		t.Fatalf("requests proxy=%d backend=%d", proxy.requests(), working.requests())
	}
}

type hangServer struct {
	n    atomic.Int32
	done chan struct{}
}

func (s *hangServer) ServeHTTP(_ http.ResponseWriter, r *http.Request) {
	s.n.Add(1)
	select {
	case <-r.Context().Done():
	case <-s.done:
	}
}

// A backend that takes requests and does not answer gets as many as one that
// answers out of the protocol, however many mirrors there are, and one more
// through each further via.
func TestProviderServerThatDoesNotAnswerGetsTwoRequests(t *testing.T) {
	hang := &hangServer{done: make(chan struct{})}
	srv := httptest.NewServer(hang)
	defer srv.Close()
	defer close(hang.done)
	u := srv.URL
	obs := twoViaCore(t, u, fmt.Sprintf(`,"timeout":"500ms","urls":[%q,%q,%q,%q]`, u+"/1", u+"/2", u+"/3", u+"/4"))
	if obs.RequestProviderRefresh("fresh") == 0 {
		t.Fatal("manual refresh refused")
	}
	waitRefreshed(t, obs)
	if n := hang.n.Load(); n != 3 {
		t.Fatalf("%d requests to a server that does not answer, want 2 and 1 via direct2", n)
	}
}

// Roads whose entries swallow requests look like a backend that does not
// answer. They must not use up the refresh before the next roads, direct
// among them, get their attempt.
func TestProviderEveryViaGetsAnAttemptAfterDeadRoads(t *testing.T) {
	hang := &hangServer{done: make(chan struct{})}
	dead := httptest.NewServer(hang)
	defer dead.Close()
	defer close(hang.done)
	working := &setServer{body: `{"outbounds":[{"tag":"x1","protocol":"freedom","selection":{"balancer":"auto","probe":"check","cost":1}}]}`}
	backend := httptest.NewServer(working)
	defer backend.Close()
	entry := strings.TrimPrefix(dead.URL, "http://")
	cfg, err := core.LoadConfig("json", strings.NewReader(fmt.Sprintf(`{
		"log":{"loglevel":"none"},"outbounds":[{"tag":"direct","protocol":"freedom"},
		{"tag":"entry1","protocol":"freedom","settings":{"redirect":%q}},
		{"tag":"entry2","protocol":"freedom","settings":{"redirect":%q}}],
		"routing":{"balancers":[{"tag":"auto","fallbackTag":"direct","strategy":{"type":"leastCost"}}]},
		"healthObservatory":{"telemetry":{"events":{}},
		"providers":[{"tag":"fresh","urls":[%q,%q],"isPlainHttpAllowed":true,"timeout":"300ms","via":["entry1","entry2","direct"],
		"triggers":{"exhausted":{"isEnabled":false},"leaseRenewal":{"isEnabled":false}},
		"apply":{"balancers":["auto"],"tagPrefix":"p-","allowedProtocols":["freedom"]}}]}}`, entry, entry, backend.URL+"/1", backend.URL+"/2")))
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
	selectProviderTraffic(t, obs)
	w := &eventWaiter{t: t, o: obs}
	if obs.RequestProviderRefresh("fresh") == 0 {
		t.Fatal("manual refresh refused")
	}
	waitRefreshed(t, obs)
	w.until("provider_fetch", nil)
	var vias []any
	for _, f := range w.since(0, "provider_fetch") {
		vias = append(vias, f.Fields["via"])
	}
	if !slices.Contains(vias, "entry2") || vias[len(vias)-1] != "direct" || working.requests() != 1 {
		t.Fatalf("attempts via %v, backend requests %d", vias, working.requests())
	}
}

// Timeouts before the request went out, such as a TLS handshake that never
// ends, end a via after four; the next via gets one more attempt.
func TestProviderTimeoutsHaveACeiling(t *testing.T) {
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var accepted atomic.Int32
	go func() {
		var held []gonet.Conn
		defer func() {
			for _, c := range held {
				c.Close()
			}
		}()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			held = append(held, c)
		}
	}()
	u := "https://" + ln.Addr().String()
	obs := twoViaCore(t, u, fmt.Sprintf(`,"timeout":"300ms","urls":[%q,%q,%q,%q]`, u+"/1", u+"/2", u+"/3", u+"/4"))
	w := &eventWaiter{t: t, o: obs}
	if obs.RequestProviderRefresh("fresh") == 0 {
		t.Fatal("manual refresh refused")
	}
	waitRefreshed(t, obs)
	w.until("provider_fetch", nil)
	fetches := w.since(0, "provider_fetch")
	if len(fetches) != 5 || accepted.Load() < 5 {
		t.Fatalf("%d attempts, %d connections; want 4 and 1 via direct2", len(fetches), accepted.Load())
	}
	for _, f := range fetches {
		if f.Fields["error"] != "timeout" {
			t.Fatalf("fetch %v, want a timeout", f.Fields)
		}
	}
}

// The text of a TLS failure names the certificate and the host asked for:
// events and Status keep only the class.
func TestProviderTLSFailureIsOnlyAClass(t *testing.T) {
	srv := httptest.NewTLSServer(&setServer{body: `{"outbounds":[]}`})
	defer srv.Close()
	u := strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)
	obs := twoViaCore(t, u)
	w := &eventWaiter{t: t, o: obs}
	if obs.RequestProviderRefresh("fresh") == 0 {
		t.Fatal("manual refresh refused")
	}
	waitRefreshed(t, obs)
	w.until("provider_fetch", nil)
	fetches := w.since(0, "provider_fetch")
	status := string(obs.Status())
	for _, leak := range []string{"x509", "localhost", "example.com", "certificate"} {
		if strings.Contains(fmt.Sprint(fetches), leak) || strings.Contains(status, leak) {
			t.Fatalf("%q in events %v or Status %s", leak, fetches, status)
		}
	}
	if len(fetches) == 0 || fetches[0].Fields["error"] != "tls" || !strings.Contains(status, `"last_error":"tls"`) {
		t.Fatalf("fetches %v, Status %s", fetches, status)
	}
}
