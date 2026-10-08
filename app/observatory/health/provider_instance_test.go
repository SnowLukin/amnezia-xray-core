package health_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	_ "github.com/xtls/xray-core/main/distro/all"
)

type setServer struct {
	mu      sync.Mutex
	body    string
	auth    []string
	queries []string
	// Answers served before body, one per request.
	first   []string
	methods []string
	types   []string
	bodies  [][]byte
}

func (s *setServer) set(body string) {
	s.mu.Lock()
	s.body = body
	s.mu.Unlock()
}

func (s *setServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sent, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.auth = append(s.auth, r.Header.Get("Authorization"))
	s.queries = append(s.queries, r.URL.RawQuery)
	s.methods = append(s.methods, r.Method)
	s.types = append(s.types, r.Header.Get("Content-Type"))
	s.bodies = append(s.bodies, sent)
	body := s.body
	if len(s.first) > 0 {
		body, s.first = s.first[0], s.first[1:]
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

type eventWaiter struct {
	t   *testing.T
	o   *health.Observer
	all []health.Event
}

// until drains events until one of the type matches, or fails the test.
func (w *eventWaiter) until(typ string, match func(health.Event) bool) health.Event {
	w.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	seen := len(w.all)
	for time.Now().Before(deadline) {
		raw, _ := w.o.DrainEvents()
		var evs []health.Event
		if err := json.Unmarshal(raw, &evs); err != nil {
			w.t.Fatal(err)
		}
		w.all = append(w.all, evs...)
		for _, e := range w.all[seen:] {
			if e.Type == typ && (match == nil || match(e)) {
				return e
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	w.t.Fatalf("no %s event; got %+v", typ, w.all[seen:])
	return health.Event{}
}

func (w *eventWaiter) since(from int, typ string) []health.Event {
	var out []health.Event
	for _, e := range w.all[from:] {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

// Outbounds that need real settings to build: an unchanged one must be
// recognized as unchanged although its config is built anew from JSON.
const vlessOutbounds = `{"tag": "x6", "protocol": "vless", "settings": {"vnext": [{"address": "127.0.0.1", "port": 1,
	  "users": [{"id": "5f88c4df-2a2d-4b68-9b27-5d21339abb60", "encryption": "none", "flow": "xtls-rprx-vision"}]}]},
	  "streamSettings": {"network": "tcp", "security": "reality", "realitySettings": {"serverName": "example.com",
	    "password": "SFQq-6EUyhY12O_zOjI9pVdY1b7y3aVaevZJg8JDaEw", "shortId": "", "fingerprint": "chrome"}}},
	{"tag": "x7", "protocol": "vless", "settings": {"vnext": [{"address": "127.0.0.1", "port": 2,
	  "users": [{"id": "5f88c4df-2a2d-4b68-9b27-5d21339abb60", "encryption": "none"}]}]},
	  "streamSettings": {"network": "xhttp", "security": "tls", "tlsSettings": {"serverName": "x.example.com", "alpn": ["h2"]},
	    "xhttpSettings": {"path": "/x", "mode": "auto"}}}`

func TestProviderRefreshInRunningCore(t *testing.T) {
	srv := &setServer{}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	config := `{
	  "log": {"loglevel": "none"},
	  "outbounds": [
	    {"tag": "direct", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}},
	    {"tag": "dead", "protocol": "blackhole"},
	    {"tag": "block", "protocol": "blackhole"}
	  ],
	  "routing": {"balancers": [{"tag": "auto", "selector": ["p-"],
	    "strategy": {"type": "leastCost", "settings": {}}, "fallbackTag": "block"}]},
	  "healthObservatory": {
	    "subjectSelector": ["p-"],
	    "providers": [{
	      "tag": "fresh", "url": "` + ts.URL + `/set", "headers": {"Authorization": "Bearer t0k"},
	      "via": ["missing", "dead", "direct"], "timeout": "3s",
	      "triggers": {"interval": "0s", "minHealthy": 0, "whenExhausted": false, "maxAge": "0s",
	                   "onNetworkChange": "never"},
	      "limits": {"exhaustedMinInterval": "10ms", "backoff": {"initial": "50ms"}},
	      "allowPlainHttp": true,
	      "apply": {"balancer": "auto", "tagPrefix": "p-", "drain": "300ms", "protocols": ["freedom", "blackhole", "vless", "nosuch"]}
	    }]
	  }
	}`
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
	defer inst.Close()
	obs := health.FromInstance(inst)
	ohm := inst.GetFeature(outbound.ManagerType()).(outbound.Manager)
	w := &eventWaiter{t: t, o: obs}

	srv.set(`{"outbounds": [{"tag": "x1", "protocol": "freedom"},
	                        {"tag": "x2", "protocol": "freedom", "settings": {"domainStrategy": "UseIP"}}, ` +
		vlessOutbounds + `],
	          "costs": {"x2": 1}}`)
	if !obs.RequestRefresh("fresh") {
		t.Fatal("provider not found")
	}
	ev := w.until("provider_apply", nil)
	if ev.Fields["ok"] != true || ev.Fields["added"] != 4.0 {
		t.Fatalf("first apply %v", ev.Fields)
	}
	fetches := w.since(0, "provider_fetch")
	if len(fetches) != 3 || fetches[0].Fields["error"] != "no such outbound" ||
		fetches[1].Fields["via"] != "dead" || fetches[1].Fields["ok"] != false ||
		fetches[2].Fields["via"] != "direct" || fetches[2].Fields["ok"] != true || fetches[2].Fields["trigger"] != "host" {
		t.Fatalf("fetches %+v; want the missing via skipped, the dead one failed, then direct", fetches)
	}
	if e, _ := fetches[1].Fields["error"].(string); e == "" || strings.Contains(e, "/set") {
		t.Fatalf("error of the dead via %q: want the cause without the URL", e)
	}
	if ohm.GetHandler("p-x1") == nil || ohm.GetHandler("p-x2") == nil {
		t.Fatal("provider outbounds are not registered")
	}
	srv.mu.Lock()
	if len(srv.auth) != 1 || srv.auth[0] != "Bearer t0k" || srv.queries[0] != "" {
		t.Fatalf("server saw auth %v, queries %v", srv.auth, srv.queries)
	}
	srv.mu.Unlock()

	// x1 leaves, x2, x6 and x7 stay as they are, x3 is new.
	x2, x6, x7 := ohm.GetHandler("p-x2"), ohm.GetHandler("p-x6"), ohm.GetHandler("p-x7")
	from := len(w.all)
	srv.set(`{"outbounds": [{"tag": "x2", "protocol": "freedom", "settings": {"domainStrategy": "UseIP"}},
	                        {"tag": "x3", "protocol": "blackhole"}, ` + vlessOutbounds + `]}`)
	obs.RequestRefresh("")
	ev = w.until("provider_apply", nil)
	if ev.Fields["added"] != 1.0 || ev.Fields["removed"] != 1.0 || ev.Fields["unchanged"] != 3.0 {
		t.Fatalf("second apply %v", ev.Fields)
	}
	if ohm.GetHandler("p-x2") != x2 || ohm.GetHandler("p-x6") != x6 || ohm.GetHandler("p-x7") != x7 {
		t.Fatal("an unchanged outbound was replaced")
	}
	// Nobody used p-x1, so its drain ends with the apply.
	if len(w.since(from, "provider_drained")) == 0 {
		w.until("provider_drained", func(e health.Event) bool { return e.Fields["outbound"] == "p-x1" })
	}
	if ohm.GetHandler("p-x1") != nil {
		t.Fatal("p-x1 is still registered after its drain")
	}

	// A broken answer through every via changes nothing.
	from = len(w.all)
	srv.set(`{"outbounds": [`)
	obs.RequestRefresh("fresh")
	w.until("provider_fetch", func(e health.Event) bool { return e.Fields["via"] == "direct" })
	if f := w.since(from, "provider_fetch"); !strings.Contains(f[len(f)-1].Fields["error"].(string), "invalid answer") {
		t.Fatalf("fetch %v", f[len(f)-1].Fields)
	}
	// A set that does not build changes nothing either. The failed host
	// request is retried after the backoff without another request.
	srv.set(`{"outbounds": [{"tag": "x4", "protocol": "freedom"}, {"tag": "x5", "protocol": "nosuch"}]}`)
	ev = w.until("provider_apply", nil)
	if ev.Fields["ok"] != false || !strings.Contains(ev.Fields["error"].(string), "build") {
		t.Fatalf("apply of a set that does not build %v", ev.Fields)
	}
	if ohm.GetHandler("p-x4") != nil || ohm.GetHandler("p-x2") != x2 || ohm.GetHandler("p-x3") == nil {
		t.Fatal("a set that does not build changed the outbounds")
	}

	var st struct {
		Providers []struct {
			Provider string `json:"provider"`
			Members  int    `json:"members"`
			Failures int    `json:"failures"`
		} `json:"providers"`
	}
	if err := json.Unmarshal(obs.Status(), &st); err != nil || len(st.Providers) != 1 || st.Providers[0].Members != 4 ||
		st.Providers[0].Failures < 2 {
		t.Fatalf("status %+v, %v", st, err)
	}
}

// While memory is low a refresh asks only the first via it can use; the
// backoff brings the next attempt.
func TestRefreshUnderLowMemoryTriesOneVia(t *testing.T) {
	srv := &setServer{}
	srv.set(`{"outbounds": [{"tag": "x1", "protocol": "freedom"}]}`)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	cfg, err := core.LoadConfig("json", strings.NewReader(`{
	  "log": {"loglevel": "none"},
	  "outbounds": [
	    {"tag": "direct", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}},
	    {"tag": "dead", "protocol": "blackhole"}
	  ],
	  "routing": {"balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost", "settings": {}}}]},
	  "healthObservatory": {
	    "subjectSelector": ["p-"],
	    "providers": [{
	      "tag": "fresh", "url": "`+ts.URL+`/set", "allowPlainHttp": true,
	      "via": ["missing", "dead", "direct"], "timeout": "500ms",
	      "triggers": {"interval": "0s", "minHealthy": 0, "whenExhausted": false, "maxAge": "0s", "onNetworkChange": "never"},
	      "limits": {"backoff": {"initial": "1h"}},
	      "apply": {"balancer": "auto", "tagPrefix": "p-", "protocols": ["freedom"]}
	    }]
	  }
	}`))
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
	obs.SetMemoryLow(true)
	w := &eventWaiter{t: t, o: obs}
	obs.RequestRefresh("fresh")
	w.until("provider_fetch", func(e health.Event) bool { return e.Fields["via"] == "dead" })
	var st struct {
		Providers []struct {
			Failures int  `json:"failures"`
			Fetching bool `json:"fetching"`
		} `json:"providers"`
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if err := json.Unmarshal(obs.Status(), &st); err != nil {
			t.Fatal(err)
		}
		if !st.Providers[0].Fetching {
			break
		}
	}
	raw, _ := obs.DrainEvents()
	var rest []health.Event
	if err := json.Unmarshal(raw, &rest); err != nil {
		t.Fatal(err)
	}
	w.all = append(w.all, rest...)
	var vias []string
	for _, f := range w.since(0, "provider_fetch") {
		vias = append(vias, f.Fields["via"].(string))
	}
	if strings.Join(vias, ",") != "missing,dead" || st.Providers[0].Failures != 1 {
		t.Fatalf("requests through %v, %d failures; want the dead via alone after the missing one", vias, st.Providers[0].Failures)
	}
}

// twoViaCore runs a core whose provider at url is reached through direct,
// then direct2; both work, so only the answer decides whether the second is
// tried. more goes into the provider, such as a method.
func twoViaCore(t *testing.T, url string, more ...string) *health.Observer {
	t.Helper()
	cfg, err := core.LoadConfig("json", strings.NewReader(`{
	  "log": {"loglevel": "none"},
	  "outbounds": [
	    {"tag": "direct", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}},
	    {"tag": "direct2", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}},
	    {"tag": "p-b", "protocol": "blackhole"},
	    {"tag": "p-a", "protocol": "blackhole"}
	  ],
	  "routing": {"balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost", "settings": {}}}]},
	  "healthObservatory": {
	    "subjectSelector": ["p-"],
	    "providers": [{`+strings.Join(more, "")+`
	      "tag": "fresh", "url": "`+url+`", "allowPlainHttp": true, "via": ["direct", "direct2"], "timeout": "3s",
	      "triggers": {"interval": "0s", "minHealthy": 0, "whenExhausted": false, "maxAge": "0s", "onNetworkChange": "never"},
	      "limits": {"backoff": {"initial": "1h"}},
	      "apply": {"balancer": "auto", "tagPrefix": "p-", "protocols": ["freedom"]}
	    }]
	  }
	}`))
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
	return health.FromInstance(inst)
}

func (s *setServer) requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.auth)
}

// An answer that is JSON but breaks the protocol comes from the backend
// itself: the other via leads to the same backend, so it is not asked, and
// the report counts the answer once, as rejected.
func TestSchemaErrorEndsTheAttempts(t *testing.T) {
	srv := &setServer{}
	srv.set(`{"protocol": 1, "outbounds": [{"tag": "x1", "protocol": "freedom"}], "dns": {"servers": ["1.1.1.1"]}}`)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	obs := twoViaCore(t, ts.URL+"/set")
	w := &eventWaiter{t: t, o: obs}
	obs.RequestRefresh("fresh")
	apply := w.until("provider_apply", nil)
	fetches := w.since(0, "provider_fetch")
	if srv.requests() != 1 || len(fetches) != 1 || fetches[0].Fields["ok"] != true || fetches[0].Fields["error"] != nil {
		t.Fatalf("%d requests, fetches %+v; want one that the backend answered", srv.requests(), fetches)
	}
	if apply.Fields["ok"] != false || apply.Fields["error"] != `invalid answer: unknown key "dns"` {
		t.Fatalf("apply %v", apply.Fields)
	}
	var report map[string]any
	if err := json.Unmarshal(obs.Report(), &report); err != nil {
		t.Fatal(err)
	}
	if report["provider_rejected"] != 1.0 || report["provider_fetch_failures"] != 0.0 || report["provider_applied"] != 0.0 {
		t.Fatalf("report rejected %v, fetch failures %v, applied %v", report["provider_rejected"],
			report["provider_fetch_failures"], report["provider_applied"])
	}
}

// A body that is not JSON may have been broken on the way: the next via is
// tried.
func TestBrokenJSONTriesNextVia(t *testing.T) {
	srv := &setServer{}
	srv.set(`{"outbounds": [{"tag": `)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	obs := twoViaCore(t, ts.URL+"/set")
	w := &eventWaiter{t: t, o: obs}
	obs.RequestRefresh("fresh")
	w.until("provider_fetch", func(e health.Event) bool { return e.Fields["via"] == "direct2" })
	fetches := w.since(0, "provider_fetch")
	if srv.requests() != 2 || len(fetches) != 2 || fetches[0].Fields["ok"] != false ||
		!strings.Contains(fetches[0].Fields["error"].(string), "invalid answer") {
		t.Fatalf("%d requests, fetches %+v; want both via tried", srv.requests(), fetches)
	}
	if applies := w.since(0, "provider_apply"); len(applies) != 0 {
		t.Fatalf("a broken answer reached the apply: %+v", applies)
	}
}

// replyServer answers every request with one status, headers and body.
type replyServer struct {
	status int
	header map[string]string
	body   string
	mu     sync.Mutex
	n      int
}

func (s *replyServer) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	for k, v := range s.header {
		w.Header().Set(k, v)
	}
	w.WriteHeader(s.status)
	_, _ = w.Write([]byte(s.body))
}

func (s *replyServer) requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// waitRefreshed waits until the first refresh of the provider is over.
func waitRefreshed(t *testing.T, obs *health.Observer) {
	t.Helper()
	var st struct {
		Providers []struct {
			Refreshes int  `json:"refreshes"`
			Fetching  bool `json:"fetching"`
		} `json:"providers"`
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if err := json.Unmarshal(obs.Status(), &st); err != nil {
			t.Fatal(err)
		}
		if st.Providers[0].Refreshes > 0 && !st.Providers[0].Fetching {
			return
		}
	}
	t.Fatal("the refresh did not end")
}

// Only the backend itself ends the attempts: by asking to wait or with an
// error body of the protocol. A bare status may come from a firewall or a
// portal on one way, so the next via is tried.
func TestBackendAnswerEndsTheAttempts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		srv      *replyServer
		requests int
		fields   map[string]any
	}{
		{"503 with Retry-After", &replyServer{status: 503, header: map[string]string{"Retry-After": "7200"}}, 1,
			// Two hours are cut to backoff.max.
			map[string]any{"status": 503.0, "retry_after_ms": 1_800_000.0}},
		{"503 without Retry-After", &replyServer{status: 503}, 2, map[string]any{"status": 503.0}},
		{"429 with an error body", &replyServer{status: 429, header: map[string]string{"Content-Type": "application/json"},
			body: `{"protocol": 1, "error": "rate_limited", "message": "slow down"}`}, 1,
			map[string]any{"status": 429.0, "code": "rate_limited"}},
		{"429 with a body of no protocol", &replyServer{status: 429, body: `{"error": "rate_limited"}`}, 2,
			map[string]any{"status": 429.0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(tc.srv)
			defer ts.Close()
			obs := twoViaCore(t, ts.URL+"/set")
			w := &eventWaiter{t: t, o: obs}
			obs.RequestRefresh("fresh")
			waitRefreshed(t, obs)
			w.until("provider_fetch", nil)
			fetches := w.since(0, "provider_fetch")
			if tc.srv.requests() != tc.requests || len(fetches) != tc.requests {
				t.Fatalf("%d requests, fetches %+v; want %d", tc.srv.requests(), fetches, tc.requests)
			}
			for k, v := range tc.fields {
				if fetches[0].Fields[k] != v {
					t.Errorf("fetch %s = %v, want %v", k, fetches[0].Fields[k], v)
				}
			}
			for _, k := range []string{"code", "retry_after_ms"} {
				if _, want := tc.fields[k]; !want && fetches[0].Fields[k] != nil {
					t.Errorf("fetch has %s %v", k, fetches[0].Fields[k])
				}
			}
			if strings.Contains(fmt.Sprint(fetches), "slow down") || strings.Contains(string(obs.Status()), "slow down") {
				t.Error("the message of an error body was kept")
			}
		})
	}
}

// Every via of one refresh sends the same body: the backend tells a request
// sent again from a new one by run and refresh.
func TestSameBodyThroughEveryVia(t *testing.T) {
	srv := &setServer{first: []string{`{"outbounds": [`}}
	srv.set(`{"protocol": 1, "outbounds": [{"tag": "x1", "protocol": "freedom"}]}`)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	obs := twoViaCore(t, ts.URL+"/set")
	w := &eventWaiter{t: t, o: obs}
	obs.RequestRefresh("fresh")
	w.until("provider_apply", nil)
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.bodies) != 2 || !bytes.Equal(srv.bodies[0], srv.bodies[1]) || len(srv.bodies[0]) == 0 {
		t.Fatalf("bodies %q", srv.bodies)
	}
	if srv.methods[0] != "POST" || srv.types[0] != "application/json" {
		t.Fatalf("method %s, content type %q", srv.methods[0], srv.types[0])
	}
	var body struct {
		Run     string `json:"run"`
		Refresh int    `json:"refresh"`
		Trigger string `json:"trigger"`
	}
	if err := json.Unmarshal(srv.bodies[0], &body); err != nil || body.Run == "" || body.Refresh != 1 || body.Trigger != "host" {
		t.Fatalf("body %s: %v", srv.bodies[0], err)
	}
	for _, f := range w.since(0, "provider_fetch") {
		if f.Fields["method"] != "POST" || f.Fields["refresh"] != 1.0 {
			t.Fatalf("fetch %v", f.Fields)
		}
	}
}

// A static file on a web server is asked for with a GET without a body.
func TestGetSendsNoBody(t *testing.T) {
	srv := &setServer{}
	srv.set(`{"outbounds": [{"tag": "x1", "protocol": "freedom"}]}`)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	obs := twoViaCore(t, ts.URL+"/set", `"method": "GET",`)
	w := &eventWaiter{t: t, o: obs}
	obs.RequestRefresh("fresh")
	if ev := w.until("provider_apply", nil); ev.Fields["ok"] != true {
		t.Fatalf("apply %v", ev.Fields)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.methods) != 1 || srv.methods[0] != "GET" || len(srv.bodies[0]) != 0 || srv.types[0] != "" {
		t.Fatalf("methods %v, bodies %q, content types %q", srv.methods, srv.bodies, srv.types)
	}
}

// Before its first answer the provider owns the outbounds of the config with
// its prefix, and the first request already tells the backend about them.
func TestRequestBodyListsConfigOutboundsBeforeTheFirstAnswer(t *testing.T) {
	srv := &setServer{}
	srv.set(`{"outbounds": [{"tag": "x1", "protocol": "freedom"}]}`)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	obs := twoViaCore(t, ts.URL+"/set")
	var st struct {
		Providers []struct {
			Members int    `json:"members"`
			Method  string `json:"method"`
		} `json:"providers"`
	}
	if err := json.Unmarshal(obs.Status(), &st); err != nil || st.Providers[0].Members != 2 || st.Providers[0].Method != "POST" {
		t.Fatalf("status before the first answer %+v, %v", st, err)
	}
	w := &eventWaiter{t: t, o: obs}
	obs.RequestRefresh("fresh")
	w.until("provider_apply", nil)
	srv.mu.Lock()
	defer srv.mu.Unlock()
	var body struct {
		Outbounds []struct {
			Tag   string `json:"tag"`
			State string `json:"state"`
		} `json:"outbounds"`
	}
	if err := json.Unmarshal(srv.bodies[0], &body); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(body.Outbounds) != "[{a unknown} {b unknown}]" {
		t.Fatalf("outbounds of the first request %v", body.Outbounds)
	}
}
