package health_test

import (
	"encoding/json"
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
}

func (s *setServer) set(body string) {
	s.mu.Lock()
	s.body = body
	s.mu.Unlock()
}

func (s *setServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.auth = append(s.auth, r.Header.Get("Authorization"))
	s.queries = append(s.queries, r.URL.RawQuery)
	body := s.body
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
// tried.
func twoViaCore(t *testing.T, url string) *health.Observer {
	t.Helper()
	cfg, err := core.LoadConfig("json", strings.NewReader(`{
	  "log": {"loglevel": "none"},
	  "outbounds": [
	    {"tag": "direct", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}},
	    {"tag": "direct2", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}}
	  ],
	  "routing": {"balancers": [{"tag": "auto", "selector": ["p-"], "strategy": {"type": "leastCost", "settings": {}}}]},
	  "healthObservatory": {
	    "subjectSelector": ["p-"],
	    "providers": [{
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
