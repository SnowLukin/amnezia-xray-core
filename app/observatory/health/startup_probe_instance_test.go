package health_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/core"
	_ "github.com/xtls/xray-core/main/distro/all"
)

// startCore starts a core from config; nothing in the test connects
// through it.
func startCore(t *testing.T, config string) *health.Observer {
	t.Helper()
	cfg, err := core.LoadConfig("json", strings.NewReader(config))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := health.TuneInstance(inst, health.InstanceKnobs{ProbeSpacing: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inst.Close() })
	return health.FromInstance(inst)
}

// screen polls View until it is connected and returns every state and
// phase it saw on the way, each once, in order.
func screen(t *testing.T, o *health.Observer) (states, phases []string, last health.ViewData) {
	t.Helper()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		v, err := o.View()
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(states, v.State) {
			states = append(states, v.State)
		}
		if v.Phase != "" && !slices.Contains(phases, v.Phase) {
			phases = append(phases, v.Phase)
		}
		if last = v; v.State == "connected" {
			return states, phases, last
		}
	}
	t.Fatalf("not connected: states %v, phases %v, last %+v", states, phases, last)
	return nil, nil, last
}

func probeResults(w *eventWaiter) []string {
	var out []string
	for _, e := range w.since(0, "probe_result") {
		out = append(out, fmt.Sprint(e.Fields["outbound"], ":", e.Fields["reason"], ":", e.Fields["is_ok"]))
	}
	return out
}

const startupCore = `{"log":{"loglevel":"none"},
  "outbounds":[{"tag":"drop","protocol":"blackhole"},
    {"tag":"a1","protocol":"freedom","selection":{"balancer":"auto","cost":1,"probe":"first"}},
    {"tag":"b1","protocol":"freedom","selection":{"balancer":"auto","cost":2,"probe":"second"}}],
  "routing":{"balancers":[{"tag":"auto","fallbackTag":"drop","strategy":{"type":"leastCost","settings":%s}}]},
  "healthObservatory":{"host":{"view":{}},"telemetry":{"events":{}},"probes":{"profiles":[
    {"name":"first","steps":[{"name":"transport","destinations":[%q],"timeout":"2s"}]},
    {"name":"second","steps":[{"name":"transport","destinations":[%q],"timeout":"2s"}]}]}}}`

// With startup.probe a running core proves its choice by itself: the view
// gets to connected though no connection ever asked the balancer.
func TestStartupProbeConnectsWithoutTraffic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	defer srv.Close()
	o := startCore(t, fmt.Sprintf(startupCore, `{"startup":{"probe":{}}}`, srv.URL+"/a", srv.URL+"/b"))
	states, phases, v := screen(t, o)
	if !slices.Equal(states, []string{"connecting", "connected"}) || v.Outbound.Tag != "a1" || v.Phase != "" {
		t.Fatalf("states %v, phases %v, view %+v", states, phases, v)
	}
	w := &eventWaiter{t: t, o: o}
	w.until("probe_result", nil)
	if got := probeResults(w); !slices.Equal(got, []string{"a1:startup:true"}) {
		t.Fatalf("probes %v", got)
	}
	if evs := w.since(0, "switch"); len(evs) != 1 || evs[0].Fields["reason"] != "start" || len(w.since(0, "first_success")) != 0 {
		t.Fatalf("switches %v, first_success %v", evs, w.since(0, "first_success"))
	}
	var st struct {
		Features map[string]bool `json:"features"`
	}
	if err := json.Unmarshal(o.Status(), &st); err != nil || !st.Features["balancers.auto.startup_probe"] {
		t.Fatalf("features %v, %v", st.Features, err)
	}
}

// The first choice does not answer its probe: the core moves on and
// proves the next one the same way.
func TestStartupProbeMovesPastADeadChoice(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/a" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	o := startCore(t, fmt.Sprintf(startupCore, `{"startup":{"probe":{}},"failover":{"shouldProbeBeforeSwitch":false}}`, srv.URL+"/a", srv.URL+"/b"))
	states, _, v := screen(t, o)
	if !slices.Equal(states, []string{"connecting", "connected"}) || v.Outbound.Tag != "b1" {
		t.Fatalf("states %v, view %+v", states, v)
	}
	w := &eventWaiter{t: t, o: o}
	w.until("probe_result", func(e health.Event) bool { return e.Fields["outbound"] == "b1" })
	if got := probeResults(w); !slices.Equal(got, []string{"a1:startup:false", "b1:startup:true"}) {
		t.Fatalf("probes %v", got)
	}
}

// Without the block the same core does nothing until a connection comes.
func TestWithoutStartupProbeARunningCoreWaits(t *testing.T) {
	requests := make(chan string, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests <- r.URL.Path }))
	defer srv.Close()
	o := startCore(t, fmt.Sprintf(startupCore, `{}`, srv.URL+"/a", srv.URL+"/b"))
	time.Sleep(300 * time.Millisecond)
	v, err := o.View()
	if err != nil || v.State != "connecting" || v.Phase != "waiting" || v.Outbound.Tag != "" || !v.IsIdle || len(requests) != 0 {
		t.Fatalf("view %+v, %v, probes %d", v, err, len(requests))
	}
	raw, _ := o.DrainEvents()
	if string(raw) != "[]" {
		t.Fatalf("events %s", raw)
	}
}

// A config without a path that works starts on the set of its provider.
// With the block the core asks for it by itself, the view says connecting
// while the request runs, and the outbound the set brings is probed at
// once.
func TestStartupProbeWaitsForTheProviderSet(t *testing.T) {
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	defer probe.Close()
	release := make(chan struct{})
	asked := make(chan struct{}, 16)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked <- struct{}{}
		<-release
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"outbounds":[{"tag":"x1","protocol":"freedom","selection":{"balancer":"auto","cost":1,"probe":"check"}}]}`))
	}))
	defer backend.Close()
	o := startCore(t, `{"log":{"loglevel":"none"},
	  "outbounds":[{"tag":"direct","protocol":"freedom"},{"tag":"drop","protocol":"blackhole"}],
	  "routing":{"balancers":[{"tag":"auto","fallbackTag":"drop","strategy":{"type":"leastCost","settings":{"startup":{"probe":{}}}}}]},
	  "healthObservatory":{"host":{"view":{}},"telemetry":{"events":{}},
	    "probes":{"profiles":[{"name":"check","steps":[{"name":"transport","destinations":["`+probe.URL+`/x"],"timeout":"2s"}]}]},
	    "providers":[{"tag":"fresh","urls":["`+backend.URL+`/set"],"isPlainHttpAllowed":true,"via":["direct"],"timeout":"10s",
	      "triggers":{"exhausted":{"delay":"0s..1s"},"leaseRenewal":{"isEnabled":false},"backendSchedule":{"isEnabled":false}},
	      "apply":{"balancers":["auto"],"tagPrefix":"p-","allowedProtocols":["freedom"]}}]}}`)
	select {
	case <-asked:
	case <-time.After(10 * time.Second):
		t.Fatal("the core did not ask the provider")
	}
	v, err := o.View()
	if err != nil || v.State != "connecting" || v.Phase != "refreshing" || v.Outbound.Tag != "drop" || !slices.Contains(v.Doing, "refreshing") {
		t.Fatalf("while the request runs: %+v, %v", v, err)
	}
	close(release)
	// The set is applied, its outbound chosen and probed: nothing on the
	// way looks like a core that gave up.
	states, _, v := screen(t, o)
	if slices.Contains(states, "blocked") || v.Outbound.Tag != "p-x1" {
		t.Fatalf("states %v, view %+v", states, v)
	}
	w := &eventWaiter{t: t, o: o}
	w.until("probe_result", nil)
	if got := probeResults(w); !slices.Equal(got, []string{"p-x1:startup:true"}) {
		t.Fatalf("probes %v", got)
	}
	if evs := w.since(0, "switch"); len(evs) != 1 || evs[0].Fields["reason"] != "candidates_changed" {
		t.Fatalf("switches %v", evs)
	}
}
