package health_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	gonet "net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/infra/conf/serial"
	_ "github.com/xtls/xray-core/main/distro/all"
)

func probeInstance(t *testing.T) *health.Observer {
	t.Helper()
	cfg, err := serial.LoadJSONConfig(strings.NewReader(`{"log":{"loglevel":"none"},"outbounds":[{"tag":"a1","protocol":"freedom","settings":{"finalRules":[{"action":"allow"}]},"selection":{"balancer":"auto","cost":1,"probe":"fixture"}},{"tag":"block","protocol":"blackhole"}],"routing":{"balancers":[{"tag":"auto","fallbackTag":"block","strategy":{"type":"leastCost"}}]},"healthObservatory":{"probes":{"profiles":[{"name":"fixture","steps":[{"name":"transport","destinations":["http://127.0.0.1:1/probe"]}]}]}}}`))
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

func TestLeastCostAddedAtRuntimeNextToAnotherObservatoryDoesNotPanic(t *testing.T) {
	cfg, err := serial.LoadJSONConfig(strings.NewReader(`{"log": {"loglevel": "none"},
		"burstObservatory": {"subjectSelector": ["a"], "pingConfig": {"destination": "http://127.0.0.1:1/", "interval": "1h"}},
		"outbounds": [{"tag": "a", "protocol": "freedom"}],
		"routing": {"rules": []}}`))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	r := inst.GetFeature(routing.RouterType()).(*router.Router)
	rule := &router.BalancingRule{Tag: "b", OutboundSelector: []string{"a"}, Strategy: "leastcost", FallbackTag: "a"}
	if err := r.ReloadRules(&router.Config{BalancingRule: []*router.BalancingRule{rule}}, true); err != nil {
		t.Logf("reload refused: %v", err)
	}
}

func TestProbeEndsByItsDeadlineAndLeavesNothing(t *testing.T) {
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []gonet.Conn
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			held = append(held, c)
		}
	}()
	defer func() {
		ln.Close()
		<-acceptDone
		for _, c := range held {
			c.Close()
		}
	}()
	o := probeInstance(t)
	before := runtime.NumGoroutine()
	start := time.Now()
	ok, _, _ := health.Probe(o, "a1", "http://"+ln.Addr().String()+"/silent", 0)
	if took := time.Since(start); ok || took > 5*time.Second {
		t.Fatalf("a server that never answers: ok %v after %v, want a failure by the 4 s deadline", ok, took)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("%d goroutines 5 s after the probe, %d before", n, before)
	}
}

func TestProbeCountsBytesOnTheWire(t *testing.T) {
	body := bytes.Repeat([]byte("abcdefgh"), 8*1024)
	var gzipped atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			gzipped.Store(true)
			w.Header().Set("Content-Encoding", "gzip")
			z := gzip.NewWriter(w)
			z.Write(body)
			z.Close()
			return
		}
		w.Write(body)
	}))
	defer srv.Close()
	o := probeInstance(t)
	ok, n, msg := health.Probe(o, "a1", srv.URL+"/bytes", 32*1024)
	if !ok || gzipped.Load() || n < 32*1024 {
		t.Fatalf("ok %v (%s), %d bytes on the wire, gzip asked %v", ok, msg, n, gzipped.Load())
	}
}

func TestProbeFailsOnFrozenBodyAndWrongStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/teapot":
			w.WriteHeader(http.StatusTeapot)
			return
		case "/short":
			w.Write([]byte("short"))
			return
		}
		w.Header().Set("Content-Length", "65536")
		w.Write(make([]byte, 16*1024))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	defer srv.Close()
	o := probeInstance(t)
	if ok, n, _ := health.Probe(o, "a1", srv.URL+"/freeze", 32*1024); ok {
		t.Fatalf("a body frozen after 16 KB passed a 32 KB probe (%d bytes)", n)
	}
	if ok, _, _ := health.Probe(o, "a1", srv.URL+"/teapot", 0, 200); ok {
		t.Fatal("status 418 passed a probe expecting 200")
	}
	// A provider sees the class of the failure, never its text.
	for path, want := range map[string]string{"/freeze": "no_progress", "/teapot": "status_418", "/short": "short_body"} {
		if got := health.ProbeClass(o, "a1", srv.URL+path, 32*1024, 200); got != want {
			t.Errorf("%s: class %q, want %q", path, got, want)
		}
	}
}

// Closing the observer ends its probes at once, and a probe ended that way
// says nothing about the outbound.
func TestCloseEndsRunningProbesWithoutCountingThem(t *testing.T) {
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	cfg, err := serial.LoadJSONConfig(strings.NewReader(`{
		"log": {"loglevel": "none"},
		"outbounds": [{"tag": "a1", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}, "selection":{"balancer":"auto","cost":1,"probe":"silent"}}, {"tag": "block", "protocol": "blackhole"}],
		"routing": {"balancers":[{"tag":"auto","fallbackTag":"block","strategy":{"type":"leastCost"}}]},
		"healthObservatory": {
			"probes": {"profiles":[{"name":"silent","steps":[{"name":"transport","destinations": ["http://` + ln.Addr().String() + `/silent"], "timeout": "3s", "progressTimeout": "3s"}]}]}}
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
	o := health.FromInstance(inst)
	var st struct {
		Running int `json:"probes_running"`
		Paths   []struct {
			Outbound     string `json:"outbound"`
			State        string `json:"state"`
			ProbesFailed int    `json:"probes_failed"`
		} `json:"paths"`
	}
	status := func() {
		if err := json.Unmarshal(o.Status(), &st); err != nil {
			t.Fatal(err)
		}
	}
	health.QueueProbe(o, "a1")
	for status(); st.Running == 0; status() {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	o.Close()
	for status(); st.Running > 0 && time.Since(start) < 5*time.Second; status() {
		time.Sleep(10 * time.Millisecond)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the probe ran %v after the observer closed", took)
	}
	for _, p := range st.Paths {
		if p.Outbound == "a1" && (p.State == "down" || p.ProbesFailed != 0) {
			t.Fatalf("a probe ended by close counted against a1: %+v", p)
		}
	}
}
