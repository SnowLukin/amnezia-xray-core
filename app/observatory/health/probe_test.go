package health_test

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
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
	cfg, err := serial.LoadJSONConfig(strings.NewReader(`{
		"log": {"loglevel": "none"},
		"outbounds": [{"tag": "a1", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}}],
		"healthObservatory": {"subjectSelector": ["a"]}
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
	rule := &router.BalancingRule{Tag: "b", OutboundSelector: []string{"a"}, Strategy: "leastcost"}
	if err := r.ReloadRules(&router.Config{BalancingRule: []*router.BalancingRule{rule}}, true); err != nil {
		t.Logf("reload refused: %v", err)
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
		if r.URL.Path == "/teapot" {
			w.WriteHeader(http.StatusTeapot)
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
}
