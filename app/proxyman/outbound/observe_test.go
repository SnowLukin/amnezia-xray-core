package outbound_test

import (
	"context"
	gonet "net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman/outbound"
	xctx "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	obs "github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/infra/conf/serial"
	_ "github.com/xtls/xray-core/main/distro/all"
)

type countingObserver struct {
	inner                      obs.ConnectionObserver
	starts, dialedOK, finished atomic.Int64
	mu                         sync.Mutex
	ids                        []xctx.ID
}

func (c *countingObserver) StartDial(ctx context.Context, tag string, server net.Destination) obs.DialTracker {
	c.starts.Add(1)
	c.mu.Lock()
	c.ids = append(c.ids, xctx.IDFromContext(ctx))
	c.mu.Unlock()
	return &countingTracker{c: c, inner: c.inner.StartDial(ctx, tag, server)}
}

type countingTracker struct {
	c     *countingObserver
	inner obs.DialTracker
}

func (t *countingTracker) Dialed(conn gonet.Conn, err error, r, w stats.Counter) (stats.Counter, stats.Counter) {
	if err == nil {
		t.c.dialedOK.Add(1)
	}
	if t.inner == nil {
		return r, w
	}
	return t.inner.Dialed(conn, err, r, w)
}

func (t *countingTracker) Finished() {
	t.c.finished.Add(1)
	if t.inner != nil {
		t.inner.Finished()
	}
}

func startInstance(t *testing.T, js string) *core.Instance {
	t.Helper()
	cfg, err := serial.DecodeJSONConfig(strings.NewReader(js))
	if err != nil {
		t.Fatal(err)
	}
	pb, err := cfg.Build()
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(pb)
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	return inst
}

func observe(t *testing.T, inst *core.Instance, tag string) *countingObserver {
	h := inst.GetFeature(obs.ManagerType()).(obs.Manager).GetHandler(tag)
	c := &countingObserver{}
	outbound.WrapObserver(h, func(in obs.ConnectionObserver) obs.ConnectionObserver {
		if in == nil {
			t.Fatal("handler has no observer")
		}
		c.inner = in
		return c
	})
	return c
}

// oneConnection dispatches like the core does internally: without a session id.
func oneConnection(t *testing.T, inst *core.Instance) {
	conn, err := core.Dial(context.Background(), inst, net.TCPDestination(net.DomainAddress("example.test"), 80))
	if err != nil {
		t.Fatal(err)
	}
	conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.test\r\n\r\n"))
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	for {
		if _, err := conn.Read(buf); err != nil {
			break
		}
	}
	conn.Close()
}

func deadPort(t *testing.T) int {
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*gonet.TCPAddr).Port
	ln.Close()
	return port
}

func TestMuxConnectionsAreObserved(t *testing.T) {
	for _, mux := range []bool{false, true} {
		muxJSON := ""
		if mux {
			muxJSON = `, "mux": {"enabled": true, "concurrency": 8}`
		}
		inst := startInstance(t, `{
			"log": {"loglevel": "none"},
			"outbounds": [{"tag": "t1", "protocol": "trojan",
				"settings": {"servers": [{"address": "127.0.0.1", "port": `+strconv.Itoa(deadPort(t))+`, "password": "x"}]}`+muxJSON+`}],
			"healthObservatory": {"subjectSelector": ["t"]}
		}`)
		c := observe(t, inst, "t1")
		for i := 0; i < 3; i++ {
			oneConnection(t, inst)
		}
		time.Sleep(200 * time.Millisecond)
		if c.starts.Load() == 0 {
			t.Errorf("mux=%v: three connections to a dead server were not observed", mux)
		}
		inst.Close()
	}
}

func TestRetriesOfOneConnectionShareAnID(t *testing.T) {
	inst := startInstance(t, `{
		"log": {"loglevel": "none"},
		"outbounds": [{"tag": "t1", "protocol": "trojan",
			"settings": {"servers": [{"address": "127.0.0.1", "port": `+strconv.Itoa(deadPort(t))+`, "password": "x"}]}}],
		"healthObservatory": {"subjectSelector": ["t"]}
	}`)
	defer inst.Close()
	c := observe(t, inst, "t1")
	for i := 0; i < 3; i++ {
		oneConnection(t, inst)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	distinct := map[xctx.ID]bool{}
	for _, id := range c.ids {
		if id == 0 {
			t.Fatalf("a dial without a session id reached the observer without one")
		}
		distinct[id] = true
	}
	if len(c.ids) <= 3 || len(distinct) != 3 {
		t.Fatalf("%d dials of 3 connections carried %d ids, want one id per connection", len(c.ids), len(distinct))
	}
}

// VLESS pre-connections are dialed through the dialer of the first
// connection after it ended; each must still be finished.
func TestPreconnectionsAreFinished(t *testing.T) {
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
			go func() {
				c.SetReadDeadline(time.Now().Add(5 * time.Second))
				b := make([]byte, 4096)
				c.Read(b)
				c.Close()
			}()
		}
	}()
	inst := startInstance(t, `{
		"log": {"loglevel": "none"},
		"outbounds": [{"tag": "v1", "protocol": "vless",
			"settings": {"address": "127.0.0.1", "port": `+strconv.Itoa(ln.Addr().(*gonet.TCPAddr).Port)+`,
				"id": "27848739-7e62-4138-9fd3-098a63964b6b", "encryption": "none", "testpre": 1}}],
		"healthObservatory": {"subjectSelector": ["v"]}
	}`)
	// The instance stays open: closing the VLESS outbound closes the channel
	// its pre-connection goroutines still send to, which the race detector
	// reports.
	c := observe(t, inst, "v1")
	const n = 20
	for i := 0; i < n; i++ {
		oneConnection(t, inst)
	}
	time.Sleep(300 * time.Millisecond)
	if c.finished.Load() < n {
		t.Errorf("%d connections ended, %d trackers finished", n, c.finished.Load())
	}
}
