package health_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	stdnet "net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"golang.org/x/crypto/curve25519"
)

func wireGuardKeys(t *testing.T) (priv, pub string) {
	t.Helper()
	var k [32]byte
	if _, err := rand.Read(k[:]); err != nil {
		t.Fatal(err)
	}
	k[0] &= 248
	k[31] = k[31]&127 | 64
	p, err := curve25519.X25519(k[:], curve25519.Basepoint)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(k[:]), base64.StdEncoding.EncodeToString(p)
}

// udpGate stands between a WireGuard outbound and its server, as the
// network does, and can stop passing datagrams, as a censor does.
type udpGate struct {
	addr    string
	blocked atomic.Bool
	// Datagrams from the outbound, whatever became of them.
	sent atomic.Int64
}

func startUDPGate(t *testing.T, server string) *udpGate {
	t.Helper()
	front, err := stdnet.ListenUDP("udp", &stdnet.UDPAddr{IP: stdnet.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	to, err := stdnet.ResolveUDPAddr("udp", server)
	if err != nil {
		t.Fatal(err)
	}
	back, err := stdnet.DialUDP("udp", nil, to)
	if err != nil {
		t.Fatal(err)
	}
	g := &udpGate{addr: front.LocalAddr().String()}
	var client atomic.Pointer[stdnet.UDPAddr]
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		b := make([]byte, 2048)
		for {
			n, from, err := front.ReadFromUDP(b)
			if err != nil {
				return
			}
			client.Store(from)
			g.sent.Add(1)
			if !g.blocked.Load() {
				back.Write(b[:n])
			}
		}
	}()
	go func() {
		defer wg.Done()
		b := make([]byte, 2048)
		for {
			n, err := back.Read(b)
			if err != nil {
				return
			}
			if c := client.Load(); c != nil && !g.blocked.Load() {
				front.WriteToUDP(b[:n], c)
			}
		}
	}()
	t.Cleanup(func() {
		front.Close()
		back.Close()
		wg.Wait()
	})
	return g
}

func freePort(t *testing.T) int {
	t.Helper()
	c, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*stdnet.UDPAddr).Port
}

// wireGuardFailover runs a core with the WireGuard outbounds w1 and w2
// behind a leastCost balancer, w1 the cheaper. Both reach a WireGuard
// inbound of the same core through their own gates; behind the inbound,
// every destination is ts.
type wireGuardFailover struct {
	inst   *core.Instance
	obs    *health.Observer
	w1, w2 *udpGate
	events *eventWaiter
}

func startWireGuardFailover(t *testing.T, ts *httptest.Server, passive string) *wireGuardFailover {
	t.Helper()
	serverPriv, serverPub := wireGuardKeys(t)
	priv1, pub1 := wireGuardKeys(t)
	priv2, pub2 := wireGuardKeys(t)
	port := freePort(t)
	server := "127.0.0.1:" + strconv.Itoa(port)
	f := &wireGuardFailover{w1: startUDPGate(t, server), w2: startUDPGate(t, server)}
	outbound := func(tag, priv, addr, endpoint string) string {
		return `{"tag": "` + tag + `", "protocol": "wireguard", "settings": {"secretKey": "` + priv + `",
		  "address": ["` + addr + `/32"], "noKernelTun": true,
		  "peers": [{"publicKey": "` + serverPub + `", "endpoint": "` + endpoint + `", "allowedIPs": ["0.0.0.0/0"]}]}}`
	}
	// Probes take long enough for the first handshake of a tunnel under the
	// race detector, which slows its cryptography down to seconds.
	cfg, err := core.LoadConfig("json", strings.NewReader(`{
	  "log": {"loglevel": "none"},
	  "inbounds": [{"tag": "server", "listen": "127.0.0.1", "port": `+strconv.Itoa(port)+`, "protocol": "wireguard",
	    "settings": {"secretKey": "`+serverPriv+`", "peers": [
	      {"publicKey": "`+pub1+`", "allowedIPs": ["10.0.0.2/32"]},
	      {"publicKey": "`+pub2+`", "allowedIPs": ["10.0.0.3/32"]}]}}],
	  "outbounds": [
	    {"tag": "behind", "protocol": "freedom", "settings": {"redirect": "`+ts.Listener.Addr().String()+`",
	      "finalRules": [{"action": "allow"}]}},
	    `+outbound("w1", priv1, "10.0.0.2", f.w1.addr)+`,
	    `+outbound("w2", priv2, "10.0.0.3", f.w2.addr)+`,
	    {"tag": "block", "protocol": "blackhole"}
	  ],
	  "routing": {"rules": [{"inboundTag": ["server"], "outboundTag": "behind"},
	                        {"ip": ["10.10.0.0/24"], "balancerTag": "auto"}],
	    "balancers": [{"tag": "auto", "selector": ["w"], "fallbackTag": "block",
	      "strategy": {"type": "leastCost", "settings": {"costs": [{"match": "w1", "value": 1}, {"match": "w2", "value": 2}]}}}]},
	  "healthObservatory": {
	    "subjectSelector": ["w"],
	    "passive": `+passive+`,
	    "probes": [{"destination": "http://10.10.0.1/probe", "timeout": "8s", "progressTimeout": "8s"}],
	    "schedule": {"spacing": "10ms", "suspectCooldown": "1s"}
	  }
	}`))
	if err != nil {
		t.Fatal(err)
	}
	f.inst, err = core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.inst.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.inst.Close() })
	f.obs = health.FromInstance(f.inst)
	f.events = &eventWaiter{t: t, o: f.obs}
	return f
}

// get fetches path from target through the balancer, as a user does.
func (f *wireGuardFailover) get(target, path string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := core.Dial(ctx, f.inst, net.TCPDestination(net.ParseAddress(target), 80))
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", path, target); err != nil {
		return err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// mark drains the events so far and returns where the next ones start.
func (f *wireGuardFailover) mark() int {
	w := f.events
	raw, _ := w.o.DrainEvents()
	var evs []health.Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		w.t.Fatal(err)
	}
	w.all = append(w.all, evs...)
	return len(w.all)
}

// first waits for the first event of typ since from that matches.
func (f *wireGuardFailover) first(from int, typ string, match func(health.Event) bool) health.Event {
	w := f.events
	w.t.Helper()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		for _, e := range w.all[from:] {
			if e.Type == typ && (match == nil || match(e)) {
				return e
			}
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("no %s event; got %+v", typ, w.all[from:])
		}
		f.mark()
	}
}

func about(tag string) func(health.Event) bool {
	return func(e health.Event) bool { return e.Fields["outbound"] == tag }
}

func stateOf(tag, s string) func(health.Event) bool {
	return func(e health.Event) bool { return e.Fields["outbound"] == tag && e.Fields["state"] == s }
}

func (f *wireGuardFailover) active(t *testing.T) string {
	t.Helper()
	var st struct {
		Balancers []struct {
			Active string `json:"active"`
		} `json:"balancers"`
	}
	if err := json.Unmarshal(f.obs.Status(), &st); err != nil || len(st.Balancers) != 1 {
		t.Fatalf("status: %v", err)
	}
	return st.Balancers[0].Active
}

// targetServer answers /probe and /small at once, and dribbles /drip, a
// kilobyte every 50 ms.
func targetServer(t *testing.T) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/drip":
			w.Header().Set("Content-Length", strconv.Itoa(1<<20))
			chunk := make([]byte, 1024)
			for range 1 << 10 {
				if _, err := w.Write(chunk); err != nil {
					return
				}
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
			}
		default:
			w.Write(make([]byte, 1024))
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

// A WireGuard outbound whose server stops answering is ruled out by the
// connections users make, which it can no longer carry: each waits for
// the tunnel in vain, a path failure that needs no probe. Left by the
// balancer, it releases its tunnel at once and sends the dead address
// nothing more.
func TestWireGuardOutboundFailsOverWhenItsServerStopsAnswering(t *testing.T) {
	ts := targetServer(t)
	f := startWireGuardFailover(t, ts, `{"dialTimeout": "1s", "firstByteTimeout": "2s", "sweepInterval": "50ms"}`)
	if err := f.get("10.10.0.1", "/small", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := f.active(t); got != "w1" {
		t.Fatalf("the balancer chose %q, want w1", got)
	}
	from := f.mark()
	f.w1.blocked.Store(true)
	stop := make(chan struct{})
	var users sync.WaitGroup
	users.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
			users.Go(func() { f.get("10.10.0."+strconv.Itoa(1+i%2), "/small", 5*time.Second) })
		}
	})
	stopUsers := sync.OnceFunc(func() {
		close(stop)
		users.Wait()
	})
	defer stopUsers()
	suspect := f.first(from, "path_state", stateOf("w1", "suspect"))
	if suspect.Fields["strong"] != true || !strings.Contains(fmt.Sprint(suspect.Fields["kinds"]), "dial_timeout") {
		t.Fatalf("suspicion %v, want strong evidence of dial timeouts", suspect.Fields)
	}
	f.first(from, "path_state", stateOf("w1", "down"))
	sw := f.first(from, "switch", nil)
	if sw.Fields["from"] != "w1" || sw.Fields["to"] != "w2" || sw.Fields["reason"] != "failover" {
		t.Fatalf("switch %v, want failover from w1 to w2", sw.Fields)
	}
	for _, e := range f.events.since(from, "probe_start") {
		if e.Fields["outbound"] == "w1" {
			t.Fatalf("strong evidence was probed: %v", e.Fields)
		}
	}
	if slept := f.first(from, "outbound_sleep", about("w1")); slept.Fields["reason"] != "down" {
		t.Fatalf("w1 slept with %v, want reason down", slept.Fields)
	}
	// Awake, its device would repeat a handshake every 5 s.
	sent := f.w1.sent.Load()
	time.Sleep(6 * time.Second)
	if n := f.w1.sent.Load() - sent; n != 0 {
		t.Fatalf("%d datagrams to the dead address after w1 slept", n)
	}
	// Under the race detector the tunnels are slow enough for the users to
	// hold up the next connection.
	stopUsers()
	if err := f.get("10.10.0.1", "/small", 10*time.Second); err != nil {
		t.Fatal("after the failover:", err)
	}
}

// A WireGuard tunnel that freezes mid-transfer stalls the downloads in it.
// Stalls at two destinations make the outbound suspect, a probe confirms
// it, and the balancer moves to the next outbound, through traffic alone.
func TestWireGuardOutboundFailsOverWhenItsTunnelFreezes(t *testing.T) {
	ts := targetServer(t)
	f := startWireGuardFailover(t, ts, `{"dialTimeout": "2s", "firstByteTimeout": "2s", "stallTimeout": "1s",
	  "failures": 2, "distinctTargets": 2, "successBytes": 1048576, "partialStallMinBytes": 2048, "sweepInterval": "50ms"}`)
	if err := f.get("10.10.0.1", "/small", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := f.active(t); got != "w1" {
		t.Fatalf("the balancer chose %q, want w1", got)
	}
	var downloads sync.WaitGroup
	var conns []stdnet.Conn
	defer func() {
		// The frozen downloads end when they are closed.
		for _, c := range conns {
			c.Close()
		}
		downloads.Wait()
	}()
	got := make([]atomic.Int64, 2)
	for i := range got {
		conn, err := core.Dial(context.Background(), f.inst, net.TCPDestination(net.ParseAddress("10.10.0."+strconv.Itoa(1+i)), 80))
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
		fmt.Fprintf(conn, "GET /drip HTTP/1.1\r\nHost: target\r\nConnection: close\r\n\r\n")
		downloads.Go(func() {
			b := make([]byte, 4096)
			for {
				n, err := conn.Read(b)
				got[i].Add(int64(n))
				if err != nil {
					return
				}
			}
		})
	}
	for i := range got {
		for deadline := time.Now().Add(15 * time.Second); got[i].Load() < 4096; time.Sleep(20 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("download %d got %d bytes before the freeze", i, got[i].Load())
			}
		}
	}
	from := f.mark()
	f.w1.blocked.Store(true)
	suspect := f.first(from, "path_state", stateOf("w1", "suspect"))
	if suspect.Fields["strong"] == true || !strings.Contains(fmt.Sprint(suspect.Fields["kinds"]), "partial_stall") {
		t.Fatalf("suspicion %v, want stalls that need a probe", suspect.Fields)
	}
	probe := f.first(from, "probe_start", about("w1"))
	if probe.Fields["reason"] != "confirm" {
		t.Fatalf("probe %v, want one that confirms the suspicion", probe.Fields)
	}
	result := f.first(from, "probe_result", about("w1"))
	if result.Fields["ok"] != false {
		t.Fatalf("the probe through the frozen tunnel passed: %v", result.Fields)
	}
	f.first(from, "path_state", stateOf("w1", "down"))
	sw := f.first(from, "switch", nil)
	if sw.Fields["from"] != "w1" || sw.Fields["to"] != "w2" || sw.Fields["reason"] != "failover" {
		t.Fatalf("switch %v, want failover from w1 to w2", sw.Fields)
	}
	if err := f.get("10.10.0.1", "/small", 10*time.Second); err != nil {
		t.Fatal("after the failover:", err)
	}
}
