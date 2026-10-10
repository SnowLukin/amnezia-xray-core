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
	"github.com/xtls/xray-core/features/outbound"
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
	// Handshake responses of the server still to lose, as the network
	// loses a datagram now and then.
	loseResponses atomic.Int64
	// Datagrams from the outbound, whatever became of them.
	sent atomic.Int64
	// Datagrams from the server, whatever became of them.
	answered atomic.Int64
}

// waitQuiet waits until neither side has sent anything for d.
func (g *udpGate) waitQuiet(t *testing.T, d time.Duration) {
	t.Helper()
	n, held := g.sent.Load()+g.answered.Load(), time.Now()
	for deadline := held.Add(d + 15*time.Second); time.Since(held) < d; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the tunnel never went quiet")
		}
		if m := g.sent.Load() + g.answered.Load(); m != n {
			n, held = m, time.Now()
		}
	}
}

// lost tells whether the datagram b from the server is a handshake response
// the gate is to lose.
func (g *udpGate) lost(b []byte) bool {
	if len(b) != wireGuardResponseSize || b[0] != wireGuardResponseType {
		return false
	}
	for {
		n := g.loseResponses.Load()
		if n <= 0 {
			return false
		}
		if g.loseResponses.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

// The handshake response of plain WireGuard: its type and size.
const (
	wireGuardResponseType = 2
	wireGuardResponseSize = 92
)

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
			g.answered.Add(1)
			if c := client.Load(); c != nil && !g.blocked.Load() && !g.lost(b[:n]) {
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
	// Probes take long enough for the first handshake of a tunnel under the
	// race detector, which slows its cryptography down to seconds.
	return startWireGuard(t, ts, passive, `{"destination": "http://10.10.0.1/probe", "timeout": "8s", "progressTimeout": "8s"}`)
}

// startWireGuard is startWireGuardFailover with the probe of the observer.
func startWireGuard(t *testing.T, ts *httptest.Server, passive, probe string) *wireGuardFailover {
	t.Helper()
	return startWireGuardWith(t, ts, passive, probe, "")
}

// startWireGuardWith is startWireGuard with more settings of the observer,
// such as "sleep", as JSON members.
func startWireGuardWith(t *testing.T, ts *httptest.Server, passive, probe, more string) *wireGuardFailover {
	t.Helper()
	if more != "" {
		more = ", " + more
	}
	serverPriv, serverPub := wireGuardKeys(t)
	priv1, pub1 := wireGuardKeys(t)
	priv2, pub2 := wireGuardKeys(t)
	port := freePort(t)
	server := "127.0.0.1:" + strconv.Itoa(port)
	f := &wireGuardFailover{w1: startUDPGate(t, server), w2: startUDPGate(t, server)}
	outbound := func(tag, priv, addr, endpoint string) string {
		cost := "1"
		if tag == "w2" {
			cost = "2"
		}
		return `{"tag": "` + tag + `", "protocol": "wireguard", "path": {"balancer": "auto", "cost": ` + cost + `, "probe": "wg"}, "settings": {"secretKey": "` + priv + `",
		  "address": ["` + addr + `/32"], "noKernelTun": true,
		  "peers": [{"publicKey": "` + serverPub + `", "endpoint": "` + endpoint + `", "allowedIPs": ["0.0.0.0/0"]}]}}`
	}
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
	    "balancers": [{"tag": "auto", "fallbackTag": "block",
	      "strategy": {"type": "leastCost", "settings": {}}}]},
	  "healthObservatory": {
	    "passive": `+passive+`,
	    "probes": [`+strings.Replace(probe, `{`, `{"name":"wg",`, 1)+`],
	    "schedule": {"minProbeInterval": "10ms", "suspectCooldown": "1s", "hostProbes": {"reuseWindow": "1ms"}}`+more+`
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

// users has users connect through the balancer every 200 ms, to two
// destinations, until the returned function is called.
func (f *wireGuardFailover) users() (stop func()) {
	done := make(chan struct{})
	var users sync.WaitGroup
	users.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			case <-time.After(200 * time.Millisecond):
			}
			users.Go(func() { f.get("10.10.0."+strconv.Itoa(1+i%2), "/small", 5*time.Second) })
		}
	})
	return sync.OnceFunc(func() {
		close(done)
		users.Wait()
	})
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
	stopUsers := f.users()
	defer stopUsers()
	suspect := f.first(from, "path_state", stateOf("w1", "suspect"))
	if suspect.Fields["is_strong"] != true || !strings.Contains(fmt.Sprint(suspect.Fields["kinds"]), "dial_timeout") {
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

// A probe that wakes a sleeping WireGuard outbound passes although the
// network loses the handshake response of its new device. The tunnel asks
// for the handshake again once the server has answered nothing for a
// second, well within progressTimeout. The device alone repeats it only
// device.RekeyTimeout after the first: the probe would fail for no
// progress and put a working outbound in quarantine.
func TestWireGuardProbeWakesThroughALostHandshakeResponse(t *testing.T) {
	ts := targetServer(t)
	// progressTimeout is below RekeyTimeout, as the default of 4 s is, and
	// leaves the race detector room for the two handshakes.
	f := startWireGuard(t, ts, `{"sweepInterval": "50ms"}`,
		`{"destination": "http://10.10.0.1/probe", "timeout": "8s", "progressTimeout": "4800ms"}`)
	// The balancer gets its candidates, and w1 its first tunnel.
	if err := f.get("10.10.0.1", "/small", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	// The connection closes before w1 sleeps: the server would otherwise
	// repeat its end of it to the next device, which the gate reaches as
	// well, and that would answer the waiting probe.
	f.w1.waitQuiet(t, 500*time.Millisecond)
	w1 := f.inst.GetFeature(outbound.ManagerType()).(outbound.Manager).GetHandler("w1").(outbound.Sleeper)
	for deadline := time.Now().Add(10 * time.Second); !w1.Sleep(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("w1 refused to sleep after its connection")
		}
	}

	f.w1.loseResponses.Store(1)
	from := f.mark()
	if _, err := f.obs.RequestProbe(health.ProbeRequest{Balancer: "auto", Outbounds: []string{"w1"}}); err != nil {
		t.Fatal(err)
	}
	result := f.first(from, "probe_result", about("w1"))
	if left := f.w1.loseResponses.Load(); left != 0 {
		t.Fatalf("the probe woke no tunnel: %d handshake responses left to lose", left)
	}
	if result.Fields["is_ok"] != true {
		t.Fatalf("the probe through the woken w1 failed: %v", result.Fields)
	}
}

// A connection opened through w1 before its server stopped answering, and
// quiet since, as the push and DNS connections of a phone are, holds the
// tunnel: awake, its device would repeat the handshake to the dead address
// for as long as the connection lasts. stallTimeout after w1 is found down
// on strong evidence, the observer ends it as silent; w1 then sleeps with
// reason down and sends the dead address nothing more.
func TestWireGuardOutboundSleepsOnceItsSilentConnectionsEnd(t *testing.T) {
	const stall = 2 * time.Second
	ts := targetServer(t)
	f := startWireGuardFailover(t, ts, `{"dialTimeout": "1s", "firstByteTimeout": "2s", "stallTimeout": "2s",
	  "sweepInterval": "50ms"}`)
	if err := f.get("10.10.0.1", "/small", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := f.active(t); got != "w1" {
		t.Fatalf("the balancer chose %q, want w1", got)
	}
	// A keep-alive connection that got its answer and waits for the next
	// request, which never comes.
	held, err := core.Dial(context.Background(), f.inst, net.TCPDestination(net.ParseAddress("10.10.0.1"), 80))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if _, err := fmt.Fprintf(held, "GET /small HTTP/1.1\r\nHost: target\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(held), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}
	ended := make(chan struct{})
	go func() {
		held.Read(make([]byte, 1))
		close(ended)
	}()

	from := f.mark()
	f.w1.blocked.Store(true)
	stopUsers := f.users()
	defer stopUsers()
	down := f.first(from, "path_state", stateOf("w1", "down"))
	sw := f.first(from, "switch", nil)
	if sw.Fields["from"] != "w1" || sw.Fields["to"] != "w2" {
		t.Fatalf("switch %v, want failover from w1 to w2", sw.Fields)
	}
	closed := f.first(from, "pending_closed", func(e health.Event) bool {
		return e.Fields["outbound"] == "w1" && e.Fields["is_silent"] == true
	})
	due := max(down.Time+stall.Milliseconds(), sw.Time)
	if after := time.Duration(closed.Time-down.Time) * time.Millisecond; closed.Fields["connections"] != float64(1) ||
		after < stall-10*time.Millisecond || closed.Time < sw.Time || time.Duration(closed.Time-due)*time.Millisecond > 2*time.Second {
		t.Fatalf("silent close %v at %d, %v after the decision; want the held connection between %d and %d after stallTimeout and switch",
			closed.Fields, closed.Time, after, due, due+2000)
	}
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the held connection is still open after the silent close")
	}
	slept := f.first(from, "outbound_sleep", about("w1"))
	if slept.Fields["reason"] != "down" || slept.Time < closed.Time {
		t.Fatalf("w1 slept with %v at %d, silent close at %d; want reason down after the close: the held "+
			"connection keeps it awake until then", slept.Fields, slept.Time, closed.Time)
	}
	if after := time.Duration(slept.Time-closed.Time) * time.Millisecond; after > 2*time.Second {
		t.Fatalf("w1 slept %v after the silent close, want at once", after)
	}
	// Awake, its device would repeat a handshake every 5 s.
	sent := f.w1.sent.Load()
	time.Sleep(6 * time.Second)
	if n := f.w1.sent.Load() - sent; n != 0 {
		t.Fatalf("%d datagrams to the dead address after w1 slept", n)
	}
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
	  "failuresToSuspect": 2, "minDistinctTargets": 2, "successBytes": 1048576, "partialStallMinBytes": 2048, "sweepInterval": "50ms"}`)
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
	if suspect.Fields["is_strong"] == true || !strings.Contains(fmt.Sprint(suspect.Fields["kinds"]), "partial_stall") {
		t.Fatalf("suspicion %v, want stalls that need a probe", suspect.Fields)
	}
	probe := f.first(from, "probe_start", about("w1"))
	if probe.Fields["reason"] != "confirm" {
		t.Fatalf("probe %v, want one that confirms the suspicion", probe.Fields)
	}
	result := f.first(from, "probe_result", about("w1"))
	if result.Fields["is_ok"] != false {
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
