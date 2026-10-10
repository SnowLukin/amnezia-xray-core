package health_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
)

// within waits up to d for the first event of typ since from that matches.
func (f *wireGuardFailover) within(from int, typ string, match func(health.Event) bool, d time.Duration) health.Event {
	w := f.events
	w.t.Helper()
	for deadline := time.Now().Add(d); ; time.Sleep(20 * time.Millisecond) {
		for _, e := range w.all[from:] {
			if e.Type == typ && (match == nil || match(e)) {
				return e
			}
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("no %s event within %v; got %+v", typ, d, w.all[from:])
		}
		f.mark()
	}
}

// A keep-alive connection through w1 that got its answer and waits, quiet,
// as the push connections of a phone do, keeps the tunnel of w1 awake once
// the balancer is on w2: the path works, and nothing else ends it. With
// sleep.closeIdleAfter, once nothing has chosen w1 for sleep.afterLastUse and the
// connection has received nothing for closeIdleAfter, the observer ends it
// with idle_closed; w1 then sleeps with reason idle the shortest
// sleep.afterLastUse later, once the end of the connection has crossed the tunnel.
// It sends nothing more, and its server at most the one passive keepalive
// that answers the end: a tunnel gone before it, the server would repeat
// its end of the connection.
func TestWireGuardUnchosenOutboundSleepsOnceItsIdleConnectionsEnd(t *testing.T) {
	const idle = 10 * time.Second
	ts := targetServer(t)
	f := startWireGuardWith(t, ts, `{"sweepInterval": "50ms"}`,
		`{"destination": "http://10.10.0.1/probe", "timeout": "8s", "progressTimeout": "8s"}`,
		`"sleep": {"afterLastUse": "10s", "closeIdleAfter": "10s"}`)
	if err := f.get("10.10.0.1", "/small", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := f.active(t); got != "w1" {
		t.Fatalf("the balancer chose %q, want w1", got)
	}
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
	ended := make(chan time.Time, 1)
	go func() {
		held.Read(make([]byte, 1))
		ended <- time.Now()
	}()

	from := f.mark()
	if !f.obs.Pin("auto", "w2", "") {
		t.Fatal("pin to w2 refused")
	}
	sw := f.first(from, "switch", nil)
	if sw.Fields["from"] != "w1" || sw.Fields["to"] != "w2" {
		t.Fatalf("switch %v, want from w1 to w2", sw.Fields)
	}
	closed := f.within(from, "idle_closed", about("w1"), 30*time.Second)
	if after := time.Duration(closed.Time-sw.Time) * time.Millisecond; closed.Fields["connections"] != float64(1) ||
		after < idle-50*time.Millisecond || after > idle+3*time.Second {
		t.Fatalf("idle close %v, %v after the switch; want the held connection, sleep.afterLastUse %v after", closed.Fields, after, idle)
	}
	for _, e := range f.events.since(from, "outbound_sleep") {
		if e.Fields["outbound"] == "w1" && e.Time < closed.Time {
			t.Fatalf("w1 slept under the held connection: %v", e.Fields)
		}
	}
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the held connection is still open after the idle close")
	}
	slept := f.within(from, "outbound_sleep", about("w1"), idle+10*time.Second)
	if slept.Fields["reason"] != "idle" || slept.Time < closed.Time {
		t.Fatalf("w1 slept with %v at %d, idle close at %d; want reason idle after the close", slept.Fields, slept.Time, closed.Time)
	}
	// The shortest sleep.afterLastUse, which is idle here too.
	if after := time.Duration(slept.Time-closed.Time) * time.Millisecond; after < idle-50*time.Millisecond ||
		after > idle+3*time.Second {
		t.Fatalf("w1 slept %v after the idle close, want %v, the shortest sleep.afterLastUse", after, idle)
	}
	sent, answered := f.w1.sent.Load(), f.w1.answered.Load()
	time.Sleep(5 * time.Second)
	if n, m := f.w1.sent.Load()-sent, f.w1.answered.Load()-answered; n != 0 || m > 1 {
		t.Fatalf("%d datagrams from the sleeping w1, %d from its server; want none, and at most a keepalive", n, m)
	}
	// w2, chosen, keeps working.
	if err := f.get("10.10.0.1", "/small", 10*time.Second); err != nil {
		t.Fatal("through w2:", err)
	}
}
