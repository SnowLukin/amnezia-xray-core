package health

import (
	"context"
	"encoding/json"
	goerrors "errors"
	gonet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestObserver() (*Observer, *fakeClock) {
	s := newSettings(&Config{SubjectSelector: []string{"a"}})
	clk := &fakeClock{t: t0}
	o := &Observer{
		s:        s,
		e:        newEngine(s, 1),
		kick:     make(chan struct{}, 1),
		trackers: map[*connTracker]struct{}{},
		watching: map[*connTracker]struct{}{},
		clock:    clk.now,
		base:     t0.Add(-time.Second),
	}
	// The tests sweep by hand.
	o.sweeping = true
	return o, clk
}

var lastConn uint32

func openTracker(o *Observer, target string, udp bool) (*connTracker, *trackerReadCounter, *trackerWriteCounter) {
	lastConn++
	t := &connTracker{o: o, tag: "a1", target: target, udp: udp, started: o.nowNano(), conn: lastConn}
	r, w := t.Dialed(nil, nil, nil, nil)
	return t, r.(*trackerReadCounter), w.(*trackerWriteCounter)
}

func failures(o *Observer, kind failureKind) int64 {
	return o.e.path("a1").stats.failuresByKind[kind]
}

func TestKeepAliveWithSmallAnswerIsNotAPartialStall(t *testing.T) {
	o, clk := newTestObserver()
	tr, r, w := openTracker(o, "x.test", false)
	w.Add(400)
	r.Add(2 * 1024)
	clk.advance(9 * time.Second)
	tr.sweep(o.nowNano())
	if n := failures(o, failPartialStall) + failures(o, failStall); n != 0 {
		t.Fatalf("idle keep-alive counted as %d stalls", n)
	}
}

func TestSilenceAfterFirstKilobytesIsAPartialStall(t *testing.T) {
	o, clk := newTestObserver()
	tr, r, w := openTracker(o, "x.test", false)
	w.Add(400)
	r.Add(12 * 1024)
	clk.advance(9 * time.Second)
	tr.sweep(o.nowNano())
	if failures(o, failPartialStall) != 1 {
		t.Fatalf("a connection frozen after 12 KB was not noticed")
	}
}

func TestSmallWriteAfterDownloadIsNotAStall(t *testing.T) {
	o, clk := newTestObserver()
	tr, r, w := openTracker(o, "x.test", false)
	w.Add(400)
	r.Add(30 * 1024)
	// An HTTP/2 window update after the answer.
	w.Add(13)
	clk.advance(9 * time.Second)
	tr.sweep(o.nowNano())
	if failures(o, failStall) != 0 {
		t.Fatalf("a control write after a finished download counted as a stall")
	}
}

func TestUnansweredRequestIsAStall(t *testing.T) {
	o, clk := newTestObserver()
	tr, r, w := openTracker(o, "x.test", false)
	w.Add(400)
	r.Add(2 * 1024)
	w.Add(500)
	clk.advance(9 * time.Second)
	tr.sweep(o.nowNano())
	if failures(o, failStall) != 1 {
		t.Fatalf("a request without an answer was not noticed")
	}
}

func TestNoFirstByteIsNoticed(t *testing.T) {
	o, clk := newTestObserver()
	tr, _, w := openTracker(o, "x.test", false)
	w.Add(400)
	clk.advance(6 * time.Second)
	tr.sweep(o.nowNano())
	if failures(o, failNoResponse) != 1 {
		t.Fatalf("a request without any answer was not noticed")
	}
}

func TestNothingSentOrReceivedSinceDialIsNoticed(t *testing.T) {
	o, clk := newTestObserver()
	tr, _, _ := openTracker(o, "x.test", false)
	clk.advance(3 * time.Second)
	if watching, _ := tr.sweep(o.nowNano()); !watching || failures(o, failNoResponse) != 0 {
		t.Fatalf("3 s after the dial: watching %v, failures %d", watching, failures(o, failNoResponse))
	}
	clk.advance(3 * time.Second)
	tr.sweep(o.nowNano())
	if failures(o, failNoResponse) != 1 {
		t.Fatal("a connection with no byte either way 6 s after its dial was not noticed")
	}
}

func TestConnectionEndedByFailureBeforeAnyAnswerIsNoticed(t *testing.T) {
	o, _ := newTestObserver()
	broken := goerrors.New("failed to read response version > io: read/write on closed pipe")
	tr, _, w := openTracker(o, "x.test", false)
	w.Add(400)
	tr.Finished(broken)
	unsent, _, _ := openTracker(o, "y.test", false)
	unsent.Finished(broken)
	if failures(o, failClosed) != 2 {
		t.Fatalf("%d of 2 connections ended by a failure before any answer were noticed", failures(o, failClosed))
	}
	answered, r, w := openTracker(o, "z.test", false)
	w.Add(400)
	r.Add(100)
	answered.Finished(broken)
	canceled, _, w := openTracker(o, "w.test", false)
	w.Add(400)
	canceled.Finished(context.Canceled)
	quiet, _, _ := openTracker(o, "v.test", false)
	quiet.Finished(nil)
	if failures(o, failClosed) != 2 {
		t.Fatal("an answered, canceled or cleanly ended connection counted as a failure")
	}
}

func TestUDPIsNotJudgedBySilence(t *testing.T) {
	o, clk := newTestObserver()
	tr, r, w := openTracker(o, "dns.test", true)
	w.Add(100)
	r.Add(10 * 1024)
	clk.advance(10 * time.Second)
	if watching, _ := tr.sweep(o.nowNano()); watching {
		t.Fatalf("a UDP flow is watched for silence")
	}
	if n := failures(o, failNoResponse) + failures(o, failPartialStall); n != 0 {
		t.Fatalf("UDP silence counted as %d failures", n)
	}
}

func TestSplicedConnectionIsNotJudgedBySilence(t *testing.T) {
	o, clk := newTestObserver()
	tr, r, w := openTracker(o, "x.test", false)
	w.Add(400)
	r.Add(10 * 1024)
	r.Spliced()
	clk.advance(9 * time.Second)
	tr.sweep(o.nowNano())
	if failures(o, failPartialStall) != 0 {
		t.Fatalf("a spliced download counted as a stall")
	}
}

func TestDialTimeoutAndStuckDial(t *testing.T) {
	o, clk := newTestObserver()
	tr := &connTracker{o: o, tag: "a1", started: o.nowNano(), conn: 1}
	clk.advance(7 * time.Second)
	watching, drop := tr.sweep(o.nowNano())
	if failures(o, failDialTimeout) != 1 || !watching || drop {
		t.Fatalf("slow dial: failures %d watching %v drop %v", failures(o, failDialTimeout), watching, drop)
	}
	clk.advance(30 * time.Second)
	if _, drop := tr.sweep(o.nowNano()); !drop {
		t.Fatalf("a dial that never reported back is kept forever")
	}
}

func TestWitnessConnectionEndsTheNetworkPause(t *testing.T) {
	s := newSettings(&Config{
		SubjectSelector: []string{"a"},
		Connectivity:    &ConnectivityConfig{Outbound: "direct", Destination: "http://x/204", Witnesses: []string{"direct"}},
	})
	clk := &fakeClock{t: t0}
	o := &Observer{s: s, e: newEngine(s, 1), kick: make(chan struct{}, 1), clock: clk.now, base: t0.Add(-time.Second),
		trackers: map[*connTracker]struct{}{}, watching: map[*connTracker]struct{}{}}
	o.e.onConnectivity(probeResult{ok: false, err: "down"}, t0)
	if !o.e.networkDown {
		t.Fatal("a failed check did not pause decisions")
	}
	if tr := o.StartDial(context.Background(), "proxy", net.Destination{}, nil); tr != nil {
		t.Fatalf("an outbound that is neither observed nor a witness is followed")
	}
	tr := o.StartDial(context.Background(), "direct", net.Destination{}, nil)
	r, _ := tr.Dialed(nil, nil, nil, nil)
	r.Add(10 * 1024)
	if !o.e.networkDown {
		t.Fatal("a few kilobytes ended the pause")
	}
	r.Add(20 * 1024)
	if o.e.networkDown || !o.e.queued("", reasonConnectivity) {
		t.Fatalf("a working direct connection: paused %v, check queued %v", o.e.networkDown, o.e.queued("", reasonConnectivity))
	}
	select {
	case <-o.kick:
	default:
		t.Fatal("the queued check does not wake the loop")
	}
}

// loopbackPair returns a dialed TCP connection and its peer, after the peer
// sent sent bytes, like a server that finished the outer handshake.
func loopbackPair(t *testing.T, sent int) (gonet.Conn, gonet.Conn) {
	t.Helper()
	ln, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	accepted := make(chan gonet.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	c, err := gonet.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	srv := <-accepted
	t.Cleanup(func() { c.Close(); srv.Close() })
	if _, err := srv.Write(make([]byte, sent)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	return c, srv
}

func TestSplicedProgressCountsFromTheSplice(t *testing.T) {
	c, srv := loopbackPair(t, 10*1024)
	if _, ok := kernelReceived(rawConnOf(c)); !ok {
		t.Skip("the kernel does not report received bytes here")
	}
	o, clk := newTestObserver()
	tr, r, w := openTracker(o, "x.test", false)
	tr.Dialed(c, nil, nil, nil)
	w.Add(400)
	r.Add(4 * 1024)
	r.Spliced()
	clk.advance(9 * time.Second)
	tr.sweep(o.nowNano())
	if n := failures(o, failPartialStall); n != 0 {
		t.Fatalf("a keep-alive with a 4 KB answer counted as a stall: the handshake before the splice was taken for data")
	}
	if _, err := srv.Write(make([]byte, 30*1024)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if watching, _ := tr.sweep(o.nowNano()); watching || o.e.path("a1").status != statusUp {
		t.Fatalf("30 KB after the splice: watching %v, a1 %v, want a success", watching, o.e.path("a1").status)
	}
}

func TestDownOutboundEndsConnectionsThatGotNothing(t *testing.T) {
	o, clk := newTestObserver()
	ended := map[string]bool{}
	dial := func(tag, name string) *connTracker {
		return o.StartDial(context.Background(), tag, net.Destination{}, func() { ended[name] = true }).(*connTracker)
	}
	dialing := dial("a1", "dialing")
	waiting := dial("a1", "waiting")
	waiting.Dialed(nil, nil, nil, nil)
	answered := dial("a1", "answered")
	r, _ := answered.Dialed(nil, nil, nil, nil)
	r.Add(100)
	other := dial("a2", "other")
	other.Dialed(nil, nil, nil, nil)
	clk.advance(time.Second)
	decided := clk.now()
	clk.advance(time.Second)
	dial("a1", "later")

	o.closePending(downMark{tag: "a1", at: decided, reason: "traffic", pending: true})
	want := map[string]bool{"dialing": true, "waiting": true}
	for _, name := range []string{"dialing", "waiting", "answered", "other", "later"} {
		if ended[name] != want[name] {
			t.Errorf("%s: ended %v, want %v", name, ended[name], want[name])
		}
	}

	// How they end is the observer's doing, not a failure of a1.
	dialing.Dialed(nil, goerrors.New("dial tcp: i/o timeout"), nil, nil)
	waiting.Finished(goerrors.New("io: read/write on closed pipe"))
	clk.advance(time.Minute)
	waiting.sweep(o.nowNano())
	if n := failures(o, failDial) + failures(o, failClosed) + failures(o, failNoResponse); n != 0 {
		t.Fatalf("ended connections counted as %d failures", n)
	}
	raw, _ := o.e.events.drain()
	var evs []Event
	_ = json.Unmarshal(raw, &evs)
	if len(evs) != 1 || evs[0].Type != "pending_closed" || evs[0].Fields["connections"] != float64(2) {
		t.Fatalf("events %s, want one pending_closed for two connections", raw)
	}
}

func TestDialsStartedInThePauseDoNotJudgeAfterIt(t *testing.T) {
	s := newSettings(&Config{
		SubjectSelector: []string{"a"},
		Connectivity:    &ConnectivityConfig{Outbound: "direct", Destination: "http://x/204"},
	})
	clk := &fakeClock{t: t0}
	o := &Observer{s: s, e: newEngine(s, 1), kick: make(chan struct{}, 1), clock: clk.now, base: t0.Add(-time.Second),
		trackers: map[*connTracker]struct{}{}, watching: map[*connTracker]struct{}{}, sweeping: true}
	o.e.onConnectivity(probeResult{ok: false, err: "down"}, clk.now())
	if !o.e.networkDown {
		t.Fatal("a failed check did not pause decisions")
	}
	var dials []*connTracker
	for i := 0; i < 3; i++ {
		dials = append(dials, o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker))
		clk.advance(time.Second)
	}
	o.e.sawSuccess("traffic", clk.now())
	if o.e.networkDown {
		t.Fatal("a working connection did not end the pause")
	}
	clk.advance(6 * time.Second)
	for _, d := range dials {
		d.sweep(o.nowNano())
	}
	if n := failures(o, failDialTimeout); n != 0 {
		t.Fatalf("%d dials that started in the pause counted against a1 after it", n)
	}
	fresh := o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker)
	clk.advance(7 * time.Second)
	fresh.sweep(o.nowNano())
	if n := failures(o, failDialTimeout); n != 1 {
		t.Fatalf("a dial after the pause counted %d times, want once", n)
	}
}

func TestSplicedFreezeIsNoticedByKernelProgress(t *testing.T) {
	c, srv := loopbackPair(t, 5*1024)
	if _, ok := kernelReceived(rawConnOf(c)); !ok {
		t.Skip("the kernel does not report received bytes here")
	}
	o, clk := newTestObserver()
	tr, r, w := openTracker(o, "x.test", false)
	tr.Dialed(c, nil, nil, nil)
	w.Add(400)
	r.Spliced()
	if _, err := srv.Write(make([]byte, 12*1024)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if watching, _ := tr.sweep(o.nowNano()); !watching {
		t.Fatalf("a young spliced connection below successBytes is not watched")
	}
	clk.advance(9 * time.Second)
	tr.sweep(o.nowNano())
	if failures(o, failPartialStall) != 1 {
		t.Fatalf("a spliced connection frozen after 12 KB was not noticed")
	}
}

func TestSlowSplicedDownloadIsNotAStall(t *testing.T) {
	c, srv := loopbackPair(t, 5*1024)
	if _, ok := kernelReceived(rawConnOf(c)); !ok {
		t.Skip("the kernel does not report received bytes here")
	}
	o, clk := newTestObserver()
	tr, r, w := openTracker(o, "x.test", false)
	tr.Dialed(c, nil, nil, nil)
	w.Add(400)
	r.Spliced()
	for i := 0; i < 4; i++ {
		if _, err := srv.Write(make([]byte, 4*1024)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
		tr.sweep(o.nowNano())
		clk.advance(5 * time.Second)
	}
	tr.sweep(o.nowNano())
	if n := failures(o, failPartialStall) + failures(o, failStall); n != 0 {
		t.Fatalf("a slow spliced download counted as %d stalls", n)
	}
}

func TestRequestOfAnotherRunIsIgnored(t *testing.T) {
	o, clk := newTestObserver()
	o.run = "r2"
	o.e.updateCandidates(o.e.addSelector("auto", newSelectorSettings(&SelectorConfig{StartPinned: "a1"})), []string{"a1", "b1"}, clk.now())
	if o.Release("auto", "r1") || o.e.selectors["auto"].pinned != "a1" {
		t.Fatal("a release meant for the previous run lifted the pin")
	}
	if !o.Release("auto", "r2") || o.e.selectors["auto"].pinned != "" {
		t.Fatal("a release for this run did not lift the pin")
	}
	if !o.Pin("auto", "b1", "") || o.e.selectors["auto"].pinned != "b1" {
		t.Fatal("a pin without a run was refused")
	}
}

func TestDialsOfThePreviousNetworkDoNotJudgeTheNewOne(t *testing.T) {
	o, clk := newTestObserver()
	o.e.updateCandidates(o.e.addSelector("auto", newSelectorSettings(&SelectorConfig{})), []string{"a1", "b1"}, clk.now())
	var dials []*connTracker
	for i := 0; i < 3; i++ {
		dials = append(dials, o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker))
	}
	o.NetworkChanged("lte")
	clk.advance(7 * time.Second)
	for _, d := range dials {
		d.sweep(o.nowNano())
	}
	if n := failures(o, failDialTimeout); n != 0 {
		t.Fatalf("%d dials started on the old network counted against a1", n)
	}
	fresh := o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker)
	clk.advance(7 * time.Second)
	fresh.sweep(o.nowNano())
	if n := failures(o, failDialTimeout); n != 1 {
		t.Fatalf("a dial in the new network counted %d times, want once", n)
	}
}

func TestWatchEndsWithoutDeadlines(t *testing.T) {
	o, _ := newTestObserver()
	tr, r, w := openTracker(o, "x.test", false)
	w.Add(400)
	r.Add(30 * 1024)
	if watching, _ := tr.sweep(o.nowNano()); watching {
		t.Fatalf("a finished download keeps the sweep running")
	}
}
