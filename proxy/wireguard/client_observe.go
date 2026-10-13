package wireguard

import "github.com/xtls/xray-core/common/safe"

import (
	"context"
	"encoding/hex"
	goerrors "errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amnezia-vpn/amneziawg-go/v3/device"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// The observer hook lives here, apart from client.go, so that Process keeps
// following upstream.
//
// The client reaches its server once for all its connections, so the
// observer of the outbound sees no dial of its own. Each connection through
// the tunnel is reported as a dial of the server instead (see
// outbound.TunnelDialer), which ends when the server answers anything after
// the connection entered the tunnel: the analogue of a dial to the server
// for other proxies. On a working tunnel the answer is the first packet of
// the connection itself, or traffic of others; a tunnel that is down or
// blocked answers nothing, and the observer finds the dial timed out.
// Inside the tunnel the connection is judged by its traffic, as any other.

const (
	// A connection the server has not answered for this long asks it for a
	// handshake, which a working server answers whatever the destination:
	// a destination that drops the connection is told from a dead tunnel
	// within a round trip, not when the server sends its keepalive, 10 s
	// after the first packet. A working tunnel answers a connection within
	// a round trip, so a busy one or a quick destination asks for nothing.
	askHandshakeAfter = time.Second
	// While a connection waits for the answer, it checks this often.
	answerPoll = 100 * time.Millisecond
	// Without an answer for this long the tunnel does not work: it covers a
	// lost handshake, retried after device.RekeyTimeout, and the wait of a
	// handshake asked for right after another one.
	answerWait = 2*device.RekeyTimeout + time.Second
)

var errNoAnswer = goerrors.New("the server answered nothing through the tunnel")

// tunnelServer is the server an observer is told the outbound dials: the
// endpoint the tunnel reaches.
func tunnelServer(conf *DeviceConfig) net.Destination {
	if len(conf.Peers) == 0 {
		return net.Destination{}
	}
	dest, err := net.ParseDestination("udp:" + conf.Peers[0].Endpoint)
	if err != nil {
		return net.Destination{}
	}
	return dest
}

// handshakePeers are the peers a waiting connection asks for a handshake.
func handshakePeers(conf *DeviceConfig) []device.NoisePublicKey {
	keys := make([]device.NoisePublicKey, 0, len(conf.Peers))
	for _, p := range conf.Peers {
		var k device.NoisePublicKey
		if k.FromHex(p.PublicKey) == nil {
			keys = append(keys, k)
		}
	}
	return keys
}

// askHandshake has the device start a handshake with its peers, which also
// renews the session keys, as the device does every two minutes anyway. A
// device sends one at most every device.RekeyTimeout, so the connections
// waiting at once ask for one between them. It reports false once the
// Handler is closed: a connection asks only after its device was up, and
// only Close takes the device away, also when its outbound sleeps or closes.
//
// The first handshake of a device is the exception. A device that has
// completed none askHandshakeAfter after it sent its first datagram lost
// the initiation or the response, one datagram each way, and would repeat
// it only device.RekeyTimeout after the first: after the progressTimeout of
// a probe that wakes the outbound, which then fails and rules out a working
// tunnel. The first ask that late has it repeat the handshake at once, and
// only that one: a server that is gone costs it one handshake more.
func (h *Handler) askHandshake() bool {
	h.mu.Lock()
	dev := h.dev
	h.mu.Unlock()
	if dev == nil {
		return false
	}
	var unfinished map[string]bool
	if first := h.recv.firstSent.Load(); first != 0 && time.Since(time.Unix(0, first)) >= askHandshakeAfter &&
		h.recv.checked.CompareAndSwap(false, true) {
		unfinished = withoutHandshake(dev)
		if len(unfinished) > 0 {
			errors.LogWarning(context.Background(), "the first handshake of the tunnel is unanswered ",
				h.recv.since(first), ", repeating it")
		}
	}
	for _, k := range handshakePeers(h.conf) {
		if p := dev.LookupPeer(k); p != nil {
			if unfinished[hex.EncodeToString(k[:])] {
				// The peer has no keys to expire yet: this ends the wait
				// for the repeat.
				p.ExpireCurrentKeypairs()
			}
			p.SendHandshakeInitiation(false)
		}
	}
	return true
}

// reportUnfinished tells, as the Handler closes, that its device has never
// completed a handshake. Whether the server sent it anything tells a
// response the device dropped from one the network lost.
func (h *Handler) reportUnfinished() {
	h.mu.Lock()
	dev := h.dev
	h.mu.Unlock()
	first := h.recv.firstSent.Load()
	if dev == nil || first == 0 || len(withoutHandshake(dev)) == 0 {
		return
	}
	errors.LogWarning(context.Background(), "the tunnel ends without a handshake ", h.recv.since(first))
}

// withoutHandshake returns the public keys, in hex, of the peers of dev that
// have never completed a handshake.
func withoutHandshake(dev *device.Device) map[string]bool {
	cfg, err := dev.IpcGet()
	if err != nil {
		return nil
	}
	peers := map[string]bool{}
	var peer string
	for _, line := range strings.Split(cfg, "\n") {
		key, value, _ := strings.Cut(line, "=")
		switch key {
		case "public_key":
			peer = value
			peers[peer] = true
		case "last_handshake_time_sec", "last_handshake_time_nsec":
			if value != "0" {
				delete(peers, peer)
			}
		}
	}
	return peers
}

// receivedCounter counts the datagrams a client receives from its server: a
// connection tells from it whether the server answered, see tunnelDial. It
// also counts those it sends, and notes when it sent the first one, for
// askHandshake.
type receivedCounter struct {
	atomic.Uint64
	sent atomic.Uint64
	// In Unix nanoseconds, zero before.
	firstSent atomic.Int64
	// askHandshake has checked the first handshake.
	checked atomic.Bool
}

// since describes the datagrams of the device since the first one it sent,
// at first, for a log line.
func (r *receivedCounter) since(first int64) string {
	return fmt.Sprintf("%v after the first datagram: %d sent, %d received",
		time.Since(time.Unix(0, first)).Round(time.Millisecond), r.sent.Load(), r.Load())
}

// wrap counts the datagrams the bind reads from its socket.
func (r *receivedCounter) wrap(pc net.PacketConn) net.PacketConn {
	return &countedPacketConn{PacketConn: pc, received: r}
}

type countedPacketConn struct {
	net.PacketConn
	received *receivedCounter
}

func (c *countedPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(p)
	if err == nil {
		c.received.Add(1)
	}
	return n, addr, err
}

func (c *countedPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if c.received.sent.Add(1) == 1 {
		c.received.firstSent.Store(time.Now().UnixNano())
	}
	return c.PacketConn.WriteTo(p, addr)
}

// tunnelDial follows one connection through the tunnel for the observer of
// the outbound. A nil one does nothing, so Process calls it either way.
type tunnelDial struct {
	// The Handler whose device carries the connection.
	h       *Handler
	tracker outbound.DialTracker
	// Ends with the user connection, also when the observer aborts it.
	ctx context.Context

	// h.recv when the connection entered the tunnel, and when: a
	// datagram that arrives later is an answer for it.
	mark  uint64
	since time.Time

	mu    sync.Mutex
	timer *time.Timer
	// Dialed has been called.
	dialed bool
	// The connection failed inside the tunnel before any answer, so an
	// answer from now on only shows that the tunnel was slow.
	late bool
	// The server answered nothing for answerWait: the connection asks for
	// no more handshakes, see giveUp.
	unanswered bool
	// Closed once Dialed has been called.
	settled chan struct{}
	// What Dialed returned for the connection inside the tunnel; a UDP flow
	// sends before the answer and loads them for every datagram.
	counters atomic.Pointer[tunnelCounters]
}

type tunnelCounters struct {
	read, write stats.Counter
}

// startTunnelDial starts following the connection when an observer follows
// the outbound, or takes over the dial a lazyClient started before its wake.
func (h *Handler) startTunnelDial(ctx context.Context, dialer internet.Dialer) *tunnelDial {
	if s, ok := dialer.(startedDial); ok {
		if s.td != nil {
			s.td.h = h
		}
		return s.td
	}
	t := startTunnelDial(ctx, dialer, tunnelServer(h.conf))
	if t != nil {
		t.h = h
	}
	return t
}

// startTunnelDial starts following the connection to server when an
// observer follows the outbound: the handler then gives the proxy an
// outbound.TunnelDialer.
func startTunnelDial(ctx context.Context, dialer internet.Dialer, server net.Destination) *tunnelDial {
	td, ok := dialer.(outbound.TunnelDialer)
	if !ok {
		return nil
	}
	tracker := td.StartTunnelDial(ctx, server)
	if tracker == nil {
		return nil
	}
	return &tunnelDial{tracker: tracker, ctx: ctx, settled: make(chan struct{})}
}

// failed reports that the connection got no tunnel: the device could not be
// created, which is a path failure like a failed dial. A connection that
// gave up waiting, or an outbound closed for good, tells nothing.
func (t *tunnelDial) failed(err error) {
	if t == nil {
		return
	}
	if t.ctx.Err() != nil || goerrors.Is(err, errOutboundClosed) {
		err = context.Canceled
	}
	t.settle(err)
}

// entered notes that the connection has the tunnel: from now on whatever
// the server sends answers it.
func (t *tunnelDial) entered() {
	if t == nil {
		return
	}
	t.mark = t.h.recv.Load()
	t.since = time.Now()
	t.mu.Lock()
	if !t.dialed {
		t.timer = time.AfterFunc(askHandshakeAfter, func() {
			safe.Run(safe.Subsystem, "proxy/wireguard/client_observe.go:t.entered", nil, func() func() { safeFn := t.poll; return func() { safeFn() } }())
		})
	}
	t.mu.Unlock()
}

func (t *tunnelDial) poll() {
	if t.answeredNow() {
		return
	}
	if t.ctx.Err() != nil {
		// The connection ended, or the observer gave up on it and the
		// tunnel. Each handshake asked for starts the retries of the device
		// anew, which would go on for 90 s to a server that may be gone.
		t.settle(context.Canceled)
		return
	}
	if time.Since(t.since) >= answerWait {
		t.giveUp()
		return
	}
	if !t.h.askHandshake() {
		// Closing the outbound ended its connections, which tells nothing.
		t.settle(context.Canceled)
		return
	}
	t.mu.Lock()
	if !t.dialed {
		t.timer.Reset(answerPoll)
	}
	t.mu.Unlock()
}

// giveUp stops asking the server for handshakes once it has answered nothing
// for answerWait: the tunnel does not work. A connection that failed inside
// the tunnel meanwhile ends its dial with that verdict. One that still waits
// there, as a TCP connection whose SYN the stack repeats until the handshake
// timeout of its policy, 60 s by default, keeps its dial open: the observer
// has timed it out by then, and it must still see the connection to end it
// when it finds the outbound down, as it does a dial to a server that hangs.
// Ended there, the connection would wait unseen and hold the tunnel awake,
// its device handshaking with the dead address.
func (t *tunnelDial) giveUp() {
	t.mu.Lock()
	t.unanswered = true
	late := t.late
	t.mu.Unlock()
	if late {
		t.settle(errNoAnswer)
	}
}

// answeredNow ends the dial when the server has answered since the
// connection entered the tunnel.
func (t *tunnelDial) answeredNow() bool {
	if t.h.recv.Load() == t.mark {
		return false
	}
	t.answer(nil)
	return true
}

// answer ends the dial: the server answered. conn is the connection inside
// the tunnel once it is open. It returns the counters for the connection.
func (t *tunnelDial) answer(conn net.Conn) *tunnelCounters {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dialed {
		return t.counters.Load()
	}
	if t.late {
		// The connection failed for the wait, not for its destination.
		t.tracker.Dialed(nil, context.Canceled, nil, nil)
		t.endLocked()
		return nil
	}
	read, write := t.tracker.Dialed(conn, nil, nil, nil)
	c := &tunnelCounters{read: read, write: write}
	t.counters.Store(c)
	t.endLocked()
	return c
}

// settle ends the dial with err unless it has ended.
func (t *tunnelDial) settle(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.dialed {
		t.tracker.Dialed(nil, err, nil, nil)
		t.endLocked()
	}
}

func (t *tunnelDial) endLocked() {
	t.dialed = true
	close(t.settled)
	if t.timer != nil {
		t.timer.Stop()
	}
}

// opened wraps the TCP connection inside the tunnel, which the server has
// just answered, with the counters of the observer. The outbound counts the
// traffic of its tunnel in its stats already, so they forward to nothing.
func (t *tunnelDial) opened(conn net.Conn) net.Conn {
	if t == nil {
		return conn
	}
	c := t.answer(conn)
	if c == nil || (c.read == nil && c.write == nil) {
		return conn
	}
	return &stat.CounterConnection{Connection: conn, ReadCounter: c.read, WriteCounter: c.write}
}

// openedPacket wraps the UDP socket of a flow inside the tunnel. The flow
// sends before the server answered; its first datagram back is an answer.
func (t *tunnelDial) openedPacket(pc net.PacketConn) net.PacketConn {
	if t == nil {
		return pc
	}
	return &observedPacketConn{PacketConn: pc, t: t}
}

// failedInside ends the dial of a connection that could not be made inside
// the tunnel, or whose destination could not be resolved there. When the
// server has answered, the tunnel works and the destination failed, which
// the observer learns when the connection ends. Otherwise the failure tells
// nothing yet, and the connection waits for the verdict, at most answerWait
// since it entered the tunnel: no answer means that the tunnel does not
// work, a late answer that it was only slow, which is held against nothing.
// Past that wait the verdict is in.
func (t *tunnelDial) failedInside() {
	if t == nil || t.answeredNow() {
		return
	}
	t.mu.Lock()
	t.late = true
	unanswered := t.unanswered
	t.mu.Unlock()
	if unanswered {
		if t.ctx.Err() != nil {
			t.settle(context.Canceled)
		} else {
			t.settle(errNoAnswer)
		}
		return
	}
	select {
	case <-t.settled:
	case <-t.ctx.Done():
		t.settle(context.Canceled)
	}
}

// end stops following the connection, which ended with err. Before the
// connection entered the tunnel, err tells why it got none. After, a dial
// still open tells nothing: the connection ended before the server answered
// or could not.
func (t *tunnelDial) end(err error) {
	if t == nil {
		return
	}
	if err != nil && t.since.IsZero() {
		t.failed(err)
		return
	}
	t.settle(context.Canceled)
}

// observedPacketConn counts a UDP flow inside the tunnel for the observer.
type observedPacketConn struct {
	net.PacketConn
	t *tunnelDial
	// Only the goroutine that reads uses it.
	answered bool
}

func (c *observedPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(p)
	if err != nil {
		return n, addr, err
	}
	if !c.answered {
		c.answered = true
		c.t.answer(nil)
	}
	if tc := c.t.counters.Load(); tc != nil && tc.read != nil {
		tc.read.Add(int64(n))
	}
	return n, addr, err
}

func (c *observedPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.PacketConn.WriteTo(p, addr)
	if err == nil {
		if tc := c.t.counters.Load(); tc != nil && tc.write != nil {
			tc.write.Add(int64(n))
		}
	}
	return n, err
}
