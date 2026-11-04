package tun

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
)

// windowPeer sends to a connection of a growing stack as fast as the window lets it, one window every round trip,
// and sees the window in the acknowledgments without waiting for them.
type windowPeer struct {
	*peer
	s    *stack
	conn *tcpConn
	// edge is where the last acknowledgment lets the peer send to
	edge atomic.Uint32
}

const windowRTT = 50 * time.Millisecond

func newWindowPeer(t *testing.T, src string) *windowPeer {
	conns := make(chan accepted, 1)
	p, l := newPeer(t, src, "10.0.0.2:80")
	s := newTestStack(t, l, accept(conns))
	s.growing = true
	w := &windowPeer{peer: p, s: s}
	var handshake atomic.Bool
	handshake.Store(true)
	l.out = func(b []byte) {
		if handshake.Load() {
			p.sent <- b
			return
		}
		if packet, ok := parsePacket(b, 0); ok && packet.proto == protoTCP {
			t := packet.payload
			w.edge.Store(binary.BigEndian.Uint32(t[8:]) + uint32(binary.BigEndian.Uint16(t[14:]))<<tcpWindowShift)
		}
	}
	// with a window scale, and the acknowledgment of the SYN-ACK a round trip later, which is what the stack times
	p.send(p.seq, tcpSyn, []byte{1, 3, 3, 7}, nil)
	p.seq++
	syn := p.receive()
	p.ack = syn.seq + 1
	w.edge.Store(p.seq + uint32(syn.window))
	time.Sleep(windowRTT)
	handshake.Store(false)
	p.send(p.seq, tcpAck, nil, nil)
	take(t, conns)
	s.mu.Lock()
	for _, c := range s.conns {
		w.conn = c
	}
	s.mu.Unlock()
	return w
}

// round sends up to where the window ends when it starts, as what is acknowledged meanwhile comes a round trip
// later, and waits that round trip
func (w *windowPeer) round() {
	payload := make([]byte, 1000)
	for edge := w.edge.Load(); int32(edge-w.peer.seq) > 0; {
		n := min(len(payload), int(edge-w.peer.seq))
		w.send(w.peer.seq, tcpAck, nil, payload[:n])
		w.peer.seq += uint32(n)
	}
	time.Sleep(windowRTT)
}

func (w *windowPeer) window() int32 {
	w.conn.mu.Lock()
	defer w.conn.mu.Unlock()
	return w.conn.rcvWindow
}

// The window of a connection grows while its reader takes what comes as fast as the window lets it, up to the most
// that the 16 bits of a window take, and stays as it is while nothing is read, also once all of it is read late.
func TestTCPWindowGrows(t *testing.T) {
	w := newWindowPeer(t, "10.0.0.1:1234")
	if got := w.window(); got != tcpGrowingWindow {
		t.Fatal("started with ", got)
	}
	var read atomic.Int64
	go func() {
		for {
			mb, err := w.conn.ReadMultiBuffer()
			if err != nil {
				return
			}
			read.Add(int64(mb.Len()))
			buf.ReleaseMulti(mb)
		}
	}()
	for range 20 {
		if w.window() == tcpGrownWindow {
			break
		}
		w.round()
	}
	if got := w.window(); got != tcpGrownWindow {
		t.Fatal("grew to ", got, " only")
	}
	for range 3 {
		w.round()
	}
	if got := w.window(); got != tcpGrownWindow {
		t.Fatal("grew past the most: ", got)
	}
	// the system is offered all of it
	if edge := w.edge.Load(); int32(edge-w.peer.seq) < tcpGrownWindow-int32(w.conn.mss) {
		t.Fatal("offered ", int32(edge-w.peer.seq))
	}

	idle := newWindowPeer(t, "10.0.0.1:1235")
	for range 6 {
		idle.round()
	}
	if got := idle.window(); got != tcpGrowingWindow {
		t.Fatal("grew without a reader to ", got)
	}
	if offered := int32(idle.edge.Load() - idle.peer.seq); offered != 0 {
		t.Fatal("offered ", offered, " more with the window full")
	}
	mb, err := idle.conn.ReadMultiBuffer()
	// a scaled window says it to 32 bytes
	if err != nil || mb.Len() > tcpGrowingWindow || mb.Len() <= tcpGrowingWindow-1<<tcpWindowShift {
		t.Fatal("read ", mb.Len(), ", error: ", err)
	}
	buf.ReleaseMulti(mb)
	if got := idle.window(); got != tcpGrowingWindow {
		t.Fatal("grew after a late read to ", got)
	}
}

// A connection of a stack that is not growing keeps its window, and one of a growing stack without a window scale
// does not grow past what 16 bits take.
func TestTCPWindowFixed(t *testing.T) {
	w := newWindowPeer(t, "10.0.0.1:1236")
	w.s.growing = false
	w.conn.mu.Lock()
	w.conn.rcvWindow = w.s.window
	w.conn.mu.Unlock()
	go func() {
		for {
			mb, err := w.conn.ReadMultiBuffer()
			if err != nil {
				return
			}
			buf.ReleaseMulti(mb)
		}
	}()
	for range 6 {
		w.round()
	}
	if got := w.window(); got != tcpWindow {
		t.Fatal("the window of a stack that is not growing became ", got)
	}

	w = newWindowPeer(t, "10.0.0.1:1237")
	w.conn.mu.Lock()
	w.conn.rcvShift = 0
	w.conn.grow(1 << 30)
	w.conn.mu.Unlock()
	time.Sleep(windowRTT)
	w.conn.mu.Lock()
	w.conn.grow(1 << 30)
	w.conn.mu.Unlock()
	if got := w.window(); got != tcpGrowingWindow {
		t.Fatal("grew without a window scale to ", got)
	}
}
