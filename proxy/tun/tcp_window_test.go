package tun

import (
	"encoding/binary"
	gonet "net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
)

// windowPeer sends to a connection of a growing stack over a way with a round trip of its own: what the stack
// acknowledges reaches the peer a round trip later, all of the delay on the way back.
type windowPeer struct {
	*peer
	s    *stack
	conn *tcpConn
	// stamps is set if the peer has the times of RFC 7323
	stamps bool
	// clock is the time of the peer, in milliseconds
	clock uint32

	mu   sync.Mutex
	acks []ackSeen
}

// ackSeen is an acknowledgment of the stack: when it was sent, where it lets the peer send to, and the time it carries
type ackSeen struct {
	at          time.Time
	edge, stamp uint32
}

const windowHandshake = 40 * time.Millisecond

func newWindowPeer(t *testing.T, src string, stamps bool) *windowPeer {
	conns := make(chan accepted, 1)
	p, l := newPeer(t, src, "10.0.0.2:80")
	s := newTestStack(t, l, accept(conns))
	s.growing, s.stamps = true, true
	w := &windowPeer{peer: p, s: s, stamps: stamps, clock: 1000}
	var handshake atomic.Bool
	handshake.Store(true)
	l.out = func(b []byte) {
		if handshake.Load() {
			p.sent <- b
			return
		}
		packet, ok := parsePacket(b, 0)
		if !ok || packet.proto != protoTCP {
			return
		}
		t := packet.payload
		a := ackSeen{at: time.Now(), edge: binary.BigEndian.Uint32(t[8:]) + uint32(binary.BigEndian.Uint16(t[14:]))<<tcpWindowShift}
		for kind, value, o := nextOption(t[tcpHeaderSize : int(t[12]>>4)*4]); kind != 0; kind, value, o = nextOption(o) {
			if kind == 8 && len(value) == 8 {
				a.stamp = binary.BigEndian.Uint32(value)
			}
		}
		w.mu.Lock()
		w.acks = append(w.acks, a)
		w.mu.Unlock()
	}
	// with a window scale, and the acknowledgment of the SYN-ACK a round trip later, which is what the stack times
	syn := []byte{1, 3, 3, 7}
	if stamps {
		syn = append(syn, w.stampOption(0)...)
	}
	p.send(p.seq, tcpSyn, syn, nil)
	p.seq++
	answer := p.receive()
	p.ack = answer.seq + 1
	w.acks = []ackSeen{{at: time.Now(), edge: p.seq + uint32(answer.window), stamp: stampOf(answer)}}
	time.Sleep(windowHandshake)
	handshake.Store(false)
	p.send(p.seq, tcpAck, w.stampOption(w.acks[0].stamp), nil)
	take(t, conns)
	s.mu.Lock()
	for _, c := range s.conns {
		w.conn = c
	}
	s.mu.Unlock()
	return w
}

func stampOf(s segment) uint32 {
	for kind, value, o := nextOption(s.options); kind != 0; kind, value, o = nextOption(o) {
		if kind == 8 && len(value) == 8 {
			return binary.BigEndian.Uint32(value)
		}
	}
	return 0
}

// stampOption is the option with the time of the peer and echo, if the peer has the times
func (w *windowPeer) stampOption(echo uint32) []byte {
	if !w.stamps {
		return nil
	}
	o := []byte{1, 1, 8, 10, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(o[4:], w.clock)
	binary.BigEndian.PutUint32(o[8:], echo)
	return o
}

// seen is the last acknowledgment that has reached the peer, rtt after the stack sent it
func (w *windowPeer) seen(rtt time.Duration) ackSeen {
	w.mu.Lock()
	defer w.mu.Unlock()
	i := len(w.acks) - 1
	for i > 0 && time.Since(w.acks[i].at) < rtt {
		i--
	}
	w.acks = w.acks[i:]
	return w.acks[0]
}

// round sends share of what the window lets the peer send in a round trip of rtt, in pieces spread over it, as a
// sender that paces does, and returns when the round trip is over
func (w *windowPeer) round(rtt time.Duration, share float64, pieces int) {
	start := time.Now()
	a := w.seen(rtt)
	end := w.peer.seq + uint32(share*float64(int32(a.edge-w.peer.seq)))
	if int32(a.edge-w.peer.seq) <= 0 {
		end = w.peer.seq
	}
	payload := make([]byte, 1000)
	for i := 1; i <= pieces; i++ {
		to := w.peer.seq + uint32(int64(int32(end-w.peer.seq))/int64(pieces-i+1))
		for int32(to-w.peer.seq) > 0 {
			n := min(len(payload), int(to-w.peer.seq))
			w.clock++
			w.send(w.peer.seq, tcpAck, w.stampOption(a.stamp), payload[:n])
			w.peer.seq += uint32(n)
		}
		time.Sleep(time.Until(start.Add(rtt * time.Duration(i) / time.Duration(pieces))))
	}
}

func (w *windowPeer) window() int32 {
	w.conn.mu.Lock()
	defer w.conn.mu.Unlock()
	return w.conn.rcvWindow
}

// read takes what comes as soon as it comes
func (w *windowPeer) read() {
	go func() {
		for {
			mb, err := w.conn.ReadMultiBuffer()
			if err != nil {
				return
			}
			buf.ReleaseMulti(mb)
		}
	}()
}

// The window of a connection whose reader keeps up grows to the most that the 16 bits of a window take, while the
// window is what holds the stream back: also when the way takes longer now than in the handshake, as when it is
// loaded or a wireless link waits, and the sender paces what it sends.
func TestTCPWindowGrows(t *testing.T) {
	port := 1234
	for _, stamps := range []bool{false, true} {
		for _, longer := range []time.Duration{1, 2, 3} {
			port++
			w := newWindowPeer(t, "10.0.0.1:"+strconv.Itoa(port), stamps)
			if got := w.window(); got != tcpGrowingWindow {
				t.Fatal("started with ", got)
			}
			w.read()
			rtt := longer * windowHandshake
			for range 25 {
				if w.window() == tcpGrownWindow {
					break
				}
				w.round(rtt, 1, 10)
			}
			if got := w.window(); got != tcpGrownWindow {
				t.Fatal("stamps ", stamps, ", round trip ", longer, " times that of the handshake: grew to ", got, " only")
			}
			for range 3 {
				w.round(rtt, 1, 10)
			}
			if got := w.window(); got != tcpGrownWindow {
				t.Fatal("grew past the most: ", got)
			}
			if edge := w.seen(0).edge; int32(edge-w.peer.seq) < tcpGrownWindow-int32(w.conn.mss) {
				t.Fatal("offered ", int32(edge-w.peer.seq))
			}
		}
	}
}

// The window stays as it is while the window is not what holds the stream back: nothing is read, or what is read
// comes slowly on average though at once now and then, or the sender sends less than the window lets it.
func TestTCPWindowStays(t *testing.T) {
	// nothing is read, and then all of it at once, late
	idle := newWindowPeer(t, "10.0.0.1:1301", false)
	for range 6 {
		idle.round(windowHandshake, 1, 1)
	}
	if got := idle.window(); got != tcpGrowingWindow {
		t.Fatal("grew without a reader to ", got)
	}
	if offered := int32(idle.seen(0).edge - idle.peer.seq); offered != 0 {
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

	// a reader that takes all that waits now and then, in small pieces one right after the other
	for _, stamps := range []bool{false, true} {
		slow := newWindowPeer(t, "10.0.0.1:1302", stamps)
		done := make(chan struct{})
		go func() {
			defer close(done)
			b := make([]byte, 4<<10)
			for range 4 {
				time.Sleep(8 * windowHandshake)
				for {
					slow.conn.mu.Lock()
					waiting := slow.conn.rcvLen
					slow.conn.mu.Unlock()
					if waiting == 0 {
						break
					}
					slow.conn.Read(b)
				}
			}
		}()
		for range 32 {
			slow.round(windowHandshake, 1, 1)
		}
		<-done
		if got := slow.window(); got != tcpGrowingWindow {
			t.Fatal("stamps ", stamps, ": grew for a reader that is slow to ", got)
		}
	}

	// the sender sends less than a third of the window in a round trip, which the times of RFC 7323 tell
	short := newWindowPeer(t, "10.0.0.1:1303", true)
	short.read()
	for range 12 {
		short.round(windowHandshake/2, 0.3, 4)
	}
	if got := short.window(); got != tcpGrowingWindow {
		t.Fatal("grew for a sender that is slow to ", got)
	}
}

// A connection of a stack that is not growing keeps its window, and one of a growing stack without a window scale
// does not grow past what 16 bits take.
func TestTCPWindowFixed(t *testing.T) {
	w := newWindowPeer(t, "10.0.0.1:1401", false)
	w.s.growing = false
	w.conn.mu.Lock()
	w.conn.rcvWindow = w.s.window
	w.conn.mu.Unlock()
	w.read()
	for range 6 {
		w.round(windowHandshake, 1, 1)
	}
	if got := w.window(); got != tcpWindow {
		t.Fatal("the window of a stack that is not growing became ", got)
	}

	w = newWindowPeer(t, "10.0.0.1:1402", false)
	w.conn.mu.Lock()
	w.conn.rcvShift = 0
	w.conn.grow(1 << 30)
	w.conn.mu.Unlock()
	time.Sleep(windowHandshake)
	w.conn.mu.Lock()
	w.conn.grow(1 << 30)
	w.conn.mu.Unlock()
	if got := w.window(); got != tcpGrowingWindow {
		t.Fatal("grew without a window scale to ", got)
	}
}

// A connection of a growing stack goes on sending once an eighth of a full window is acknowledged: through a tunnel
// what it sends comes back a long round trip later, and a writer that waits for half of the window leaves half of
// that way empty.
func TestTCPWindowInFlight(t *testing.T) {
	const window = 1 << 20
	p, l := newPeer(t, "10.0.0.1:1501", "10.0.0.2:80")
	conns := make(chan accepted, 1)
	s := newTestStack(t, l, accept(conns))
	s.growing, s.window = true, tcpGrownWindow
	// the window of the peer is 1 MiB, scaled by 32
	p.window = window >> 5
	p.connect([]byte{1, 3, 3, 5})
	a := take(t, conns)
	go a.conn.(buf.Writer).WriteMultiBuffer(buf.MergeBytes(nil, make([]byte, 16<<20)))
	// what the stack sends until it stops, the end of it
	sent := func(wait time.Duration) (end uint32, any bool) {
		end = p.ack
		for {
			select {
			case b := <-p.sent:
				seg := p.segment(b)
				if n := len(seg.payload); n > 0 {
					end, any = seg.seq+uint32(n), true
				}
			case <-time.After(wait):
				return end, any
			}
		}
	}
	// all of it acknowledged whenever the stack stops, until it stops at the window
	var end uint32
	for range 30 {
		end, _ = sent(50 * time.Millisecond)
		if end-p.ack >= window-uint32(a.conn.(*tcpConn).mss) {
			break
		}
		p.ack = end
		p.send(p.seq, tcpAck, nil, nil)
	}
	if end-p.ack < window-uint32(a.conn.(*tcpConn).mss) {
		t.Fatal("stopped at ", end-p.ack, " of a window of ", window)
	}
	// a quarter of it acknowledged: the writer goes on
	p.ack += window / 4
	p.send(p.seq, tcpAck, nil, nil)
	if _, any := sent(200 * time.Millisecond); !any {
		t.Fatal("nothing sent with a quarter of the window free")
	}
}

// A UDP flow that is not read keeps what fits its queue, and drops what comes after.
func TestUDPQueueBound(t *testing.T) {
	p, l := newPeer(t, "10.0.0.1:53", "10.0.0.2:0")
	s := newDialStack(t, l, "10.0.0.2")
	conn, err := s.DialUDP()
	if err != nil {
		t.Fatal(err)
	}
	local := conn.LocalAddr().(*gonet.UDPAddr).AddrPort()
	for range 3 * udpQueueLimit {
		p.l.in <- udpPacket(p.src, local, []byte("x"))
	}
	time.Sleep(100 * time.Millisecond)
	u := conn.(*udpSocket)
	u.queue.mu.Lock()
	queued := len(u.queue.items)
	u.queue.mu.Unlock()
	if queued != 128 {
		t.Fatal("queued ", queued)
	}
}
