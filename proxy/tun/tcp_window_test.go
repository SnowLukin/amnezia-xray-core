package tun

import (
	"encoding/binary"
	gonet "net"
	"net/netip"
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
	// handshake is set until the connection is open: what the stack sends is taken one by one until then
	handshake atomic.Bool
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
	return newWindowPeers(t, []string{src}, stamps, tcpGrowBudget)[0]
}

// newWindowPeers connects peers from srcs to one growing stack with budget
func newWindowPeers(t *testing.T, srcs []string, stamps bool, budget int64) []*windowPeer {
	conns := make(chan accepted, len(srcs))
	l := newLink()
	s := newTestStack(t, l, accept(conns))
	s.growing, s.stamps, s.budget = true, true, budget
	var mu sync.Mutex
	peers := map[uint16]*windowPeer{}
	l.out = func(b []byte) {
		packet, ok := parsePacket(b, 0)
		if !ok || packet.proto != protoTCP {
			return
		}
		t := packet.payload
		mu.Lock()
		w := peers[binary.BigEndian.Uint16(t[2:])]
		mu.Unlock()
		if w == nil {
			return
		}
		if w.handshake.Load() {
			w.peer.sent <- b
			return
		}
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
	var out []*windowPeer
	for _, src := range srcs {
		p := &peer{t: t, l: l, sent: make(chan []byte, 1024), src: netip.MustParseAddrPort(src), dst: netip.MustParseAddrPort("10.0.0.2:80"), seq: 1000, window: 0xffff}
		w := &windowPeer{peer: p, s: s, stamps: stamps, clock: 1000}
		w.handshake.Store(true)
		mu.Lock()
		peers[p.src.Port()] = w
		mu.Unlock()
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
		w.handshake.Store(false)
		p.send(p.seq, tcpAck, w.stampOption(w.acks[0].stamp), nil)
		take(t, conns)
		s.mu.Lock()
		for f, c := range s.conns {
			if f.src == p.src {
				w.conn = c
			}
		}
		s.mu.Unlock()
		out = append(out, w)
	}
	return out
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
	a := w.acks[i]
	// a few seconds of them, for the round trips that come
	j := 0
	for j < i && time.Since(w.acks[j].at) > 5*time.Second {
		j++
	}
	w.acks = w.acks[j:]
	return a
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
			for range 40 {
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

	// a way that takes 128 KiB in 20 ms, and keeps what more it is given in a queue, which makes the round trip
	// longer: the reader gets no faster with a larger window. Without the times of RFC 7323 the round trip is how
	// long a window takes to come, which says less
	for i, stamps := range []bool{true, false} {
		queued := newWindowPeer(t, "10.0.0.1:"+strconv.Itoa(1304+i), stamps)
		queued.read()
		for range 16 {
			offered := int32(queued.seen(0).edge - queued.peer.seq)
			queued.round(max(windowHandshake/2, time.Duration(offered)*(windowHandshake/2)/tcpGrowingWindow), 1, 4)
		}
		if got := queued.window(); got > 4*tcpGrowingWindow {
			t.Fatal("stamps ", stamps, ": grew for a way that is full to ", got)
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

// A connection of a growing stack goes on sending once a sixth, more than an eighth, of a full window is acknowledged: through a tunnel
// what it sends comes back a long round trip later, and a writer that waits for half of the window leaves half of
// that way empty.
func TestTCPWindowInFlight(t *testing.T) {
	const window = 1 << 20
	p, l := newPeer(t, "10.0.0.1:1501", "10.0.0.2:80")
	conns := make(chan accepted, 1)
	s := newTestStack(t, l, accept(conns))
	s.growing, s.window, s.budget = true, tcpGrownWindow, tcpGrowBudget
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
	// a sixth of it acknowledged: the writer goes on
	p.ack += window / 6
	p.send(p.seq, tcpAck, nil, nil)
	if _, any := sent(200 * time.Millisecond); !any {
		t.Fatal("nothing sent with a sixth of the window free")
	}
}

// A UDP port that DialUDP opened keeps what fits its queue while it is not read, and drops what comes after.
func TestUDPQueueBound(t *testing.T) {
	p, l := newPeer(t, "10.0.0.1:53", "10.0.0.2:0")
	s := newDialStack(t, l, "10.0.0.2")
	conn, err := s.DialUDP()
	if err != nil {
		t.Fatal(err)
	}
	local := conn.LocalAddr().(*gonet.UDPAddr).AddrPort()
	for range 3 * udpSocketQueueLimit {
		p.l.in <- udpPacket(p.src, local, []byte("x"))
	}
	time.Sleep(100 * time.Millisecond)
	u := conn.(*udpSocket)
	u.queue.mu.Lock()
	queued := len(u.queue.items)
	u.queue.mu.Unlock()
	if queued != 256 {
		t.Fatal("queued ", queued)
	}
}

// readUntil takes what comes as soon as it comes, until stop
func (w *windowPeer) readUntil(stop *atomic.Bool) {
	go func() {
		for !stop.Load() {
			mb, err := w.conn.ReadMultiBuffer()
			if err != nil {
				return
			}
			buf.ReleaseMulti(mb)
		}
	}()
}

// The window of a reader that keeps up grows again when the way gets slower after it grew, as for a phone that goes
// from Wi-Fi to a mobile network under the same tunnel, or with traffic of others on the way.
func TestTCPWindowGrowsSlowerWay(t *testing.T) {
	for i, stamps := range []bool{false, true} {
		w := newWindowPeer(t, "10.0.0.1:"+strconv.Itoa(1601+i), stamps)
		w.read()
		for range 25 {
			if w.window() >= 512<<10 {
				break
			}
			w.round(windowHandshake, 1, 10)
		}
		if got := w.window(); got < 512<<10 {
			t.Fatal("stamps ", stamps, ": grew to ", got, " only")
		}
		for range 60 {
			if w.window() == tcpGrownWindow {
				break
			}
			w.round(4*windowHandshake, 1, 10)
		}
		if got := w.window(); got != tcpGrownWindow {
			t.Fatal("stamps ", stamps, ": stayed at ", got, " on a way four times as long")
		}
	}
}

// Before the segments tell how long a round trip takes, it is the one of the handshake.
func TestTCPWindowFirstRoundTrip(t *testing.T) {
	w := newWindowPeer(t, "10.0.0.1:1701", false)
	c := w.conn
	c.mu.Lock()
	rtt := c.rttMin
	c.mu.Unlock()
	if rtt < windowHandshake || rtt > 2*windowHandshake {
		t.Fatal("the handshake took ", rtt)
	}
	// a reader that takes two windows each round trip, in two round trips of it
	for range 3 {
		time.Sleep(rtt + rtt/4)
		c.mu.Lock()
		c.grow(2 * c.rcvWindow)
		c.mu.Unlock()
	}
	if got := w.window(); got == tcpGrowingWindow {
		t.Fatal("did not grow with a round trip of ", rtt)
	}
}

// The round trip that the times of RFC 7323 tell takes an eighth of each sample, and only of segments with data:
// a pure acknowledgment may be sent long after what it answers.
func TestTCPRoundTripSamples(t *testing.T) {
	// segments that answer what was sent 40 ms ago, and then one that answers what was sent 400 ms ago
	c := &tcpConn{stack: &stack{}, stampBase: 1000}
	answer := func(ago time.Duration) {
		o := []byte{8, 10, 0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(o[6:], c.stampBase+uint32((time.Since(tcpStarted)-ago)/time.Millisecond))
		c.stamped(segment{options: o, payload: []byte{1}})
	}
	for range 10 {
		answer(40 * time.Millisecond)
	}
	answer(400 * time.Millisecond)
	if c.rcvRTT < 70*time.Millisecond || c.rcvRTT > 100*time.Millisecond {
		t.Fatal("one sample of 400 ms after ten of 40 made it ", c.rcvRTT)
	}

	w := newWindowPeer(t, "10.0.0.1:1702", true)
	time.Sleep(4 * windowHandshake)
	old := w.seen(0).stamp
	w.clock++
	w.send(w.peer.seq, tcpAck, w.stampOption(old), nil)
	time.Sleep(windowHandshake / 2)
	w.conn.mu.Lock()
	rtt := w.conn.rcvRTT
	w.conn.mu.Unlock()
	if rtt != 0 {
		t.Fatal("a pure acknowledgment made the round trip ", rtt)
	}
	// the first segments after a pause carry back what was sent before it, the ones after an answer to them what
	// was sent since
	w.read()
	for range 2 {
		w.clock++
		w.send(w.peer.seq, tcpAck, w.stampOption(old), make([]byte, 100))
		w.peer.seq += 100
	}
	time.Sleep(windowHandshake / 2)
	w.conn.mu.Lock()
	rtt = w.conn.rcvRTT
	w.conn.mu.Unlock()
	if rtt != 0 {
		t.Fatal("what answered a segment from before a pause made the round trip ", rtt)
	}
	time.Sleep(windowHandshake)
	w.clock++
	w.send(w.peer.seq, tcpAck, w.stampOption(w.seen(0).stamp), make([]byte, 100))
	w.peer.seq += 100
	time.Sleep(windowHandshake / 4)
	w.conn.mu.Lock()
	rtt = w.conn.rcvRTT
	w.conn.mu.Unlock()
	if rtt < windowHandshake || rtt > 2*windowHandshake {
		t.Fatal("a segment with data made the round trip ", rtt)
	}
}

// Connections of a stack grow together only as far as its budget lets them, and give it back when they are gone.
func TestTCPWindowBudget(t *testing.T) {
	const budget = 2 << 20
	ws := newWindowPeers(t, []string{"10.0.0.1:1801", "10.0.0.1:1802", "10.0.0.1:1803"}, true, budget)
	var wg sync.WaitGroup
	for _, w := range ws {
		w.read()
		wg.Go(func() {
			for range 25 {
				w.round(windowHandshake, 1, 10)
			}
		})
	}
	wg.Wait()
	var grown int64
	for _, w := range ws {
		grown += int64(w.window() - tcpGrowingWindow)
	}
	if s := ws[0].s; grown > budget || s.grown.Load() > budget || grown < budget/2 {
		t.Fatal("the windows grew by ", grown, ", the stack has ", s.grown.Load(), " of ", budget)
	}
	for _, w := range ws {
		w.conn.Close()
	}
	if got := ws[0].s.grown.Load(); got != 0 {
		t.Fatal("closed connections still take ", got)
	}
}

// Readers that stopped hold no more than what their connections start with and the budget, however many they are.
func TestTCPWindowStoppedReaders(t *testing.T) {
	const budget = 1 << 20
	var srcs []string
	for i := range 6 {
		srcs = append(srcs, "10.0.0.1:"+strconv.Itoa(1811+i))
	}
	ws := newWindowPeers(t, srcs, true, budget)
	stops := make([]atomic.Bool, len(ws))
	var wg sync.WaitGroup
	for i, w := range ws {
		w.readUntil(&stops[i])
		wg.Go(func() {
			for range 20 {
				w.round(windowHandshake, 1, 10)
			}
			stops[i].Store(true)
			for range 6 {
				w.round(windowHandshake, 1, 4)
			}
		})
	}
	wg.Wait()
	var held int64
	for _, w := range ws {
		w.conn.mu.Lock()
		held += int64(w.conn.rcvLen)
		w.conn.mu.Unlock()
	}
	if most := int64(len(ws))*tcpGrowingWindow + budget; held > most {
		t.Fatal("readers that stopped hold ", held, " of at most ", most)
	}
}

// growAll has the peers send to their connections, which read, until each window is as large as it gets
func growAll(ws []*windowPeer, rounds int) {
	var wg sync.WaitGroup
	for _, w := range ws {
		wg.Go(func() {
			for range rounds {
				if w.window() == tcpGrownWindow {
					return
				}
				w.round(windowHandshake, 1, 10)
			}
		})
	}
	wg.Wait()
}

// roundsTo is how many round trips of rtt a window takes to grow as large as it gets, or -1
func roundsTo(w *windowPeer, rtt time.Duration, most int) int {
	for i := range most {
		if w.window() == tcpGrownWindow {
			return i
		}
		w.round(rtt, 1, 10)
	}
	return -1
}

// After a pause of the sender, as between the segments of a video or the requests over a connection that is kept,
// the window grows again as it did at the start.
func TestTCPWindowAfterPause(t *testing.T) {
	for i, stamps := range []bool{true, false} {
		w := newWindowPeer(t, "10.0.0.1:"+strconv.Itoa(1901+i), stamps)
		w.read()
		first := roundsTo(w, windowHandshake, 40)
		time.Sleep(tcpQuiet + tcpQuiet/2)
		again := roundsTo(w, windowHandshake, 40)
		if first < 0 || again < 0 || again > first+4 {
			t.Fatal("stamps ", stamps, ": ", first, " round trips to grow, after a pause ", again)
		}
		w.conn.mu.Lock()
		rtt := w.conn.rcvRTT
		w.conn.mu.Unlock()
		if rtt > 3*windowHandshake {
			t.Fatal("stamps ", stamps, ": after a pause the round trip is ", rtt)
		}
		// a video: 8 round trips of a segment, then 2 s of nothing
		var mean int64
		for range 3 {
			time.Sleep(2 * time.Second)
			for range 8 {
				mean += int64(w.window())
				w.round(windowHandshake, 1, 10)
			}
		}
		if mean /= 24; mean < 1<<20 {
			t.Fatal("stamps ", stamps, ": the window of segments after pauses is ", mean, " on average")
		}
	}
}

// A quiet connection, whose reader took all, gives back its part of the budget once what the system was told it may
// send has come, and another one grows into it.
func TestTCPWindowQuietShare(t *testing.T) {
	const budget = 4 << 20
	ws := newWindowPeers(t, []string{"10.0.0.1:1911", "10.0.0.1:1912", "10.0.0.1:1913"}, true, budget)
	for _, w := range ws {
		w.read()
	}
	growAll(ws[:2], 40)
	time.Sleep(tcpQuiet + tcpQuiet/2)
	for range 10 {
		ws[2].round(windowHandshake, 1, 10)
	}
	if got := ws[2].window(); got >= 1<<20 {
		t.Fatal("grew to ", got, " while quiet ones hold ", ws[0].s.grown.Load(), " of ", budget)
	}
	// the quiet ones get what they were told they may send, once
	for _, w := range ws[:2] {
		w.round(windowHandshake, 1, 10)
	}
	if n := roundsTo(ws[2], windowHandshake, 40); n < 0 {
		t.Fatal("grew to ", ws[2].window(), " only after the quiet ones gave back")
	}
	for _, w := range ws[:2] {
		if got := w.window(); got != tcpGrowingWindow {
			t.Fatal("a quiet one came back with ", got)
		}
	}
}

// What the system was told it may send stays: a window that starts again after a pause is not one that takes it back.
func TestTCPWindowEdgeKept(t *testing.T) {
	w := newWindowPeer(t, "10.0.0.1:1921", true)
	w.read()
	roundsTo(w, windowHandshake, 40)
	edge := w.seen(0).edge
	time.Sleep(tcpQuiet + tcpQuiet/2)
	payload := make([]byte, 1000)
	for int32(edge-w.peer.seq) > 0 {
		n := min(len(payload), int(edge-w.peer.seq))
		w.clock++
		w.send(w.peer.seq, tcpAck, w.stampOption(0), payload[:n])
		w.peer.seq += uint32(n)
	}
	time.Sleep(4 * windowHandshake)
	w.conn.mu.Lock()
	nxt := w.conn.rcvNxt
	w.conn.mu.Unlock()
	if nxt != edge {
		t.Fatal("took ", int32(nxt-edge), " of what the system was told it may send")
	}
	if got := w.window(); got != tcpGrowingWindow {
		t.Fatal("after a pause the window is ", got)
	}
	if got := w.s.grown.Load(); got != 0 {
		t.Fatal("once all came the window takes ", got, " of the budget")
	}
}

// charges is what the connections of s take of its budget
func charges(s *stack) (sum int64) {
	s.mu.Lock()
	conns := make([]*tcpConn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		c.mu.Lock()
		sum += c.charged
		c.mu.Unlock()
	}
	return sum
}

// Each way that a connection that grew to receive ends gives back what it took of the budget.
func TestTCPWindowBudgetBack(t *testing.T) {
	var srcs []string
	for i := range 5 {
		srcs = append(srcs, "10.0.0.1:"+strconv.Itoa(1931+i))
	}
	ws := newWindowPeers(t, srcs, true, 64<<20)
	stops := make([]atomic.Bool, len(ws))
	for i, w := range ws {
		w.readUntil(&stops[i])
	}
	growAll(ws, 40)
	s := ws[0].s
	if s.grown.Load() == 0 || s.grown.Load() != charges(s) {
		t.Fatal("grown ", s.grown.Load(), ", charged ", charges(s))
	}
	// the app closes, the other side resets, the reader stops and the app closes, a timeout
	ws[0].conn.Close()
	ws[1].send(ws[1].peer.seq, tcpRst|tcpAck, nil, nil)
	stops[2].Store(true)
	ws[2].round(windowHandshake, 1, 2)
	ws[2].round(windowHandshake, 1, 2)
	ws[2].conn.Close()
	ws[3].conn.mu.Lock()
	ws[3].conn.abort(errTCPTimeout)
	ws[3].conn.mu.Unlock()
	time.Sleep(4 * windowHandshake)
	if got, left := s.grown.Load(), charges(s); got != left || got > int64(tcpGrownWindow) {
		t.Fatal("grown ", got, " where the one left takes ", left)
	}
	// the stack goes away
	s.Close()
	time.Sleep(4 * windowHandshake)
	if got := s.grown.Load(); got != 0 {
		t.Fatal("a stack that is gone takes ", got)
	}
}

// What a connection takes of the budget to send more goes back when all it sent is acknowledged, and when it ends.
func TestTCPWindowBudgetSend(t *testing.T) {
	for i, end := range []string{"acknowledged", "reset", "timeout", "close"} {
		p, l := newPeer(t, "10.0.0.1:"+strconv.Itoa(1941+i), "10.0.0.2:80")
		conns := make(chan accepted, 1)
		s := newTestStack(t, l, accept(conns))
		s.growing, s.window, s.budget = true, tcpLargeWindow, 64<<20
		p.connect([]byte{1, 3, 3, 5})
		c := take(t, conns).conn.(*tcpConn)
		go c.WriteMultiBuffer(buf.MergeBytes(nil, make([]byte, 64<<20)))
		sent := p.ack
		drain := func(d time.Duration) {
			for {
				select {
				case b := <-p.sent:
					seg := p.segment(b)
					if n := len(seg.payload); n > 0 && int32(seg.seq+uint32(n)-sent) > 0 {
						sent = seg.seq + uint32(n)
					}
				case <-time.After(d):
					return
				}
			}
		}
		widened := false
		for range 300 {
			drain(5 * time.Millisecond)
			c.mu.Lock()
			widened = c.window == tcpGrownWindow
			c.mu.Unlock()
			if widened {
				break
			}
			p.ack = sent
			p.send(p.seq, tcpAck, nil, nil)
		}
		if !widened || s.grown.Load() == 0 {
			t.Fatal(end, ": did not take from the budget to send more")
		}
		switch end {
		case "acknowledged":
			// all of it, and the window of the peer closed, so that nothing more is in flight
			drain(50 * time.Millisecond)
			p.window = 0
			p.ack = sent
			p.send(p.seq, tcpAck, nil, nil)
			time.Sleep(50 * time.Millisecond)
		case "reset":
			p.send(p.seq, tcpRst|tcpAck, nil, nil)
		case "timeout":
			c.mu.Lock()
			c.abort(errTCPTimeout)
			c.mu.Unlock()
		case "close":
			c.Close()
		}
		go drain(500 * time.Millisecond)
		time.Sleep(50 * time.Millisecond)
		if got := s.grown.Load(); got != 0 {
			t.Fatal(end, ": still takes ", got)
		}
	}
}
