package health

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	stdnet "net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

func stunReply(request []byte) []byte {
	response := append([]byte(nil), request...)
	binary.BigEndian.PutUint16(response[:2], 0x0101)
	return response
}

func TestSTUNSuccessValidation(t *testing.T) {
	request := make([]byte, 20)
	binary.BigEndian.PutUint16(request[:2], 0x0001)
	binary.BigEndian.PutUint32(request[4:8], stunCookie)
	copy(request[8:], "abcdefghijkl")
	valid := stunReply(request)
	tests := map[string][]byte{"valid": valid, "request": request, "short": valid[:19]}
	cookie := append([]byte(nil), valid...)
	cookie[4] = 0
	tests["cookie"] = cookie
	length := append([]byte(nil), valid...)
	length[3] = 4
	tests["length"] = length
	malformed := append(append([]byte(nil), valid...), 0, 1, 0, 5)
	malformed[3] = 4
	tests["attribute"] = malformed
	for name, packet := range tests {
		t.Run(name, func(t *testing.T) {
			id, ok := stunSuccess(packet)
			if ok != (name == "valid") {
				t.Fatalf("accepted=%v", ok)
			}
			if ok && string(id[:]) != "abcdefghijkl" {
				t.Fatal(id)
			}
		})
	}
}

func TestUDPMeasurementsUseLiteralThresholdAndSendOrder(t *testing.T) {
	ts := []stunTransaction{{sent: time.Now(), answered: true, rtt: 30 * time.Millisecond}, {sent: time.Now(), answered: true, rtt: 10 * time.Millisecond}, {sent: time.Now(), answered: true, rtt: 20 * time.Millisecond}, {sent: time.Now()}, {sent: time.Now()}, {sent: time.Now()}}
	r := udpMeasured(ts)
	if r.state != "ok" || r.sent != 6 || r.answered != 3 || r.rtt != 20*time.Millisecond || r.jitter != 15*time.Millisecond {
		t.Fatalf("%+v", r)
	}
	ts[2].answered = false
	r = udpMeasured(ts)
	if r.state != "lossy" || r.rtt != 20*time.Millisecond {
		t.Fatalf("%+v", r)
	}
	ts[0].answered = false
	ts[1].answered = false
	if r = udpMeasured(ts); r.state != "dead" {
		t.Fatalf("%+v", r)
	}
}

type stunConn struct {
	packets              chan []byte
	closed               chan struct{}
	first                chan struct{}
	closeOnce, firstOnce sync.Once
	readExited           atomic.Bool
	onWrite              func([]byte, int)
	count                int
}

func newSTUNConn() *stunConn {
	return &stunConn{packets: make(chan []byte, 32), closed: make(chan struct{}), first: make(chan struct{})}
}
func (c *stunConn) Read(p []byte) (int, error) {
	select {
	case packet := <-c.packets:
		return copy(p, packet), nil
	case <-c.closed:
		c.readExited.Store(true)
		return 0, io.EOF
	}
}
func (c *stunConn) Write(p []byte) (int, error) {
	c.count++
	c.firstOnce.Do(func() { close(c.first) })
	if c.onWrite != nil {
		c.onWrite(append([]byte(nil), p...), c.count)
	}
	return len(p), nil
}
func (c *stunConn) Close() error                     { c.closeOnce.Do(func() { close(c.closed) }); return nil }
func (c *stunConn) LocalAddr() stdnet.Addr           { return nil }
func (c *stunConn) RemoteAddr() stdnet.Addr          { return nil }
func (c *stunConn) SetDeadline(time.Time) error      { return nil }
func (c *stunConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stunConn) SetWriteDeadline(time.Time) error { return nil }

func TestSTUNOwnIDsDuplicatesAndCleanup(t *testing.T) {
	conn := newSTUNConn()
	ids := map[string]bool{}
	conn.onWrite = func(p []byte, index int) {
		id := string(p[8:])
		if ids[id] {
			t.Error("duplicate request ID")
		}
		ids[id] = true
		foreign := stunReply(p)
		foreign[8] ^= 1
		conn.packets <- foreign
		malformed := stunReply(p)
		malformed[3] = 4
		conn.packets <- malformed
		if index <= 2 {
			conn.packets <- stunReply(p)
			conn.packets <- stunReply(p)
		}
	}
	r := runSTUN(context.Background(), conn, &udpSettings{requests: 5, interval: time.Millisecond, timeout: 30 * time.Millisecond})
	if r.state != "lossy" || r.answered != 2 || r.sent != 5 || !conn.readExited.Load() {
		t.Fatalf("%+v exited=%v", r, conn.readExited.Load())
	}
}

func TestSTUNCancelClosesNoOpDeadlineConnection(t *testing.T) {
	conn := newSTUNConn()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan udpProbeResult, 1)
	go func() {
		result <- runSTUN(ctx, conn, &udpSettings{requests: 5, interval: time.Hour, timeout: time.Hour})
	}()
	<-conn.first
	cancel()
	select {
	case r := <-result:
		if !r.canceled || !conn.readExited.Load() {
			t.Fatalf("%+v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("reader or writer did not stop")
	}
}

func TestUDPProbeFallbackAndStrictTaggedContext(t *testing.T) {
	previous := tagged.Dialer
	defer func() { tagged.Dialer = previous }()
	var destinations []string
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, dest net.Destination, tag string) (net.Conn, error) {
		if tag != "selected" || dest.Network != net.Network_UDP || !dest.Address.Family().IsDomain() {
			t.Fatalf("tag=%s dest=%v", tag, dest)
		}
		content := session.ContentFromContext(ctx)
		if content == nil || !content.SkipDNSResolve || !content.SkipMuxReuse {
			t.Fatalf("content=%+v", content)
		}
		state := probeStateFrom(ctx)
		if state == nil {
			t.Fatal("not a probe")
		}
		state.recordDial(nil)
		destinations = append(destinations, dest.Address.String())
		conn := newSTUNConn()
		if len(destinations) == 2 {
			conn.onWrite = func(p []byte, _ int) { conn.packets <- stunReply(p) }
		}
		return conn, nil
	}
	o := &Observer{runCtx: context.Background(), s: newSettings(&Config{})}
	settings := &udpSettings{requests: 5, interval: time.Millisecond, timeout: 30 * time.Millisecond, destinations: []net.Destination{net.UDPDestination(net.DomainAddress("first.test"), 3478), net.UDPDestination(net.DomainAddress("second.test"), 3478), net.UDPDestination(net.DomainAddress("third.test"), 3478)}}
	r := o.probeUDP("selected", settings)
	if r.state != "ok" || r.destinationIndex != 1 || !reflect.DeepEqual(destinations, []string{"first.test", "second.test"}) {
		t.Fatalf("%+v %v", r, destinations)
	}
	destinations = nil
	settings.destinations = settings.destinations[1:2]
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, dest net.Destination, tag string) (net.Conn, error) {
		destinations = append(destinations, dest.Address.String())
		probeStateFrom(ctx).recordDial(nil)
		conn := newSTUNConn()
		conn.onWrite = func(p []byte, _ int) { conn.packets <- stunReply(p) }
		return conn, nil
	}
	r = o.probeUDP("selected", settings)
	if r.destinationIndex != 0 || r.answered != 5 || len(destinations) != 1 {
		t.Fatalf("%+v", r)
	}
}

func TestUDPProbeImmediateAndAsyncRefusal(t *testing.T) {
	previous := tagged.Dialer
	defer func() { tagged.Dialer = previous }()
	o := &Observer{runCtx: context.Background(), s: newSettings(&Config{})}
	settings := &udpSettings{requests: 5, interval: time.Millisecond, timeout: time.Millisecond, destinations: []net.Destination{net.UDPDestination(net.DomainAddress("stun.test"), 3478)}}
	for _, async := range []bool{false, true} {
		tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, _ net.Destination, _ string) (net.Conn, error) {
			err := errors.New("UDP refused")
			if !async {
				return nil, err
			}
			conn := newSTUNConn()
			conn.onWrite = func([]byte, int) { probeStateFrom(ctx).recordDial(err) }
			return conn, nil
		}
		r := o.probeUDP("selected", settings)
		if r.state != "unsupported" || r.class == "" || r.canceled {
			t.Fatalf("async=%v %+v", async, r)
		}
	}
}

type blockingSTUNConn struct {
	*stunConn
	successfulWrites int
	entered          chan struct{}
	writeDone        chan struct{}
}

func newBlockingSTUNConn(successfulWrites int) *blockingSTUNConn {
	return &blockingSTUNConn{stunConn: newSTUNConn(), successfulWrites: successfulWrites, entered: make(chan struct{}), writeDone: make(chan struct{})}
}

func (c *blockingSTUNConn) Write(packet []byte) (int, error) {
	if c.count < c.successfulWrites {
		return c.stunConn.Write(packet)
	}
	close(c.entered)
	<-c.closed
	close(c.writeDone)
	return 0, io.ErrClosedPipe
}

func TestUDPBlockingWriteAfterSuccessfulDialIsBounded(t *testing.T) {
	previous := tagged.Dialer
	defer func() { tagged.Dialer = previous }()
	for _, successfulWrites := range []int{0, 1} {
		t.Run(string(rune('0'+successfulWrites)), func(t *testing.T) {
			conn := newBlockingSTUNConn(successfulWrites)
			tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, _ net.Destination, _ string) (net.Conn, error) {
				probeStateFrom(ctx).recordDial(nil)
				return conn, nil
			}
			o := &Observer{runCtx: context.Background(), s: newSettings(&Config{})}
			o.s.passive.dialTimeout = 10 * time.Millisecond
			settings := &udpSettings{requests: 5, interval: time.Millisecond, timeout: 30 * time.Millisecond,
				destinations: []net.Destination{net.UDPDestination(net.DomainAddress("stun.test"), 3478)}}
			results := make(chan udpProbeResult, 1)
			go func() { results <- o.probeUDP("selected", settings) }()
			select {
			case <-conn.entered:
			case <-time.After(time.Second):
				t.Fatal("write did not start")
			}
			select {
			case r := <-results:
				if r.canceled || r.state != "dead" || r.class != "send_timeout" || r.sent != successfulWrites || r.answered != 0 {
					t.Fatalf("%+v", r)
				}
				if !conn.readExited.Load() {
					t.Fatal("reader was not joined")
				}
				select {
				case <-conn.writeDone:
				default:
					t.Fatal("writer was not joined")
				}
			case <-time.After(time.Second):
				_ = conn.Close()
				t.Fatal("send timeout did not release blocking Write")
			}
		})
	}
}

func TestUDPBlockingWriteParentCancelRemainsCanceled(t *testing.T) {
	conn := newBlockingSTUNConn(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan udpProbeResult, 1)
	go func() {
		results <- runSTUN(ctx, conn, &udpSettings{requests: 5, interval: time.Millisecond, timeout: time.Hour})
	}()
	<-conn.entered
	cancel()
	select {
	case r := <-results:
		if !r.canceled || r.sent != 0 || !conn.readExited.Load() {
			t.Fatalf("%+v", r)
		}
	case <-time.After(time.Second):
		_ = conn.Close()
		t.Fatal("parent cancellation left Write running")
	}
}

func TestUDPSendTimeoutReleasesSlotForTCPFailover(t *testing.T) {
	te := udpEngineFixture(t)
	udp := te.enqueueUDP("a1", reasonHost, "auto", t0)
	te.beginUDP(udp, t0)
	tcp := te.enqueue("a2", reasonFailover, "auto", at(time.Millisecond))
	te.sel.failoverProbing = map[string]bool{"a2": true}
	if jobs, _ := te.tick(at(time.Millisecond)); len(jobs) != 0 {
		t.Fatalf("TCP started before UDP finished: %+v", jobs)
	}
	conn := newBlockingSTUNConn(0)
	results := make(chan udpProbeResult, 1)
	go func() {
		results <- runSTUN(context.Background(), conn, &udpSettings{requests: 5, interval: time.Millisecond, timeout: 30 * time.Millisecond})
	}()
	select {
	case r := <-results:
		if r.class != "send_timeout" || r.canceled {
			t.Fatalf("%+v", r)
		}
		te.onUDPProbeResult(udp, r, at(time.Second))
	case <-time.After(time.Second):
		_ = conn.Close()
		t.Fatal("UDP worker did not finish")
	}
	jobs, _ := te.tick(at(2 * time.Second))
	if len(jobs) != 1 || jobs[0] != tcp || jobs[0].isUDP {
		t.Fatalf("TCP failover slot was not released: %+v", jobs)
	}
}

func TestUDPReceiveTimeoutStartsAfterLastSuccessfulWrite(t *testing.T) {
	conn := newSTUNConn()
	settings := &udpSettings{requests: 5, interval: 10 * time.Millisecond, timeout: 40 * time.Millisecond}
	var replies sync.WaitGroup
	conn.onWrite = func(packet []byte, index int) {
		if index != settings.requests {
			return
		}
		replies.Add(1)
		go func() { defer replies.Done(); time.Sleep(3 * settings.timeout / 4); conn.packets <- stunReply(packet) }()
	}
	r := runSTUN(context.Background(), conn, settings)
	replies.Wait()
	if r.state != "lossy" || r.sent != 5 || r.answered != 1 || r.class != "" || r.canceled {
		t.Fatalf("%+v", r)
	}
	if !conn.readExited.Load() {
		t.Fatal("reader was not joined")
	}
}

func TestUDPProbeUsesFourthDeclaredDestination(t *testing.T) {
	previous := tagged.Dialer
	defer func() { tagged.Dialer = previous }()
	var seen []string
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, dest net.Destination, _ string) (net.Conn, error) {
		probeStateFrom(ctx).recordDial(nil)
		seen = append(seen, dest.Address.String())
		conn := newSTUNConn()
		if len(seen) == 4 {
			conn.onWrite = func(p []byte, _ int) { conn.packets <- stunReply(p) }
		}
		return conn, nil
	}
	observer := &Observer{runCtx: context.Background(), s: newSettings(&Config{})}
	settings := &udpSettings{requests: 5, interval: time.Millisecond, timeout: 20 * time.Millisecond}
	for _, name := range []string{"first.test", "second.test", "third.test", "fourth.test"} {
		settings.destinations = append(settings.destinations, net.UDPDestination(net.DomainAddress(name), 3478))
	}
	result := observer.probeUDP("selected", settings)
	if result.state != "ok" || result.destinationIndex != 3 || len(seen) != 4 {
		t.Fatalf("result=%+v seen=%v", result, seen)
	}
}
