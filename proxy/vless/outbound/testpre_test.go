package outbound

import (
	"bytes"
	"context"
	gonet "net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/proxy/vless"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/pipe"
)

type trackingConn struct {
	gonet.Conn
	closed atomic.Bool
}

func (c *trackingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// preDialer hands out connections whose server side is closed; with hold set, each dial waits for it or for ctx
type preDialer struct {
	mu      sync.Mutex
	conns   []*trackingConn
	hold    chan struct{}
	dialing atomic.Int32
}

func (d *preDialer) Dial(ctx context.Context, _ net.Destination) (stat.Connection, error) {
	if d.hold != nil {
		d.dialing.Add(1)
		defer d.dialing.Add(-1)
		select {
		case <-d.hold:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	client, server := gonet.Pipe()
	server.Close()
	c := &trackingConn{Conn: client}
	d.mu.Lock()
	d.conns = append(d.conns, c)
	d.mu.Unlock()
	return c, nil
}

func (d *preDialer) DestIpAddress() net.IP { return nil }

func (d *preDialer) SetOutboundGateway(context.Context, *session.Outbound) {}

func (d *preDialer) allClosed() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.conns {
		if !c.closed.Load() {
			return false
		}
	}
	return true
}

func (d *preDialer) dialed() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns)
}

func newTestpreHandler(n uint32) *Handler {
	h := &Handler{
		server: &protocol.ServerSpec{
			Destination: net.TCPDestination(net.LocalHostIP, 443),
			User:        &protocol.MemoryUser{Account: &vless.MemoryAccount{ID: protocol.NewID(uuid.New())}},
		},
		policyManager: policy.DefaultManager{},
		testpre:       n,
	}
	h.initLifetime()
	return h
}

func processAsync(ctx context.Context, h *Handler, d *preDialer) <-chan error {
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: net.TCPDestination(net.ParseAddress("1.1.1.1"), 80)}})
	uplinkReader, uplinkWriter := pipe.New()
	downlinkReader, downlinkWriter := pipe.New()
	uplinkWriter.Close()
	done := make(chan error, 1)
	go func() {
		defer downlinkReader.Interrupt()
		done <- h.Process(ctx, &transport.Link{Reader: uplinkReader, Writer: downlinkWriter}, d)
	}()
	return done
}

func waitResult(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: Process did not return", what)
		return nil
	}
}

func producers() int {
	stacks := make([]byte, 1<<20)
	stacks = stacks[:runtime.Stack(stacks, true)]
	return bytes.Count(stacks, []byte("(*Handler).preConnect("))
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type failingDialer struct {
	preDialer
	dials atomic.Int32
}

func (d *failingDialer) Dial(context.Context, net.Destination) (stat.Connection, error) {
	d.dials.Add(1)
	return nil, gonet.ErrClosed
}

func TestTestpreWaitsAfterDialError(t *testing.T) {
	h := newTestpreHandler(1)
	d := &failingDialer{}
	go h.preConnect(d, h.server.Destination)
	time.Sleep(500 * time.Millisecond)
	h.Close()
	if n := d.dials.Load(); n > 4 {
		t.Fatalf("%d dials in 500 ms", n)
	}
}

func TestTestpreCloseWithBlockedProducers(t *testing.T) {
	h := newTestpreHandler(3)
	d := &preDialer{}
	waitResult(t, processAsync(context.Background(), h, d), "first connection")
	// the producers that did not serve the first connection now block on handing over what they dialed
	eventually(t, "producers did not dial ahead", func() bool { return d.dialed() >= 3 })

	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal("second Close:", err)
	}
	eventually(t, "a pre-dialed connection held by a producer is left open", d.allClosed)
	eventually(t, "producer goroutines outlive Close", func() bool { return producers() == 0 })
}

func TestTestpreCloseDuringDial(t *testing.T) {
	h := newTestpreHandler(2)
	d := &preDialer{hold: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := processAsync(ctx, h, d)
	eventually(t, "producers did not start dialing", func() bool { return d.dialing.Load() == 2 })

	h.Close()
	if err := waitResult(t, done, "Close while waiting for a pre-connection"); err == nil {
		t.Fatal("Process on a closed handler succeeded")
	}
	eventually(t, "dials of a closed handler are not cancelled", func() bool { return d.dialing.Load() == 0 })
	eventually(t, "producer goroutines outlive Close", func() bool { return producers() == 0 })
}

func TestTestpreProcessAfterClose(t *testing.T) {
	h := newTestpreHandler(2)
	d := &preDialer{}
	h.Close()
	if err := waitResult(t, processAsync(context.Background(), h, d), "Process after Close"); err == nil {
		t.Fatal("Process on a closed handler succeeded")
	}
	if n := d.dialed(); n != 0 {
		t.Fatalf("closed handler dialed %d connections", n)
	}
}

func TestTestpreProcessHonoursContext(t *testing.T) {
	h := newTestpreHandler(1)
	defer h.Close()
	d := &preDialer{hold: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := processAsync(ctx, h, d)
	eventually(t, "producer did not start dialing", func() bool { return d.dialing.Load() == 1 })
	cancel()
	if err := waitResult(t, done, "cancelled connection waiting for a pre-connection"); err == nil {
		t.Fatal("cancelled Process succeeded")
	}
}

func TestTestpreNewHandlerWorksAfterClose(t *testing.T) {
	old := newTestpreHandler(3)
	d := &preDialer{}
	waitResult(t, processAsync(context.Background(), old, d), "old handler")
	eventually(t, "producers did not dial ahead", func() bool { return d.dialed() >= 3 })
	old.Close()
	eventually(t, "old producers did not stop", d.allClosed)

	fresh := newTestpreHandler(1)
	defer fresh.Close()
	fd := &preDialer{}
	waitResult(t, processAsync(context.Background(), fresh, fd), "new handler after Close of another")
	if fd.dialed() == 0 {
		t.Fatal("new handler did not use a pre-connection")
	}
}

func TestTestpreCloseConcurrentWithProcess(t *testing.T) {
	h := newTestpreHandler(2)
	d := &preDialer{}
	results := make([]<-chan error, 8)
	for i := range results {
		results[i] = processAsync(context.Background(), h, d)
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Close()
		}()
	}
	wg.Wait()
	for _, r := range results {
		waitResult(t, r, "Process racing Close")
	}
	eventually(t, "a pre-dialed connection is left open", d.allClosed)
}
