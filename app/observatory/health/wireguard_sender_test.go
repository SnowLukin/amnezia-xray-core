package health_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

var wireGuardSenders = &wireGuardSenderRegistry{ledgers: make(map[string]*wireGuardSenderLedger)}

func TestMain(m *testing.M) {
	// Instance.Close does not join every dialer caller; keep this dialer until process exit.
	internet.UseAlternativeSystemDialer(&wireGuardSenderDialer{
		SystemDialer: &internet.DefaultSystemDialer{},
		registry:     wireGuardSenders,
	})
	os.Exit(m.Run())
}

type wireGuardSenderRegistry struct {
	mu      sync.Mutex
	ledgers map[string]*wireGuardSenderLedger
	next    uint64
}

func (r *wireGuardSenderRegistry) reserve(endpoint, owner string) (*wireGuardSenderLedger, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.ledgers[endpoint]; exists {
		return nil, false
	}
	r.next++
	l := &wireGuardSenderLedger{endpoint: endpoint, owner: owner, epoch: r.next}
	// A late dial must retain its old fixture even after that fixture's cleanup.
	r.ledgers[endpoint] = l
	return l, true
}

func (r *wireGuardSenderRegistry) lookup(endpoint string) *wireGuardSenderLedger {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ledgers[endpoint]
}

type wireGuardSenderDialer struct {
	internet.SystemDialer
	registry *wireGuardSenderRegistry
}

func (d *wireGuardSenderDialer) Dial(ctx context.Context, source net.Address, destination net.Destination, sockopt *internet.SocketConfig) (net.Conn, error) {
	var ledger *wireGuardSenderLedger
	if destination.Network == net.Network_UDP {
		ledger = d.registry.lookup(destination.NetAddr())
	}
	if ledger == nil {
		return d.SystemDialer.Dial(ctx, source, destination, sockopt)
	}
	id := ledger.begin("dial", 0, 0)
	c, err := d.SystemDialer.Dial(ctx, source, destination, sockopt)
	return ledger.finishDial(id, c, err), err
}

type wireGuardSenderCall struct {
	kind       string
	generation int
	started    time.Time
	finished   time.Time
	size       int
	written    int
	err        string
}

type wireGuardSenderGeneration struct {
	closed bool
}

type wireGuardSenderSnapshot struct {
	endpoint    string
	owner       string
	epoch       uint64
	calls       []wireGuardSenderCall
	generations []wireGuardSenderGeneration
	problem     string
}

type wireGuardSenderLedger struct {
	mu          sync.Mutex
	endpoint    string
	owner       string
	epoch       uint64
	calls       []wireGuardSenderCall
	generations []wireGuardSenderGeneration
	problem     string
}

func (l *wireGuardSenderLedger) begin(kind string, generation, size int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	id := len(l.calls)
	l.calls = append(l.calls, wireGuardSenderCall{kind: kind, generation: generation, size: size, started: time.Now()})
	return id
}

func (l *wireGuardSenderLedger) finishLocked(id, written int, err error) {
	call := &l.calls[id]
	call.finished = time.Now()
	call.written = written
	if err != nil {
		call.err = err.Error()
	}
}

func (l *wireGuardSenderLedger) finishDial(id int, c net.Conn, err error) net.Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.finishLocked(id, 0, err)
	if err != nil {
		return c
	}
	generation := len(l.generations) + 1
	l.generations = append(l.generations, wireGuardSenderGeneration{})
	l.calls[id].generation = generation
	packet, ok := c.(*internet.PacketConnWrapper)
	if !ok || packet == nil || packet.PacketConn == nil {
		l.problem = fmt.Sprintf("unobserved UDP connection type %T", c)
		return c
	}
	packet.PacketConn = &wireGuardSenderPacketConn{PacketConn: packet.PacketConn, ledger: l, generation: generation}
	return c
}

func (l *wireGuardSenderLedger) snapshot() wireGuardSenderSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return wireGuardSenderSnapshot{
		endpoint:    l.endpoint,
		owner:       l.owner,
		epoch:       l.epoch,
		calls:       append([]wireGuardSenderCall(nil), l.calls...),
		generations: append([]wireGuardSenderGeneration(nil), l.generations...),
		problem:     l.problem,
	}
}

func (s wireGuardSenderSnapshot) closedProblem() string {
	if s.problem != "" {
		return s.problem
	}
	for _, call := range s.calls {
		if call.finished.IsZero() {
			return fmt.Sprintf("outstanding %s for generation %d", call.kind, call.generation)
		}
	}
	if len(s.generations) == 0 {
		return "no observed socket generation"
	}
	for i, generation := range s.generations {
		if !generation.closed {
			return fmt.Sprintf("generation %d has no successful completed close", i+1)
		}
	}
	return ""
}

func (l *wireGuardSenderLedger) closedSnapshot(t *testing.T) wireGuardSenderSnapshot {
	t.Helper()
	s := l.snapshot()
	if problem := s.closedProblem(); problem != "" {
		t.Fatalf("inconclusive sender baseline: %s; %+v", problem, s)
	}
	return s
}

type wireGuardSenderPacketConn struct {
	net.PacketConn
	ledger     *wireGuardSenderLedger
	generation int
}

func (c *wireGuardSenderPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	id := c.ledger.begin("write", c.generation, len(p))
	n, err := c.PacketConn.WriteTo(p, addr)
	c.ledger.mu.Lock()
	c.ledger.finishLocked(id, n, err)
	c.ledger.mu.Unlock()
	return n, err
}

func (c *wireGuardSenderPacketConn) Close() error {
	id := c.ledger.begin("close", c.generation, 0)
	err := c.PacketConn.Close()
	c.ledger.mu.Lock()
	c.ledger.finishLocked(id, 0, err)
	c.ledger.generations[c.generation-1].closed = err == nil
	c.ledger.mu.Unlock()
	return err
}
