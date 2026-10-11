package health_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

type wireGuardSenderFakePacket struct {
	net.PacketConn
	write func([]byte, net.Addr) (int, error)
}

func (c *wireGuardSenderFakePacket) WriteTo(p []byte, addr net.Addr) (int, error) {
	if c.write != nil {
		return c.write(p, addr)
	}
	return len(p), nil
}

func (c *wireGuardSenderFakePacket) Close() error { return nil }

type wireGuardSenderFakeDialer struct {
	dial func() (net.Conn, error)
}

func (d *wireGuardSenderFakeDialer) Dial(context.Context, net.Address, net.Destination, *internet.SocketConfig) (net.Conn, error) {
	return d.dial()
}

func (d *wireGuardSenderFakeDialer) DestIpAddress() net.IP { return nil }

func TestWireGuardSenderBaselineRejectsWriteOverlappingClose(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	ledger := &wireGuardSenderLedger{}
	packet := &wireGuardSenderFakePacket{write: func(p []byte, _ net.Addr) (int, error) {
		close(entered)
		<-release
		return len(p), nil
	}}
	c := ledger.finishDial(ledger.begin("dial", 0, 0), &internet.PacketConnWrapper{PacketConn: packet}, nil).(*internet.PacketConnWrapper)
	done := make(chan struct{})
	go func() {
		c.WriteTo([]byte{1}, nil)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write did not enter the underlying socket")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	before := ledger.snapshot()
	if !before.generations[0].closed || before.closedProblem() == "" {
		t.Fatalf("completed close hid the outstanding write: %+v", before)
	}
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("write did not finish after release")
	}
	if problem := ledger.snapshot().closedProblem(); problem != "" {
		t.Fatal(problem)
	}
	if !before.calls[1].finished.IsZero() {
		t.Fatal("later completion changed the earlier snapshot")
	}
}

func TestWireGuardSenderBaselineRejectsDialCreatingLateGeneration(t *testing.T) {
	registry := &wireGuardSenderRegistry{ledgers: make(map[string]*wireGuardSenderLedger)}
	ledger, _ := registry.reserve("127.0.0.1:12001", t.Name())
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	dialer := &wireGuardSenderDialer{registry: registry, SystemDialer: &wireGuardSenderFakeDialer{dial: func() (net.Conn, error) {
		close(entered)
		<-release
		return &internet.PacketConnWrapper{PacketConn: &wireGuardSenderFakePacket{}}, nil
	}}}
	done := make(chan net.Conn, 1)
	go func() {
		c, _ := dialer.Dial(context.Background(), nil, net.UDPDestination(net.ParseAddress("127.0.0.1"), 12001), nil)
		done <- c
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("dial did not enter the underlying dialer")
	}
	before := ledger.snapshot()
	if len(before.calls) != 1 || before.calls[0].kind != "dial" || before.closedProblem() == "" {
		t.Fatalf("outstanding dial was not visible before its socket existed: %+v", before)
	}
	unblock()
	var c net.Conn
	select {
	case c = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dial did not finish after release")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	after := ledger.snapshot()
	if len(after.generations) != 1 || after.closedProblem() != "" {
		t.Fatalf("late socket was not tracked through close: %+v", after)
	}
	if len(before.generations) != 0 || !before.calls[0].finished.IsZero() {
		t.Fatal("late generation changed the earlier snapshot")
	}
}

func TestWireGuardSenderEndpointRetainsOldLedgerAndFailedAttempts(t *testing.T) {
	registry := &wireGuardSenderRegistry{ledgers: make(map[string]*wireGuardSenderLedger)}
	old, ok := registry.reserve("127.0.0.1:12002", "old fixture")
	if !ok {
		t.Fatal("first endpoint reservation failed")
	}
	failedWrite := errors.New("write failed")
	dialer := &wireGuardSenderDialer{registry: registry, SystemDialer: &wireGuardSenderFakeDialer{dial: func() (net.Conn, error) {
		return &internet.PacketConnWrapper{PacketConn: &wireGuardSenderFakePacket{write: func([]byte, net.Addr) (int, error) {
			return 0, failedWrite
		}}}, nil
	}}}
	destination := net.UDPDestination(net.ParseAddress("127.0.0.1"), 12002)
	first, err := dialer.Dial(context.Background(), nil, destination, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	before := old.closedSnapshot(t)
	if replacement, ok := registry.reserve("127.0.0.1:12002", "new fixture"); ok || replacement != nil {
		t.Fatal("endpoint ownership was replaced after close")
	}
	other, ok := registry.reserve("127.0.0.1:12003", "new fixture")
	if !ok || other.epoch == old.epoch {
		t.Fatal("distinct endpoint did not get its own epoch")
	}
	if _, err := first.(*internet.PacketConnWrapper).WriteTo([]byte{1}, nil); !errors.Is(err, failedWrite) {
		t.Fatalf("write result changed: %v", err)
	}
	late, err := dialer.Dial(context.Background(), nil, destination, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := late.Close(); err != nil {
		t.Fatal(err)
	}
	after := old.snapshot()
	if len(after.calls)-len(before.calls) != 3 || after.calls[len(before.calls)].err != failedWrite.Error() || len(after.generations) != 2 {
		t.Fatalf("late dial or failed write escaped the old ledger: before=%+v after=%+v", before, after)
	}
	if len(other.snapshot().calls) != 0 || registry.lookup("127.0.0.1:12002") != old {
		t.Fatal("old operations were attributed to the new fixture")
	}
}
