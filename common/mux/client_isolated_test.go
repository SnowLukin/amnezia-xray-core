package mux

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

type isolatedWorkerFactory struct {
	created  *ClientWorker
	parent   context.Context
	uplink   *pipe.Reader
	downlink *pipe.Writer
}

func (*isolatedWorkerFactory) Create() (*ClientWorker, error) {
	return nil, errors.New("pooled worker creation called")
}

func (f *isolatedWorkerFactory) CreateWithContext(ctx context.Context) (*ClientWorker, error) {
	f.parent = ctx
	uplinkReader, uplinkWriter := pipe.New()
	downlinkReader, downlinkWriter := pipe.New()
	f.uplink = uplinkReader
	f.downlink = downlinkWriter
	worker, err := NewClientWorker(transport.Link{Reader: downlinkReader, Writer: uplinkWriter}, ClientStrategy{})
	f.created = worker
	return worker, err
}

func TestIsolatedWorkerClosesOnRequestCancelWithoutJoiningPool(t *testing.T) {
	factory := &isolatedWorkerFactory{}
	picker := &IncrementalWorkerPicker{Factory: factory}
	manager := &ClientManager{Enabled: true, Picker: picker}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("example.test"), 80)}})
	requestReader, requestWriter := pipe.New()
	responseReader, responseWriter := pipe.New()
	defer requestReader.Interrupt()
	defer requestWriter.Close()
	defer responseReader.Interrupt()
	defer responseWriter.Close()
	if err := manager.DispatchIsolated(ctx, &transport.Link{Reader: requestReader, Writer: responseWriter}); err != nil {
		cancel()
		t.Fatal(err)
	}
	if factory.parent != ctx || factory.created == nil || len(picker.workers) != 0 {
		cancel()
		t.Fatal("isolated worker lost the request context or joined the reusable pool")
	}
	defer factory.uplink.Interrupt()
	defer factory.downlink.Close()
	cancel()
	select {
	case <-factory.created.WaitClosed():
	case <-time.After(time.Second):
		t.Fatal("isolated worker stayed open after request cancellation")
	}
	deadline := time.Now().Add(time.Second)
	for !factory.created.sessionManager.Closed() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !factory.created.sessionManager.Closed() {
		t.Fatal("isolated worker left its sessions running")
	}
}

func TestIsolatedWorkerRejectsFactoryWithoutContext(t *testing.T) {
	picker := &IncrementalWorkerPicker{Factory: noContextWorkerFactory{}}
	if _, err := picker.PickIsolated(context.Background()); err == nil {
		t.Fatal("factory without context support created an isolated worker")
	}
}

type noContextWorkerFactory struct{}

func (noContextWorkerFactory) Create() (*ClientWorker, error) { return nil, nil }
