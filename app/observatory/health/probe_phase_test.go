package health

import (
	"bufio"
	"context"
	"errors"
	"io"
	gonet "net"
	"net/http"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	_ "github.com/xtls/xray-core/transport/internet/tagged/taggedimpl"
)

type probePhaseDispatcher struct {
	o          *Observer
	dialDelay  time.Duration
	reportDial bool
	serve      func(gonet.Conn)
}

func (*probePhaseDispatcher) Type() interface{} { return routing.DispatcherType() }
func (*probePhaseDispatcher) Start() error      { return nil }
func (*probePhaseDispatcher) Close() error      { return nil }

func (d *probePhaseDispatcher) Dispatch(ctx context.Context, dest net.Destination) (*transport.Link, error) {
	if d.dialDelay > 0 {
		timer := time.NewTimer(d.dialDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if d.reportDial {
		d.o.StartDial(ctx, "a1", dest, nil).Dialed(nil, nil, nil, nil)
	}
	if d.serve == nil {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	client, server := gonet.Pipe()
	go func() {
		<-ctx.Done()
		client.Close()
		server.Close()
	}()
	go func() {
		defer server.Close()
		d.serve(server)
	}()
	return &transport.Link{Reader: buf.NewReader(client), Writer: buf.NewWriter(client)}, nil
}

func (d *probePhaseDispatcher) DispatchLink(context.Context, net.Destination, *transport.Link) error {
	return errors.New("unused")
}

func probePhaseObserver(dialTimeout time.Duration, d *probePhaseDispatcher) *Observer {
	ctx := context.WithValue(context.Background(), core.XrayKey(1), &core.Instance{})
	o := newObserver(ctx, &Config{})
	o.s.passive.dialTimeout = dialTimeout
	o.dispatcher = d
	d.o = o
	return o
}

func probePhaseSettings(progress, timeout time.Duration, minBytes int64) *probeSettings {
	return &probeSettings{
		destination:     "http://example.test/check",
		method:          http.MethodGet,
		minBytes:        minBytes,
		progressTimeout: progress,
		timeout:         timeout,
	}
}

func serveProbeChunks(c gonet.Conn, firstByteDelay, chunkDelay time.Duration, chunks int) {
	req, err := http.ReadRequest(bufio.NewReader(c))
	if err != nil {
		return
	}
	req.Body.Close()
	if firstByteDelay > 0 {
		time.Sleep(firstByteDelay)
	}
	if _, err := io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n"); err != nil {
		return
	}
	for i := 0; i < chunks; i++ {
		if _, err := io.WriteString(c, "x"); err != nil {
			return
		}
		if i+1 < chunks {
			time.Sleep(chunkDelay)
		}
	}
}

func TestProbePhaseFlowSlowDialLiveResponse(t *testing.T) {
	d := &probePhaseDispatcher{dialDelay: 100 * time.Millisecond, reportDial: true,
		serve: func(c gonet.Conn) { serveProbeChunks(c, 100*time.Millisecond, 0, 10) }}
	o := probePhaseObserver(250*time.Millisecond, d)
	defer o.stopRun()
	r := o.probe("a1", probePhaseSettings(40*time.Millisecond, 400*time.Millisecond, 10))
	if !r.ok {
		t.Fatalf("slow dial and live response: %+v", r)
	}
}

func TestProbePhaseFlowSlowResponse(t *testing.T) {
	d := &probePhaseDispatcher{reportDial: true,
		serve: func(c gonet.Conn) { serveProbeChunks(c, 120*time.Millisecond, 0, 10) }}
	o := probePhaseObserver(250*time.Millisecond, d)
	defer o.stopRun()
	r := o.probe("a1", probePhaseSettings(40*time.Millisecond, 400*time.Millisecond, 10))
	if r.class != "no_progress" {
		t.Fatalf("slow response: %+v, want no_progress", r)
	}
}

func TestProbePhaseFlowDeadDial(t *testing.T) {
	d := &probePhaseDispatcher{}
	o := probePhaseObserver(80*time.Millisecond, d)
	defer o.stopRun()
	r := o.probe("a1", probePhaseSettings(40*time.Millisecond, 400*time.Millisecond, 10))
	if r.class != "dial_timeout" {
		t.Fatalf("dead dial: %+v, want dial_timeout", r)
	}
}

func TestProbePhaseFlowResponseWithoutDialed(t *testing.T) {
	d := &probePhaseDispatcher{serve: func(c gonet.Conn) { serveProbeChunks(c, 0, 35*time.Millisecond, 10) }}
	o := probePhaseObserver(80*time.Millisecond, d)
	defer o.stopRun()
	r := o.probe("a1", probePhaseSettings(60*time.Millisecond, 600*time.Millisecond, 10))
	if !r.ok {
		t.Fatalf("streaming response without Dialed: %+v", r)
	}
}

func TestProbePhaseFlowNoDialedDoesNotExtendStep(t *testing.T) {
	d := &probePhaseDispatcher{serve: func(c gonet.Conn) { serveProbeChunks(c, 300*time.Millisecond, 30*time.Millisecond, 10) }}
	o := probePhaseObserver(time.Second, d)
	defer o.stopRun()
	r := o.probe("a1", probePhaseSettings(100*time.Millisecond, 500*time.Millisecond, 10))
	if r.class != "timeout" {
		t.Fatalf("response without Dialed extended the step: %+v, want timeout", r)
	}
}

func TestProbePhaseFlowRequestCapAfterDialed(t *testing.T) {
	d := &probePhaseDispatcher{dialDelay: 50 * time.Millisecond, reportDial: true,
		serve: func(c gonet.Conn) { serveProbeChunks(c, 0, 35*time.Millisecond, 10) }}
	o := probePhaseObserver(200*time.Millisecond, d)
	defer o.stopRun()
	r := o.probe("a1", probePhaseSettings(200*time.Millisecond, 70*time.Millisecond, 10))
	if r.class != "timeout" {
		t.Fatalf("request cap after dial: %+v, want timeout", r)
	}
}

func TestProbePhaseAbsoluteCapHasRequestClass(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := &probeState{}
	state.begin(time.Now().Add(-maxProbeTimeout-time.Millisecond), cancel, time.Minute, time.Minute, time.Minute)
	defer state.stop()
	state.recordDial(errors.New("late dial"))
	if reason, _, _ := state.result(); reason != "timeout" {
		t.Fatalf("absolute cap: reason %q, want timeout", reason)
	}
}

func TestProbePhaseFlowAbsoluteCapWithoutDialed(t *testing.T) {
	d := &probePhaseDispatcher{}
	o := probePhaseObserver(time.Minute, d)
	defer o.stopRun()
	start := time.Now()
	r := o.probe("a1", probePhaseSettings(time.Minute, time.Minute, 10))
	if r.class != "timeout" || time.Since(start) > maxProbeTimeout+2*time.Second {
		t.Fatalf("absolute cap without Dialed after %v: %+v, want timeout", time.Since(start), r)
	}
}
