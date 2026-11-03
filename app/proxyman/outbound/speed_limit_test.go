package outbound

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

func speedLimitPayload(bytes int) buf.MultiBuffer {
	var mb buf.MultiBuffer
	for bytes > 0 {
		b := buf.New()
		n := min(bytes, int(buf.Size))
		b.Extend(int32(n))
		mb = append(mb, b)
		bytes -= n
	}
	return mb
}

func drain(r buf.Reader) {
	for {
		mb, err := r.ReadMultiBuffer()
		buf.ReleaseMulti(mb)
		if err != nil {
			return
		}
	}
}

func limitedDownlink(t *testing.T, ctx context.Context, l *speedLimits) (buf.Writer, func()) {
	t.Helper()
	upReader, upWriter := pipe.New(pipe.WithoutSizeLimit())
	downReader, downWriter := pipe.New(pipe.WithoutSizeLimit())
	link := &transport.Link{Reader: upReader, Writer: downWriter}
	l.limit(ctx, link)
	go drain(downReader)
	return link.Writer, func() { upWriter.Close(); downReader.Interrupt() }
}

func writeChunks(w buf.Writer, total int) error {
	for total > 0 {
		n := min(total, 64<<10)
		if err := w.WriteMultiBuffer(speedLimitPayload(n)); err != nil {
			return err
		}
		total -= n
	}
	return nil
}

func TestSpeedLimitDownlinkRate(t *testing.T) {
	// 4 MiB/s with a 1 MiB burst: 3 MiB need about half a second.
	l := newSpeedLimits(&proxyman.SpeedLimit{DownlinkBytesPerSecond: 4 << 20})
	w, done := limitedDownlink(t, context.Background(), l)
	defer done()
	start := time.Now()
	if err := writeChunks(w, 3<<20); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("3 MiB at 4 MiB/s took %v", elapsed)
	}
}

func TestSpeedLimitSharedByConnections(t *testing.T) {
	// Two connections of one outbound share 4 MiB/s: 1.5 MiB each takes as
	// long as 3 MiB through one connection.
	l := newSpeedLimits(&proxyman.SpeedLimit{DownlinkBytesPerSecond: 4 << 20})
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		w, done := limitedDownlink(t, context.Background(), l)
		defer done()
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- writeChunks(w, 3<<19)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond {
		t.Fatalf("two connections bypassed the shared budget: %v", elapsed)
	}
}

func TestSpeedLimitInterruptEndsWait(t *testing.T) {
	l := newSpeedLimits(&proxyman.SpeedLimit{DownlinkBytesPerSecond: 8 << 10})
	w, done := limitedDownlink(t, context.Background(), l)
	defer done()
	// The first write spends the whole burst; the next one would wait 8 s.
	if err := w.WriteMultiBuffer(speedLimitPayload(speedLimitMinBurst)); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- w.WriteMultiBuffer(speedLimitPayload(speedLimitMinBurst)) }()
	time.Sleep(50 * time.Millisecond)
	w.(interface{ Interrupt() }).Interrupt()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("interrupted write succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("interrupt did not end the wait")
	}
}

func TestSpeedLimitKeepsReaderInterfaces(t *testing.T) {
	l := newSpeedLimits(&proxyman.SpeedLimit{UplinkBytesPerSecond: 1 << 20})
	upReader, upWriter := pipe.New(pipe.WithoutSizeLimit())
	_, downWriter := pipe.New(pipe.WithoutSizeLimit())
	link := &transport.Link{Reader: upReader, Writer: downWriter}
	inbound := &session.Inbound{CanSpliceCopy: 2}
	l.limit(session.ContextWithInbound(context.Background(), inbound), link)

	if inbound.CanSpliceCopy != 3 {
		t.Fatalf("limited connection may splice: %d", inbound.CanSpliceCopy)
	}
	if link.Writer != downWriter {
		t.Fatal("unlimited downlink was wrapped")
	}
	timeoutReader, ok := link.Reader.(buf.TimeoutReader)
	if !ok {
		t.Fatal("limited reader lost ReadMultiBufferTimeout")
	}
	if wrapped, ok := link.Reader.(interface{ UnderlyingReader() buf.Reader }); !ok || wrapped.UnderlyingReader() != upReader {
		t.Fatal("mux cannot see the direct pipe reader")
	}
	upWriter.WriteMultiBuffer(speedLimitPayload(100))
	mb, err := timeoutReader.ReadMultiBufferTimeout(time.Second)
	if err != nil || mb.Len() != 100 {
		t.Fatalf("read %d bytes, %v", mb.Len(), err)
	}
	buf.ReleaseMulti(mb)
}

func TestSpeedLimitAbsent(t *testing.T) {
	if newSpeedLimits(nil) != nil || newSpeedLimits(&proxyman.SpeedLimit{}) != nil {
		t.Fatal("empty speed limit creates a limiter")
	}
	link := &transport.Link{}
	(*speedLimits)(nil).limit(context.Background(), link)
	if link.Reader != nil || link.Writer != nil {
		t.Fatal("nil speed limit wrapped the link")
	}
}
