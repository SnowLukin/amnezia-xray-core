package outbound

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

type udpFlowRecorder struct {
	upBytes, upPackets     int64
	downBytes, downPackets int64
	finished               int
}

type udpFixedReader struct{ mb buf.MultiBuffer }

func (r *udpFixedReader) ReadMultiBuffer() (buf.MultiBuffer, error) { return r.mb, nil }
func (r *udpFixedReader) Interrupt()                                {}

type udpAcceptWriter struct{ wrote atomic.Bool }

func (w *udpAcceptWriter) WriteMultiBuffer(buf.MultiBuffer) error {
	w.wrote.Store(true)
	return nil
}
func (w *udpAcceptWriter) Close() error { return nil }

type udpGatedCloseWriter struct {
	entered, release chan struct{}
	closed           atomic.Bool
}

func (w *udpGatedCloseWriter) WriteMultiBuffer(buf.MultiBuffer) error {
	if w.closed.Load() {
		return io.ErrClosedPipe
	}
	return nil
}

func (w *udpGatedCloseWriter) Close() error {
	close(w.entered)
	<-w.release
	w.closed.Store(true)
	return nil
}

type udpGatedInterruptReader struct {
	mb               buf.MultiBuffer
	entered, release chan struct{}
	closed           atomic.Bool
}

func (r *udpGatedInterruptReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if r.closed.Load() {
		return nil, io.EOF
	}
	return r.mb, nil
}

func (r *udpGatedInterruptReader) Interrupt() {
	close(r.entered)
	<-r.release
	r.closed.Store(true)
}

type gatedUDPFlow struct {
	upEntered, upRelease     chan struct{}
	downEntered, downRelease chan struct{}
	upBytes, writes          atomic.Int64
	downBytes, reads         atomic.Int64
	finished                 atomic.Int64
	unanswered               atomic.Bool
	done                     chan struct{}
}

func (f *gatedUDPFlow) Uplink(bytes, packets int64) {
	if f.upEntered != nil {
		close(f.upEntered)
		<-f.upRelease
	}
	f.upBytes.Add(bytes)
	f.writes.Add(packets)
}

func (f *gatedUDPFlow) Downlink(bytes, packets int64) {
	if f.downEntered != nil {
		close(f.downEntered)
		<-f.downRelease
	}
	f.downBytes.Add(bytes)
	f.reads.Add(packets)
}

func (f *gatedUDPFlow) Finished() {
	f.unanswered.Store(f.writes.Load() > 0 && f.downBytes.Load() == 0)
	if f.finished.Add(1) == 1 {
		close(f.done)
	}
}

func TestUDPLinkReadAlreadyReturnedBeforeBothSidesClose(t *testing.T) {
	packet := udpTestBuffer("request", 53)
	flow := &gatedUDPFlow{upEntered: make(chan struct{}), upRelease: make(chan struct{}), done: make(chan struct{})}
	link := &transport.Link{Reader: &udpFixedReader{mb: buf.MultiBuffer{packet}}, Writer: &udpAcceptWriter{}}
	observeUDPLink(link, flow)
	read := make(chan buf.MultiBuffer, 1)
	go func() {
		mb, _ := link.Reader.ReadMultiBuffer()
		read <- mb
	}()
	<-flow.upEntered
	link.Writer.(interface{ Close() error }).Close()
	if flow.finished.Load() != 0 {
		t.Fatal("closing downlink ended an active uplink read")
	}
	link.Reader.(interface{ Interrupt() }).Interrupt()
	if flow.finished.Load() != 0 {
		t.Fatal("both terminals ended a flow before its uplink callback")
	}
	close(flow.upRelease)
	got := <-read
	<-flow.done
	if len(got) != 1 || got[0] != packet || flow.upBytes.Load() != 7 || flow.writes.Load() != 1 ||
		flow.finished.Load() != 1 || !flow.unanswered.Load() {
		t.Fatalf("late uplink was lost: bytes=%d writes=%d finishes=%d", flow.upBytes.Load(), flow.writes.Load(), flow.finished.Load())
	}
	buf.ReleaseMulti(got)
	link.Reader.(interface{ Interrupt() }).Interrupt()
	link.Writer.(interface{ Close() error }).Close()
	if flow.finished.Load() != 1 {
		t.Fatal("repeated terminal call finished twice")
	}
}

func TestUDPLinkDecodedReplyCountsBeforeBothSidesFinish(t *testing.T) {
	request := udpTestBuffer("request", 53)
	writer := &udpAcceptWriter{}
	flow := &gatedUDPFlow{downEntered: make(chan struct{}), downRelease: make(chan struct{}), done: make(chan struct{})}
	link := &transport.Link{Reader: &udpFixedReader{mb: buf.MultiBuffer{request}}, Writer: writer}
	observeUDPLink(link, flow)
	read, err := link.Reader.ReadMultiBuffer()
	if err != nil || flow.writes.Load() != 1 {
		t.Fatalf("request not counted: %v", err)
	}
	buf.ReleaseMulti(read)
	reply := udpTestBuffer("answer", 53)
	written := make(chan error, 1)
	go func() { written <- link.Writer.WriteMultiBuffer(buf.MultiBuffer{reply}) }()
	<-flow.downEntered
	link.Reader.(interface{ Interrupt() }).Interrupt()
	link.Writer.(interface{ Close() error }).Close()
	if flow.finished.Load() != 0 {
		t.Fatal("decoded reply was still in its callback")
	}
	close(flow.downRelease)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	<-flow.done
	if flow.downBytes.Load() != 6 || flow.reads.Load() != 1 || flow.finished.Load() != 1 ||
		flow.unanswered.Load() || !writer.wrote.Load() || reply.UDP.Port != 53 {
		t.Fatalf("reply lost or payload changed: bytes=%d reads=%d unanswered=%v", flow.downBytes.Load(), flow.reads.Load(), flow.unanswered.Load())
	}
	reply.Release()
}

func TestUDPLinkWriteDuringUnderlyingCloseCountsReply(t *testing.T) {
	request := udpTestBuffer("request", 53)
	writer := &udpGatedCloseWriter{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-writer.release:
		default:
			close(writer.release)
		}
	})
	flow := &gatedUDPFlow{done: make(chan struct{})}
	link := &transport.Link{Reader: &udpFixedReader{mb: buf.MultiBuffer{request}}, Writer: writer}
	observeUDPLink(link, flow)
	read, err := link.Reader.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	buf.ReleaseMulti(read)
	link.Reader.(interface{ Interrupt() }).Interrupt()
	closed := make(chan error, 1)
	go func() { closed <- link.Writer.(interface{ Close() error }).Close() }()
	<-writer.entered
	reply := udpTestBuffer("answer", 53)
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{reply}); err != nil {
		t.Fatal(err)
	}
	if flow.finished.Load() != 0 || flow.downBytes.Load() != 6 || flow.reads.Load() != 1 {
		t.Fatal("successful reply during lower Close was not counted before finish")
	}
	close(writer.release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	<-flow.done
	if flow.finished.Load() != 1 || flow.unanswered.Load() || flow.writes.Load() != 1 {
		t.Fatal("closed flow lost its successful reply")
	}
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{reply}); err != io.ErrClosedPipe {
		t.Fatalf("lower writer accepted a packet after Close: %v", err)
	}
	reply.Release()
}

func TestUDPLinkReadDuringUnderlyingInterruptCountsRequest(t *testing.T) {
	packet := udpTestBuffer("request", 53)
	reader := &udpGatedInterruptReader{mb: buf.MultiBuffer{packet}, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-reader.release:
		default:
			close(reader.release)
		}
	})
	flow := &gatedUDPFlow{done: make(chan struct{})}
	link := &transport.Link{Reader: reader, Writer: &udpAcceptWriter{}}
	observeUDPLink(link, flow)
	link.Writer.(interface{ Close() error }).Close()
	interrupted := make(chan struct{})
	go func() {
		link.Reader.(interface{ Interrupt() }).Interrupt()
		close(interrupted)
	}()
	<-reader.entered
	read, err := link.Reader.ReadMultiBuffer()
	if err != nil || len(read) != 1 || read[0] != packet {
		t.Fatalf("lower reader could not return its buffer: %v", err)
	}
	if flow.finished.Load() != 0 || flow.upBytes.Load() != 7 || flow.writes.Load() != 1 {
		t.Fatal("successful read during lower Interrupt was not counted")
	}
	close(reader.release)
	<-interrupted
	<-flow.done
	if flow.finished.Load() != 1 || !flow.unanswered.Load() {
		t.Fatal("read during Interrupt was omitted from final flow")
	}
	if _, err := link.Reader.ReadMultiBuffer(); err != io.EOF {
		t.Fatalf("lower reader returned data after Interrupt: %v", err)
	}
	buf.ReleaseMulti(read)
}

type udpBenchFlow struct {
	mu       sync.Mutex
	bytes    int64
	writes   atomic.Int64
	finished atomic.Bool
}

func (f *udpBenchFlow) Uplink(bytes, packets int64) {
	f.mu.Lock()
	f.bytes += bytes
	f.writes.Add(packets)
	f.mu.Unlock()
}
func (f *udpBenchFlow) Downlink(bytes, _ int64) {
	f.mu.Lock()
	f.bytes += bytes
	f.mu.Unlock()
}
func (f *udpBenchFlow) Finished() { f.finished.Store(true) }

func BenchmarkObservedUDPLinkUplink(b *testing.B) {
	packet := udpTestBuffer("benchmark payload", 53)
	flow := &udpBenchFlow{}
	link := &transport.Link{Reader: &udpFixedReader{mb: buf.MultiBuffer{packet}}, Writer: &udpAcceptWriter{}}
	observeUDPLink(link, flow)
	b.SetBytes(int64(packet.Len()))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = link.Reader.ReadMultiBuffer()
	}
	b.StopTimer()
	packet.Release()
}

func (f *udpFlowRecorder) Uplink(bytes, packets int64) {
	f.upBytes += bytes
	f.upPackets += packets
}

func (f *udpFlowRecorder) Downlink(bytes, packets int64) {
	f.downBytes += bytes
	f.downPackets += packets
}

func (f *udpFlowRecorder) Finished() { f.finished++ }

func udpTestBuffer(payload string, port net.Port) *buf.Buffer {
	b := buf.New()
	b.WriteString(payload)
	b.UDP = &net.Destination{Network: net.Network_UDP, Address: net.LocalHostIP, Port: port}
	return b
}

func TestObserveUDPLinkCountsBuffersWithoutChangingPayload(t *testing.T) {
	upReader, upWriter := pipe.New()
	downReader, downWriter := pipe.New()
	flow := &udpFlowRecorder{}
	link := &transport.Link{Reader: upReader, Writer: downWriter}
	observeUDPLink(link, flow)
	if _, ok := link.Reader.(buf.TimeoutReader); !ok {
		t.Fatal("pipe timeout reader was hidden")
	}
	if got := link.Reader.(interface{ UnderlyingReader() buf.Reader }).UnderlyingReader(); got != upReader {
		t.Fatal("mux cannot recognize the original pipe reader")
	}
	first := udpTestBuffer("dns", 53)
	second := udpTestBuffer("stun", 19302)
	if err := upWriter.WriteMultiBuffer(buf.MultiBuffer{first, second}); err != nil {
		t.Fatal(err)
	}
	gotUp, err := link.Reader.(buf.TimeoutReader).ReadMultiBufferTimeout(time.Second)
	if err != nil || len(gotUp) != 2 || gotUp[0] != first || gotUp[1] != second ||
		first.UDP.Port != 53 || second.UDP.Port != 19302 {
		t.Fatalf("uplink payload or packet boundaries changed: %v, %v", gotUp, err)
	}
	if flow.upBytes != 7 || flow.upPackets != 2 {
		t.Fatalf("uplink counts %d bytes, %d packets", flow.upBytes, flow.upPackets)
	}
	buf.ReleaseMulti(gotUp)

	reply := udpTestBuffer("reply", 53)
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{reply}); err != nil {
		t.Fatal(err)
	}
	gotDown, err := downReader.ReadMultiBuffer()
	if err != nil || len(gotDown) != 1 || gotDown[0] != reply || reply.UDP.Port != 53 {
		t.Fatalf("downlink payload changed: %v, %v", gotDown, err)
	}
	if flow.downBytes != 5 || flow.downPackets != 1 {
		t.Fatalf("downlink counts %d bytes, %d packets", flow.downBytes, flow.downPackets)
	}
	buf.ReleaseMulti(gotDown)
	link.Writer.(interface{ Close() error }).Close()
	if flow.finished != 0 {
		t.Fatal("one closed side ended the association")
	}
	link.Reader.(interface{ Interrupt() }).Interrupt()
	if flow.finished != 1 {
		t.Fatalf("finish calls %d", flow.finished)
	}
}
