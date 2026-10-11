package outbound

import (
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
)

// observeUDPLink follows one routed association before mux or the proxy sees it.
func observeUDPLink(link *transport.Link, flow outbound.UDPFlowTracker) {
	lifecycle := &udpLinkLifecycle{flow: flow}
	reader := &udpFlowReader{reader: link.Reader, lifecycle: lifecycle}
	if timeoutReader, ok := link.Reader.(buf.TimeoutReader); ok {
		link.Reader = &udpFlowTimeoutReader{udpFlowReader: reader, timeoutReader: timeoutReader}
	} else {
		link.Reader = reader
	}
	link.Writer = &udpFlowWriter{writer: link.Writer, lifecycle: lifecycle}
}

type udpLinkLifecycle struct {
	mu       sync.Mutex
	flow     outbound.UDPFlowTracker
	active   int
	uplink   bool
	downlink bool
	finished bool
}

func (l *udpLinkLifecycle) begin(uplink bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finished || uplink && l.uplink || !uplink && l.downlink {
		return false
	}
	l.active++
	return true
}

func (l *udpLinkLifecycle) end() {
	l.mu.Lock()
	l.active--
	finish := l.readyToFinish()
	l.mu.Unlock()
	if finish {
		l.flow.Finished()
	}
}

func (l *udpLinkLifecycle) terminal(uplink bool) {
	l.mu.Lock()
	if uplink {
		l.uplink = true
	} else {
		l.downlink = true
	}
	finish := l.readyToFinish()
	l.mu.Unlock()
	if finish {
		l.flow.Finished()
	}
}

func (l *udpLinkLifecycle) readyToFinish() bool {
	if !l.finished && l.uplink && l.downlink && l.active == 0 {
		l.finished = true
		return true
	}
	return false
}

func udpBufferCounts(mb buf.MultiBuffer) (bytes, packets int64) {
	for _, b := range mb {
		if b != nil && !b.IsEmpty() {
			bytes += int64(b.Len())
			packets++
		}
	}
	return bytes, packets
}

type udpFlowReader struct {
	reader    buf.Reader
	lifecycle *udpLinkLifecycle
}

func (r *udpFlowReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	active := r.lifecycle.begin(true)
	mb, err := r.reader.ReadMultiBuffer()
	if active {
		r.count(mb)
		r.lifecycle.end()
	}
	return mb, err
}

func (r *udpFlowReader) count(mb buf.MultiBuffer) {
	bytes, packets := udpBufferCounts(mb)
	if packets != 0 {
		r.lifecycle.flow.Uplink(bytes, packets)
	}
}

// UnderlyingReader lets mux preserve its existing direct pipe-reader path.
func (r *udpFlowReader) UnderlyingReader() buf.Reader { return r.reader }

func (r *udpFlowReader) Close() error {
	defer r.lifecycle.terminal(true)
	return common.Close(r.reader)
}

func (r *udpFlowReader) Interrupt() {
	defer r.lifecycle.terminal(true)
	common.Interrupt(r.reader)
}

type udpFlowTimeoutReader struct {
	*udpFlowReader
	timeoutReader buf.TimeoutReader
}

func (r *udpFlowTimeoutReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	active := r.lifecycle.begin(true)
	mb, err := r.timeoutReader.ReadMultiBufferTimeout(timeout)
	if active {
		r.count(mb)
		r.lifecycle.end()
	}
	return mb, err
}

type udpFlowWriter struct {
	writer    buf.Writer
	lifecycle *udpLinkLifecycle
}

func (w *udpFlowWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if w.lifecycle.begin(false) {
		bytes, packets := udpBufferCounts(mb)
		if packets != 0 {
			w.lifecycle.flow.Downlink(bytes, packets)
		}
		w.lifecycle.end()
	}
	return w.writer.WriteMultiBuffer(mb)
}

func (w *udpFlowWriter) Close() error {
	defer w.lifecycle.terminal(false)
	return common.Close(w.writer)
}

func (w *udpFlowWriter) Interrupt() {
	defer w.lifecycle.terminal(false)
	common.Interrupt(w.writer)
}
