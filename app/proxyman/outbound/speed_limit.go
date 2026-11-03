package outbound

import (
	"context"
	"math"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"golang.org/x/time/rate"
)

// A limited outbound may send a quarter second of traffic at once, so that a
// short burst after an idle period does not wait buffer by buffer.
const (
	speedLimitBurstShare = 4
	speedLimitMinBurst   = 64 * 1024
)

// speedLimits holds the byte budgets of one outbound. Every connection of the
// outbound, with or without mux, draws from the same budget.
type speedLimits struct {
	uplink, downlink *rate.Limiter
}

func newSpeedLimits(c *proxyman.SpeedLimit) *speedLimits {
	if c == nil || c.UplinkBytesPerSecond == 0 && c.DownlinkBytesPerSecond == 0 {
		return nil
	}
	return &speedLimits{uplink: newSpeedLimiter(c.UplinkBytesPerSecond), downlink: newSpeedLimiter(c.DownlinkBytesPerSecond)}
}

func newSpeedLimiter(bytesPerSecond uint64) *rate.Limiter {
	if bytesPerSecond == 0 {
		return nil
	}
	burst := min(max(bytesPerSecond/speedLimitBurstShare, speedLimitMinBurst), math.MaxInt32)
	return rate.NewLimiter(rate.Limit(bytesPerSecond), int(burst))
}

// limit wraps the link of one routed connection. Splice would copy past the
// link in the kernel, so a limited connection never splices.
func (l *speedLimits) limit(ctx context.Context, link *transport.Link) {
	if l == nil {
		return
	}
	if inbound := session.InboundFromContext(ctx); inbound != nil {
		inbound.CanSpliceCopy = 3
	}
	if l.uplink != nil {
		reader := &speedLimitReader{reader: link.Reader, wait: newSpeedWait(ctx, l.uplink)}
		if timeoutReader, ok := link.Reader.(buf.TimeoutReader); ok {
			link.Reader = &speedLimitTimeoutReader{speedLimitReader: reader, timeoutReader: timeoutReader}
		} else {
			link.Reader = reader
		}
	}
	if l.downlink != nil {
		link.Writer = &speedLimitWriter{writer: link.Writer, wait: newSpeedWait(ctx, l.downlink)}
	}
}

type speedWait struct {
	ctx     context.Context
	cancel  context.CancelFunc
	limiter *rate.Limiter
}

// Each direction ends its own waits: a half-closed connection keeps the other.
func newSpeedWait(ctx context.Context, limiter *rate.Limiter) speedWait {
	ctx, cancel := context.WithCancel(ctx)
	return speedWait{ctx: ctx, cancel: cancel, limiter: limiter}
}

// take blocks until the budget covers n bytes. Closing or interrupting the
// side ends the wait, so a gone connection does not hold its place in line.
func (w speedWait) take(n int32) error {
	for n > 0 {
		k := min(int(n), w.limiter.Burst())
		if err := w.limiter.WaitN(w.ctx, k); err != nil {
			return err
		}
		n -= int32(k)
	}
	return nil
}

type speedLimitReader struct {
	reader buf.Reader
	wait   speedWait
}

func (r *speedLimitReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	return r.charge(r.reader.ReadMultiBuffer())
}

func (r *speedLimitReader) charge(mb buf.MultiBuffer, err error) (buf.MultiBuffer, error) {
	if werr := r.wait.take(mb.Len()); werr != nil {
		buf.ReleaseMulti(mb)
		return nil, werr
	}
	return mb, err
}

// UnderlyingReader lets mux preserve its existing direct pipe-reader path.
func (r *speedLimitReader) UnderlyingReader() buf.Reader {
	if wrapped, ok := r.reader.(interface{ UnderlyingReader() buf.Reader }); ok {
		return wrapped.UnderlyingReader()
	}
	return r.reader
}

func (r *speedLimitReader) Close() error {
	r.wait.cancel()
	return common.Close(r.reader)
}

func (r *speedLimitReader) Interrupt() {
	r.wait.cancel()
	common.Interrupt(r.reader)
}

type speedLimitTimeoutReader struct {
	*speedLimitReader
	timeoutReader buf.TimeoutReader
}

func (r *speedLimitTimeoutReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	return r.charge(r.timeoutReader.ReadMultiBufferTimeout(timeout))
}

type speedLimitWriter struct {
	writer buf.Writer
	wait   speedWait
}

func (w *speedLimitWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if err := w.wait.take(mb.Len()); err != nil {
		buf.ReleaseMulti(mb)
		return err
	}
	return w.writer.WriteMultiBuffer(mb)
}

func (w *speedLimitWriter) Close() error {
	w.wait.cancel()
	return common.Close(w.writer)
}

func (w *speedLimitWriter) Interrupt() {
	w.wait.cancel()
	common.Interrupt(w.writer)
}
