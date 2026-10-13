package health

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

type udpSettings struct {
	destinations      []net.Destination
	requests          int
	interval, timeout time.Duration
	perHour           int
}

type udpProbeResult struct {
	state            string
	answered, sent   int
	rtt, jitter      time.Duration
	destinationIndex int
	class            string
	canceled         bool
}

const stunCookie = 0x2112a442

type stunTransaction struct {
	id       [12]byte
	sent     time.Time
	rtt      time.Duration
	answered bool
}

func stunSuccess(packet []byte) ([12]byte, bool) {
	var id [12]byte
	if len(packet) < 20 || binary.BigEndian.Uint16(packet[:2]) != 0x0101 || binary.BigEndian.Uint32(packet[4:8]) != stunCookie {
		return id, false
	}
	size := int(binary.BigEndian.Uint16(packet[2:4]))
	if size%4 != 0 || size+20 != len(packet) {
		return id, false
	}
	for offset := 20; offset < len(packet); {
		if len(packet)-offset < 4 {
			return id, false
		}
		length := int(binary.BigEndian.Uint16(packet[offset+2 : offset+4]))
		offset += 4 + ((length + 3) &^ 3)
		if offset > len(packet) {
			return id, false
		}
	}
	copy(id[:], packet[8:20])
	return id, true
}

func udpMeasured(transactions []stunTransaction) udpProbeResult {
	r := udpProbeResult{state: "dead"}
	var samples []time.Duration
	for _, t := range transactions {
		if !t.sent.IsZero() {
			r.sent++
		}
		if t.answered {
			samples = append(samples, t.rtt)
		}
	}
	r.answered = len(samples)
	if r.answered == 0 {
		return r
	}
	r.state = "lossy"
	if r.answered >= 3 {
		r.state = "ok"
	}
	for i := 1; i < len(samples); i++ {
		delta := samples[i] - samples[i-1]
		if delta < 0 {
			delta = -delta
		}
		r.jitter += delta
	}
	if len(samples) > 1 {
		r.jitter /= time.Duration(len(samples) - 1)
	}
	slices.Sort(samples)
	middle := len(samples) / 2
	r.rtt = samples[middle]
	if len(samples)%2 == 0 {
		r.rtt = samples[middle-1] + (samples[middle]-samples[middle-1])/2
	}
	return r
}

func (o *Observer) probeUDP(tag string, settings *udpSettings) udpProbeResult {
	return o.probeUDPWithContext(o.runCtx, tag, settings)
}

func (o *Observer) probeUDPWithContext(parent context.Context, tag string, settings *udpSettings) udpProbeResult {
	if settings == nil || len(settings.destinations) == 0 {
		return udpProbeResult{state: "unsupported", class: "disabled"}
	}
	for i, destination := range settings.destinations {
		r := o.probeUDPDestination(parent, tag, destination, settings)
		r.destinationIndex = i
		if r.canceled || r.answered > 0 || i == len(settings.destinations)-1 {
			return r
		}
	}
	return udpProbeResult{state: "dead"}
}

func (o *Observer) probeUDPDestination(parent context.Context, tag string, destination net.Destination, settings *udpSettings) udpProbeResult {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var bytes atomic.Int64
	ctx = withProbe(ctx, &bytes)
	ctx = session.ContextWithContent(ctx, &session.Content{SkipDNSResolve: true, SkipMuxReuse: true})
	state := probeStateFrom(ctx)
	dialErrors := make(chan error, 1)
	dialTimedOut := atomic.Bool{}
	dialTimer := time.AfterFunc(o.s.passive.dialTimeout, func() { dialTimedOut.Store(true); cancel() })
	defer dialTimer.Stop()
	state.udpDial = func(err error) {
		dialTimer.Stop()
		if err != nil {
			select {
			case dialErrors <- err:
			default:
			}
			cancel()
		}
	}
	defer state.stop()
	// Dispatch may return before the outbound has completed its dial.
	conn, err := tagged.Dialer(ctx, o.dispatcher, destination, tag)
	if err != nil {
		if parent.Err() != nil {
			return udpProbeResult{state: "dead", canceled: true}
		}
		return udpProbeResult{state: "unsupported", class: probeDialFailureKind(err).String()}
	}
	if conn == nil {
		return udpProbeResult{state: "unsupported", class: "closed"}
	}
	r := runSTUN(ctx, conn, settings)
	r.canceled = parent.Err() != nil
	if dialTimedOut.Load() && !r.canceled {
		r.state = "unsupported"
		r.class = "dial_timeout"
	}
	select {
	case err := <-dialErrors:
		r.state = "unsupported"
		r.class = probeDialFailureKind(err).String()
		r.canceled = parent.Err() != nil
	default:
	}
	return r
}

func runSTUN(parent context.Context, conn net.Conn, settings *udpSettings) udpProbeResult {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var sendTimedOut atomic.Bool
	var mu sync.Mutex
	transactions := make([]stunTransaction, settings.requests)
	readDone := make(chan struct{})
	closeDone := make(chan struct{})
	stopClose := make(chan struct{})
	go func() {
		defer close(closeDone)
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopClose:
		}
	}()
	go func() {
		defer close(readDone)
		packet := make([]byte, 65535)
		for {
			n, err := conn.Read(packet)
			received := time.Now()
			if n > 0 {
				if id, valid := stunSuccess(packet[:n]); valid {
					mu.Lock()
					for i := range transactions {
						t := &transactions[i]
						if t.id == id && !t.sent.IsZero() && !t.answered {
							t.answered = true
							t.rtt = max(received.Sub(t.sent), 0)
							break
						}
					}
					mu.Unlock()
				}
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { close(stopClose); _ = conn.Close(); <-closeDone; <-readDone }()
	var writeErr error
	for i := range transactions {
		if i > 0 {
			timer := time.NewTimer(settings.interval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			case <-readDone:
				timer.Stop()
			}
		}
		if ctx.Err() != nil {
			break
		}
		select {
		case <-readDone:
			writeErr = io.ErrClosedPipe
		default:
		}
		if writeErr != nil {
			break
		}
		var packet [20]byte
		binary.BigEndian.PutUint16(packet[:2], 0x0001)
		binary.BigEndian.PutUint32(packet[4:8], stunCookie)
		for {
			if _, writeErr = rand.Read(packet[8:]); writeErr != nil {
				break
			}
			mu.Lock()
			unique := true
			for j := 0; j < i; j++ {
				if string(transactions[j].id[:]) == string(packet[8:]) {
					unique = false
					break
				}
			}
			if unique {
				copy(transactions[i].id[:], packet[8:])
				transactions[i].sent = time.Now()
			}
			mu.Unlock()
			if unique {
				break
			}
		}
		if writeErr != nil {
			break
		}
		// Each write is bounded even when the connection ignores deadlines.
		sendTimerDone := make(chan struct{})
		sendTimer := time.AfterFunc(settings.timeout, func() {
			sendTimedOut.Store(true)
			cancel()
			close(sendTimerDone)
		})
		n, err := conn.Write(packet[:])
		if !sendTimer.Stop() {
			<-sendTimerDone
		}
		if err != nil || n != len(packet) {
			mu.Lock()
			transactions[i].sent = time.Time{}
			transactions[i].answered = false
			mu.Unlock()
			writeErr = err
			if writeErr == nil {
				writeErr = io.ErrShortWrite
			}
			break
		}
	}
	if writeErr == nil && ctx.Err() == nil {
		// cnc connections ignore deadlines: close and join the reader explicitly.
		timer := time.NewTimer(settings.timeout)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		case <-readDone:
			timer.Stop()
		}
	}
	_ = conn.Close()
	<-readDone
	mu.Lock()
	r := udpMeasured(transactions)
	mu.Unlock()
	r.canceled = parent.Err() != nil
	if sendTimedOut.Load() {
		r.class = "send_timeout"
	} else if writeErr != nil && r.answered == 0 {
		r.state = "unsupported"
		r.class = errorClass(writeErr)
	}
	return r
}
