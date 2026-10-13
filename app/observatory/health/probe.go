package health

import (
	"context"
	gotls "crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/xtls/xray-core/common/safe"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/tagged"
	"github.com/xtls/xray-core/transport/internet/tls"
)

type probeState struct {
	bytes      *atomic.Int64
	credential *probeCredential
	udpDial    func(error)

	mu              sync.Mutex
	started         time.Time
	cancel          context.CancelFunc
	dialTimer       *time.Timer
	progressTimer   *time.Timer
	requestTimer    *time.Timer
	absoluteTimer   *time.Timer
	dialTimeout     time.Duration
	requestTimeout  time.Duration
	progressTimeout time.Duration
	dialElapsed     time.Duration
	dialErr         error
	dialDone        bool
	reason          string
	finished        bool
}

func (s *probeState) begin(started time.Time, cancel context.CancelFunc, dialTimeout, requestTimeout, progressTimeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = started
	s.cancel = cancel
	s.dialTimeout = dialTimeout
	s.requestTimeout = requestTimeout
	s.progressTimeout = progressTimeout
	if dialTimeout < maxProbeTimeout {
		s.dialTimer = time.AfterFunc(max(0, dialTimeout-time.Since(started)), func() {
			defer safe.Recover(safe.Connection, "app/observatory/health/probe.go:probeState.expire", nil)
			s.expire("dial_timeout")
		})
	}
	s.absoluteTimer = time.AfterFunc(max(0, maxProbeTimeout-time.Since(started)), func() {
		defer safe.Recover(safe.Connection, "app/observatory/health/probe.go:probeState.expire", nil)
		s.expire("timeout")
	})
}

func (s *probeState) expire(reason string) {
	s.mu.Lock()
	if s.finished || s.reason != "" {
		s.mu.Unlock()
		return
	}
	s.reason = reason
	cancel := s.cancel
	s.mu.Unlock()
	cancel()
}

func (s *probeState) recordDial(err error) {
	if s.udpDial != nil {
		s.udpDial(err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started.IsZero() || s.finished || (s.dialDone && s.dialErr == nil) {
		return
	}
	s.dialDone = true
	s.dialElapsed = time.Since(s.started)
	s.dialErr = err
	s.checkDialDeadlineLocked(s.dialElapsed)
	if err != nil {
		return
	}
	if s.dialTimer != nil {
		s.dialTimer.Stop()
	}
	if s.reason != "" {
		return
	}
	s.startRequestTimersLocked(s.requestTimeout)
}

func (s *probeState) checkDialDeadlineLocked(elapsed time.Duration) {
	if s.reason != "" {
		return
	}
	if elapsed >= maxProbeTimeout {
		s.reason = "timeout"
		s.cancel()
	} else if elapsed >= s.dialTimeout {
		s.reason = "dial_timeout"
		s.cancel()
	}
}

func (s *probeState) startRequestTimersLocked(requestRemaining time.Duration) {
	progress := max(s.progressTimeout, 3*s.dialElapsed)
	s.progressTimer = time.AfterFunc(progress, func() {
		defer safe.Recover(safe.Connection, "app/observatory/health/probe.go:probeState.expire", nil)
		s.expire("no_progress")
	})
	s.requestTimer = time.AfterFunc(requestRemaining, func() {
		defer safe.Recover(safe.Connection, "app/observatory/health/probe.go:probeState.expire", nil)
		s.expire("timeout")
	})
}

func probeDialFailureKind(err error) failureKind {
	if errorClass(err) == "timeout" {
		return failDialTimeout
	}
	return classifyDialError(err)
}

func (s *probeState) response() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if (!s.dialDone || s.dialErr != nil) && !s.finished && s.reason == "" {
		elapsed := time.Since(s.started)
		s.dialDone = true
		s.dialElapsed = 0
		s.dialErr = nil
		if elapsed >= s.requestTimeout {
			s.reason = "timeout"
			s.cancel()
			return
		}
		s.checkDialDeadlineLocked(elapsed)
		if s.reason != "" {
			return
		}
		if s.dialTimer != nil {
			s.dialTimer.Stop()
		}
		s.startRequestTimersLocked(s.requestTimeout - elapsed)
	}
	s.resetProgress()
}

func (s *probeState) progress() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetProgress()
}

func (s *probeState) resetProgress() {
	if !s.finished && s.reason == "" && s.progressTimer != nil {
		s.progressTimer.Reset(max(s.progressTimeout, 3*s.dialElapsed))
	}
}

func (s *probeState) result() (reason string, dialDone bool, dialErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reason, s.dialDone, s.dialErr
}

func (s *probeState) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished = true
	if s.dialTimer != nil {
		s.dialTimer.Stop()
	}
	if s.progressTimer != nil {
		s.progressTimer.Stop()
	}
	if s.requestTimer != nil {
		s.requestTimer.Stop()
	}
	if s.absoluteTimer != nil {
		s.absoluteTimer.Stop()
	}
}

func (o *Observer) probe(tag string, ps *probeSettings) probeResult {
	return o.probeWithContext(o.runCtx, tag, ps)
}

func (o *Observer) probeWithContext(ctx context.Context, tag string, ps *probeSettings) probeResult {
	steps := ps.steps
	if len(steps) == 0 {
		name := ps.name
		if name == "" {
			name = "default"
		}
		steps = []probeStepSettings{{
			name: name, destinations: []string{ps.destination}, method: ps.method,
			expectStatus: ps.expectStatus, minBytes: ps.minBytes,
			timeout: ps.timeout, progressTimeout: ps.progressTimeout,
		}}
	}
	result := probeResult{profile: ps.name, state: statusUp}
	for i := range steps {
		step, attempt := o.probeStepWithContext(ctx, tag, &steps[i])
		result.bytes += step.bytes
		if ctx.Err() != nil {
			result.canceled = true
			result.err = "canceled"
			return result
		}
		result.steps = append(result.steps, step)
		if !step.ok {
			if !steps[i].isOptional {
				result.state = statusDown
				result.err = attempt.err
				result.class = step.class
				return result
			}
			result.state = statusDegraded
			continue
		}
		if result.rtt == 0 {
			result.rtt = attempt.rtt
		}
	}
	result.ok = true
	return result
}

type probeAttemptResult struct {
	ok      bool
	rtt     time.Duration
	connect time.Duration
	bytes   int64
	err     string
	class   string
}

func (o *Observer) probeStep(tag string, step *probeStepSettings) (probeStepResult, probeAttemptResult) {
	return o.probeStepWithContext(o.runCtx, tag, step)
}

func (o *Observer) probeStepWithContext(parent context.Context, tag string, step *probeStepSettings) (probeStepResult, probeAttemptResult) {
	started := time.Now()
	result := probeStepResult{name: step.name, destination: -1}
	results := make(chan struct {
		index int
		probeAttemptResult
	}, len(step.destinations))
	cancels := make([]context.CancelFunc, len(step.destinations))
	for i, destination := range step.destinations {
		ctx, cancel := context.WithCancel(parent)
		cancels[i] = cancel
		go func(index int, destination string) {
			defer safe.Recover(safe.Subsystem, "app/observatory/health/probe.go:Observer.probeDestination", func() {
				o.fail()
				results <- struct {
					index int
					probeAttemptResult
				}{index, probeAttemptResult{err: "worker panic recovered", class: "other"}}
			})
			r := o.probeDestination(ctx, tag, step, destination)
			results <- struct {
				index int
				probeAttemptResult
			}{index, r}
		}(i, destination)
	}
	failures := make([]probeAttemptResult, len(step.destinations))
	for range step.destinations {
		attempt := <-results
		result.bytes += attempt.bytes
		failures[attempt.index] = attempt.probeAttemptResult
		if attempt.ok && !result.ok && parent.Err() == nil {
			result.ok = true
			result.destination = attempt.index
			result.connect = attempt.connect
			for i, cancel := range cancels {
				if i != attempt.index {
					cancel()
				}
			}
		}
	}
	for _, cancel := range cancels {
		cancel()
	}
	result.elapsed = time.Since(started)
	if result.ok {
		return result, failures[result.destination]
	}
	if len(failures) > 0 {
		result.connect = failures[0].connect
		result.class = failures[0].class
		return result, failures[0]
	}
	return result, probeAttemptResult{err: "no destinations", class: "other"}
}

// probeDestination makes one request through the outbound and reads the body.
// Its context and counters belong only to this address, including when another wins.
func (o *Observer) probeDestination(parent context.Context, tag string, ps *probeStepSettings, destination string) probeAttemptResult {
	var bytes atomic.Int64
	probeCtx := withProbe(parent, &bytes)
	probeCtx = session.ContextWithContent(probeCtx, &session.Content{SkipMuxReuse: true})
	state := probeStateFrom(probeCtx)
	if !ps.isOptional {
		state.credential, _ = parent.Value(probeCredentialKey{}).(*probeCredential)
	}
	ctx, cancel := context.WithCancel(probeCtx)
	defer cancel()
	defer state.stop()

	// Keep-alive stays on: a server told to close the connection after the
	// answer races its last bytes on some transports, such as WebSocket. The
	// connection is closed here once the answer is read.
	transport := &http.Transport{
		DisableCompression: true,
		DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
			dest, err := net.ParseDestination(network + ":" + addr)
			if err != nil {
				return nil, err
			}
			return tagged.Dialer(ctx, o.dispatcher, dest, tag)
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	fail := func(err, class string) probeAttemptResult {
		if o.runCtx.Err() != nil {
			return probeAttemptResult{err: "canceled", bytes: bytes.Load()}
		}
		state.mu.Lock()
		connect := time.Duration(0)
		if state.dialDone {
			connect = state.dialElapsed
		}
		state.mu.Unlock()
		return probeAttemptResult{err: err, class: class, connect: connect, bytes: bytes.Load()}
	}

	req, err := http.NewRequestWithContext(ctx, ps.method, destination, nil)
	if err != nil {
		return fail(err.Error(), "other")
	}
	utils.TryDefaultHeadersWith(req.Header, "nav")

	failed := func(err error) probeAttemptResult {
		reason, dialDone, dialErr := state.result()
		switch reason {
		case "dial_timeout":
			return fail("dial timeout", "dial_timeout")
		case "no_progress":
			return fail("no progress", "no_progress")
		case "timeout":
			return fail("timeout", "timeout")
		}
		if dialDone && dialErr != nil {
			return fail(dialErr.Error(), probeDialFailureKind(dialErr).String())
		}
		if errorClass(err) == "timeout" {
			return fail(err.Error(), "timeout")
		}
		return fail(err.Error(), "closed")
	}

	start := time.Now()
	state.begin(start, cancel, o.s.passive.dialTimeout, ps.timeout, ps.progressTimeout)
	resp, err := client.Do(req)
	if err != nil {
		return failed(err)
	}
	defer resp.Body.Close()
	rtt := time.Since(start)
	state.response()
	if reason, _, _ := state.result(); reason != "" {
		return failed(context.Canceled)
	}
	if !ps.statusOK(resp.StatusCode) {
		return fail("status "+strconv.Itoa(resp.StatusCode), "status_"+strconv.Itoa(resp.StatusCode))
	}
	limit := ps.minBytes
	if limit <= 0 {
		limit = probeReadLimit
	}
	buf := make([]byte, 16*1024)
	var read int64
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			read += int64(n)
			state.progress()
		}
		if read >= limit {
			break
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return failed(err)
		}
	}
	if read < ps.minBytes {
		return fail("short body "+strconv.FormatInt(read, 10), "short_body")
	}
	if reason, _, _ := state.result(); reason != "" || o.runCtx.Err() != nil {
		return failed(context.Canceled)
	}
	state.mu.Lock()
	connect := time.Duration(0)
	if state.dialDone && state.dialErr == nil {
		connect = state.dialElapsed
	}
	state.mu.Unlock()
	return probeAttemptResult{ok: true, rtt: rtt, connect: connect, bytes: bytes.Load()}
}

// errorClass names the error of a probe by its type: the class goes to a
// provider, and the text of the error may carry the URL of the probe.
// Strings are never parsed; what the types do not tell is other.
func errorClass(err error) string {
	var timeout interface{ Timeout() bool }
	var alert gotls.AlertError
	var record gotls.RecordHeaderError
	var verify *gotls.CertificateVerificationError
	var authority x509.UnknownAuthorityError
	var host x509.HostnameError
	var invalid x509.CertificateInvalidError
	switch {
	case errors.Is(err, syscall.ECONNRESET):
		return "reset"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	case errors.As(err, &alert), errors.As(err, &record), errors.As(err, &verify),
		errors.As(err, &authority), errors.As(err, &host), errors.As(err, &invalid):
		return "tls"
	case errors.As(err, &timeout) && timeout.Timeout():
		return "timeout"
	}
	return "other"
}

// serverName groups outbounds that look like one server to a censor: the TLS
// or REALITY server name, or the server address without one.
func (o *Observer) serverName(tag string) string {
	if o.ohm == nil {
		return tag
	}
	h := o.ohm.GetHandler(tag)
	if h == nil {
		return tag
	}
	if ss := h.SenderSettings(); ss != nil {
		if inst, err := ss.GetInstance(); err == nil {
			if sc, ok := inst.(*proxyman.SenderConfig); ok && sc.StreamSettings != nil {
				if name := securityServerName(sc.StreamSettings); name != "" {
					return name
				}
			}
		}
	}
	if v, ok := o.e.lastServer.Load(tag); ok {
		return v.(string)
	}
	return tag
}

func securityServerName(sc *internet.StreamConfig) string {
	for _, m := range sc.SecuritySettings {
		inst, err := m.GetInstance()
		if err != nil {
			continue
		}
		switch c := inst.(type) {
		case *tls.Config:
			if c.ServerName != "" {
				return c.ServerName
			}
		case *reality.Config:
			if c.ServerName != "" {
				return c.ServerName
			}
		}
	}
	return ""
}
