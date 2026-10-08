package health

import (
	"context"
	gotls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/tagged"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// probe makes one request through the outbound and reads the body. It fails
// on a wrong status, a short body, or no progress for progressTimeout.
func (o *Observer) probe(tag string, ps *probeSettings) probeResult {
	var bytes atomic.Int64
	ctx, cancel := context.WithTimeout(withProbe(o.runCtx, &bytes), ps.timeout)
	defer cancel()

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
	fail := func(err, class string) probeResult {
		if o.runCtx.Err() != nil {
			return probeResult{canceled: true, err: "canceled", bytes: bytes.Load()}
		}
		return probeResult{err: err, class: class, bytes: bytes.Load()}
	}

	req, err := http.NewRequestWithContext(ctx, ps.method, ps.destination, nil)
	if err != nil {
		return fail(err.Error(), "other")
	}
	utils.TryDefaultHeadersWith(req.Header, "nav")

	stalled := false
	var stallMu sync.Mutex
	watchdog := time.AfterFunc(ps.progressTimeout, func() {
		stallMu.Lock()
		stalled = true
		stallMu.Unlock()
		cancel()
	})
	defer watchdog.Stop()
	failed := func(err error) probeResult {
		stallMu.Lock()
		defer stallMu.Unlock()
		if stalled {
			return fail("no progress", "no_progress")
		}
		if ctx.Err() == context.DeadlineExceeded {
			return fail("timeout", "timeout")
		}
		return fail(err.Error(), errorClass(err))
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return failed(err)
	}
	defer resp.Body.Close()
	rtt := time.Since(start)
	if !ps.statusOK(resp.StatusCode) {
		return fail("status "+strconv.Itoa(resp.StatusCode), "status_"+strconv.Itoa(resp.StatusCode))
	}
	watchdog.Reset(ps.progressTimeout)
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
			watchdog.Reset(ps.progressTimeout)
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
	return probeResult{ok: true, rtt: rtt, bytes: bytes.Load()}
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
