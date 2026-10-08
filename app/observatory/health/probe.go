package health

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
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
	ctx, cancel := context.WithTimeout(withProbe(o.ctx, &bytes), ps.timeout)
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
	fail := func(err string) probeResult {
		return probeResult{err: err, bytes: bytes.Load()}
	}

	req, err := http.NewRequestWithContext(ctx, ps.method, ps.destination, nil)
	if err != nil {
		return fail(err.Error())
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
	reason := func(err error) string {
		stallMu.Lock()
		defer stallMu.Unlock()
		if stalled {
			return "no progress"
		}
		if ctx.Err() == context.DeadlineExceeded {
			return "timeout"
		}
		return err.Error()
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return fail(reason(err))
	}
	defer resp.Body.Close()
	rtt := time.Since(start)
	if !ps.statusOK(resp.StatusCode) {
		return fail("status " + strconv.Itoa(resp.StatusCode))
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
			return fail(reason(err))
		}
	}
	if read < ps.minBytes {
		return fail("short body " + strconv.FormatInt(read, 10))
	}
	return probeResult{ok: true, rtt: rtt, bytes: bytes.Load()}
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
	if v, ok := o.lastServer.Load(tag); ok {
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
