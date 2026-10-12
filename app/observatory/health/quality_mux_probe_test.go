package health_test

// Integrated fixture: API checked against accepted Stage 1 cf3b4a8d and
// the current quality spec. Root owns compilation and runtime checks.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	gonet "net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf/serial"
	_ "github.com/xtls/xray-core/main/distro/all"
)

const qualityProbeMuxBodyBytes = 32 * 1024

// The cold case catches the physical worker losing the probe context. The
// warm case also proves isolation at the TCP boundary: quality alone cannot
// reveal a small probe mixed into an open user's connection below 1 MiB.
func TestQualityIntegrationMuxProbeExcluded(t *testing.T) {
	t.Run("cold_probe_has_no_quality", func(t *testing.T) {
		f := startQualityProbeMuxFixture(t)
		assertQualityProbeMuxEmpty(t, readQualityProbeMuxSnapshot(t, f.observer))
		ok, wire, msg := health.Probe(f.observer, "v1", f.http.URL+"/probe", qualityProbeMuxBodyBytes, http.StatusOK)
		// Check this before success/wire assertions: the old worker can answer
		// HTTP while its detached context records the probe as user quality.
		assertQualityProbeMuxEmpty(t, readQualityProbeMuxSnapshot(t, f.observer))
		f.assertProbeResult(t, ok, wire, msg)
		probe := f.relay.next(t)
		probe.waitDone(t)
		probe.assertTransferred(t)
		assertQualityProbeMuxEmpty(t, readQualityProbeMuxSnapshot(t, f.observer))
		f.relay.assertNoExtra(t)
	})

	t.Run("warm_user_quality_survives_isolated_probe", func(t *testing.T) {
		f := startQualityProbeMuxFixture(t)
		warm := f.openWarmUser(t)
		defer warm.conn.Close()
		user := f.relay.next(t)
		before := waitQualityProbeMuxInitial(t, f.observer)
		warm.assertLive(t)
		user.assertOpen(t)
		ok, wire, msg := health.Probe(f.observer, "v1", f.http.URL+"/probe", qualityProbeMuxBodyBytes, http.StatusOK)
		f.assertProbeResult(t, ok, wire, msg)
		// A second accept proves the probe did not reuse the still-open user
		// physical worker, even when the small response crosses no bulk mark.
		probe := f.relay.next(t)
		if probe == user {
			t.Fatal("probe reused the user's physical TCP connection")
		}
		probe.waitDone(t)
		probe.assertTransferred(t)
		warm.assertLive(t)
		f.warmRoundtrip(t, warm, "/warm-after-probe")
		user.assertOpen(t)
		after := readQualityProbeMuxSnapshot(t, f.observer)
		if got, want := after.canonical(t), before.canonical(t); got != want {
			t.Fatalf("isolated probe changed user quality: before %s, after %s", want, got)
		}
		f.relay.assertNoExtra(t)
		// Keep the user channel and its context alive through both snapshots.
		// This assertion concerns its initial quality; it does not claim to
		// account for final physical bytes after the user worker eventually ends.
	})
}

type qualityProbeMuxRequest struct {
	path, method, userAgent string
	written                 int
	err                     error
}

type qualityProbeMuxFixture struct {
	client   *core.Instance
	observer *health.Observer
	http     *httptest.Server
	relay    *qualityProbeMuxRelay
	requests chan qualityProbeMuxRequest
	body     string
}

func startQualityProbeMuxFixture(t *testing.T) *qualityProbeMuxFixture {
	t.Helper()
	f := &qualityProbeMuxFixture{
		requests: make(chan qualityProbeMuxRequest, 8),
		body:     strings.Repeat("q", qualityProbeMuxBodyBytes),
	}
	f.http = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", strconv.Itoa(len(f.body)))
		w.Header().Set("X-Quality-Mux-Fixture", r.URL.Path)
		if r.URL.Path != "/warm" && r.URL.Path != "/warm-after-probe" && r.URL.Path != "/probe" {
			w.WriteHeader(http.StatusInternalServerError)
		}
		n, err := io.WriteString(w, f.body)
		f.requests <- qualityProbeMuxRequest{path: r.URL.Path, method: r.Method,
			userAgent: r.Header.Get("User-Agent"), written: n, err: err}
	}))
	f.http.Config.ReadHeaderTimeout = 3 * time.Second
	f.http.Config.WriteTimeout = 3 * time.Second
	// Covers the complete fixture phase budget; each active HTTP operation
	// below has its own 5s bound. This is a lifetime, not a signal threshold.
	f.http.Config.IdleTimeout = 20 * time.Second
	f.http.Start()
	t.Cleanup(f.http.Close)

	reservation, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverPort := reservation.Addr().(*gonet.TCPAddr).Port
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	const userID = "ce372f23-82e8-4a4b-a0d8-83928b0be021"
	server := startQualityProbeMuxInstance(t, fmt.Sprintf(`{
		"log": {"loglevel": "none"},
		"inbounds": [{"listen": "127.0.0.1", "port": %d, "protocol": "vless",
			"settings": {"clients": [{"id": %q}], "decryption": "none"}}],
		"outbounds": [{"protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]}}]
	}`, serverPort, userID))
	t.Cleanup(func() { _ = server.Close() })
	f.relay = startQualityProbeMuxRelay(t, gonet.JoinHostPort("127.0.0.1", strconv.Itoa(serverPort)))
	relayPort := f.relay.listener.Addr().(*gonet.TCPAddr).Port
	f.client = startQualityProbeMuxInstance(t, fmt.Sprintf(`{
		"log": {"loglevel": "none"},
		"outbounds": [{"tag": "v1", "protocol": "vless",
			"settings": {"vnext": [{"address": "127.0.0.1", "port": %d,
				"users": [{"id": %q, "encryption": "none"}]}]},
			"mux": {"enabled": true, "concurrency": 8},
			"path": {"balancer": "auto", "cost": 1, "probe": "quality-local"}}],
		"routing": {"balancers": [{"tag": "auto", "strategy": {"type": "leastCost",
			"settings": {"startOutbound": "v1", "shouldProbeBeforeSwitch": false}}}]},
		"healthObservatory": {
			"passive": {"dialTimeout": "3s", "firstByteTimeout": "3s", "stallTimeout": "5s"},
			"probes": [{"name": "quality-local", "destination": %q, "expectedStatuses": [200],
				"minBytes": %d, "timeout": "4s", "progressTimeout": "2s"}]
		}
	}`, relayPort, userID, f.http.URL+"/configured-probe", qualityProbeMuxBodyBytes))
	t.Cleanup(func() { _ = f.client.Close() })
	f.observer = health.FromInstance(f.client)
	if f.observer == nil {
		t.Fatal("quality Mux fixture has no health observer")
	}
	// There is one path, already selected, and no connectivity config,
	// providers, network change, or cheaper path that can schedule probes.
	// The named fallback URL is also local and has a distinct path so any
	// unintended scheduled request fails the fixture's request assertion.
	f.relay.assertNoExtra(t)
	return f
}

func startQualityProbeMuxInstance(t *testing.T, raw string) *core.Instance {
	t.Helper()
	cfg, err := serial.LoadJSONConfig(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		_ = inst.Close()
		t.Fatal(err)
	}
	return inst
}

type qualityProbeMuxWarm struct {
	conn   gonet.Conn
	ctx    context.Context
	reader *bufio.Reader
}

func (warm *qualityProbeMuxWarm) assertLive(t *testing.T) {
	t.Helper()
	if err := warm.ctx.Err(); err != nil {
		t.Fatalf("warm user context ended before quality comparison: %v", err)
	}
}

func (f *qualityProbeMuxFixture) openWarmUser(t *testing.T) *qualityProbeMuxWarm {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	ctx = session.SetForcedOutboundTagToContext(ctx, "v1")
	port := net.Port(f.http.Listener.Addr().(*gonet.TCPAddr).Port)
	conn, err := core.Dial(ctx, f.client, net.TCPDestination(net.ParseAddress("127.0.0.1"), port))
	if err != nil {
		t.Fatalf("warm user dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	t.Cleanup(func() { stopClose() })
	warm := &qualityProbeMuxWarm{conn: conn, ctx: ctx, reader: bufio.NewReader(conn)}
	f.warmRoundtrip(t, warm, "/warm")
	return warm
}

func (f *qualityProbeMuxFixture) warmRoundtrip(t *testing.T, warm *qualityProbeMuxWarm, path string) {
	t.Helper()
	warm.assertLive(t)
	phase, cancel := context.WithTimeout(warm.ctx, 5*time.Second)
	defer cancel()
	stopClose := context.AfterFunc(phase, func() { _ = warm.conn.Close() })
	defer stopClose()
	if _, err := fmt.Fprintf(warm.conn, "GET %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: quality-mux-user\r\nConnection: keep-alive\r\n\r\n", path, f.http.Listener.Addr()); err != nil {
		t.Fatalf("warm user request: %v", err)
	}
	response, err := http.ReadResponse(warm.reader, nil)
	if err != nil {
		t.Fatalf("warm user response: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != f.body || response.StatusCode != http.StatusOK || response.Close ||
		response.Header.Get("X-Quality-Mux-Fixture") != path {
		t.Fatalf("warm user response: status %d, close %v, bytes %d, error %v", response.StatusCode, response.Close, len(body), err)
	}
	f.assertRequest(t, path)
	// A successful stop proves the operation's close callback did not start
	// while its server-side completion assertion was still waiting.
	if !stopClose() || phase.Err() != nil {
		t.Fatalf("warm user HTTP roundtrip exceeded its operation bound: %v", phase.Err())
	}
	warm.assertLive(t)
}

func (f *qualityProbeMuxFixture) assertProbeResult(t *testing.T, ok bool, wire int64, msg string) {
	t.Helper()
	if !ok || wire <= 0 {
		t.Fatalf("local Mux probe: ok %v, physical bytes %d, error %q", ok, wire, msg)
	}
	f.assertRequest(t, "/probe")
}

func (f *qualityProbeMuxFixture) assertRequest(t *testing.T, path string) {
	t.Helper()
	select {
	case request := <-f.requests:
		if request.path != path || request.method != "GET" || request.userAgent == "" ||
			request.written != len(f.body) || request.err != nil {
			t.Fatalf("local HTTP request: %+v, want GET %s with headers and %d body bytes", request, path, len(f.body))
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("no completed local HTTP request for %s", path)
	}
}

type qualityProbeMuxSnapshot struct {
	Paths          map[string]map[string]json.RawMessage `json:"paths"`
	Witness        map[string]json.RawMessage            `json:"witness,omitempty"`
	SamplesDropped uint64                                `json:"samples_dropped,omitempty"`
}

func readQualityProbeMuxSnapshot(t *testing.T, observer *health.Observer) qualityProbeMuxSnapshot {
	t.Helper()
	var status struct {
		Paths []struct {
			Outbound string          `json:"outbound"`
			Quality  json.RawMessage `json:"quality"`
		} `json:"paths"`
		Witness        json.RawMessage `json:"witness_quality"`
		SamplesDropped uint64          `json:"quality_samples_dropped"`
	}
	if err := json.Unmarshal(observer.Status(), &status); err != nil {
		t.Fatal(err)
	}
	snapshot := qualityProbeMuxSnapshot{Paths: make(map[string]map[string]json.RawMessage), SamplesDropped: status.SamplesDropped}
	for _, path := range status.Paths {
		if len(path.Quality) == 0 {
			continue
		}
		var quality map[string]json.RawMessage
		if err := json.Unmarshal(path.Quality, &quality); err != nil || quality == nil {
			t.Fatalf("invalid quality for %s: %s, error %v", path.Outbound, path.Quality, err)
		}
		snapshot.Paths[path.Outbound] = quality
	}
	if len(status.Witness) != 0 {
		if err := json.Unmarshal(status.Witness, &snapshot.Witness); err != nil || snapshot.Witness == nil {
			t.Fatalf("invalid witness quality: %s, error %v", status.Witness, err)
		}
	}
	return snapshot
}

func (snapshot qualityProbeMuxSnapshot) canonical(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func assertQualityProbeMuxEmpty(t *testing.T, snapshot qualityProbeMuxSnapshot) {
	t.Helper()
	if len(snapshot.Paths) != 0 || snapshot.Witness != nil || snapshot.SamplesDropped != 0 {
		t.Fatalf("probe-only Mux traffic entered measured quality: %s", snapshot.canonical(t))
	}
}

func waitQualityProbeMuxInitial(t *testing.T, observer *health.Observer) qualityProbeMuxSnapshot {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	for {
		snapshot := readQualityProbeMuxSnapshot(t, observer)
		quality := snapshot.Paths["v1"]
		count := func(name string) uint64 {
			var value uint64
			if raw := quality[name]; len(raw) != 0 {
				if err := json.Unmarshal(raw, &value); err != nil {
					t.Fatalf("invalid %s: %s", name, raw)
				}
			}
			return value
		}
		ttfb, dial, tcp := count("ttfb_count"), count("dial_count"), count("tcp_count")
		if ttfb > 1 || dial > 1 || tcp > 1 {
			t.Fatalf("fixture recorded more than one physical user connection: %s", snapshot.canonical(t))
		}
		if ttfb == 1 && dial == 1 {
			for _, field := range []string{"ttfb_p50_ms", "ttfb_p90_ms", "dial_p50_ms", "dial_p90_ms"} {
				if len(quality[field]) == 0 {
					t.Fatalf("warm user has no measured %s: %s", field, snapshot.canonical(t))
				}
			}
			if tcp != 0 {
				for _, field := range []string{"tcp_rtt_p50_ms", "tcp_rtt_p90_ms"} {
					if len(quality[field]) == 0 {
						t.Fatalf("native TCP sample has no %s: %s", field, snapshot.canonical(t))
					}
				}
			}
			if len(quality["bytes_down"]) != 0 || count("bulk_count") != 0 {
				t.Fatalf("warm physical user connection already ended or crossed a bulk mark: %s", snapshot.canonical(t))
			}
			return snapshot
		}
		select {
		case <-poll.C:
		case <-deadline.C:
			t.Fatalf("warm Mux user has no initial TTFB/dial quality: %s", snapshot.canonical(t))
		}
	}
}

// The relay counts physical TCP accepts and completed copy bytes without
// replacing the client's real TCP socket or inspecting Mux internals.
type qualityProbeMuxRelay struct {
	listener gonet.Listener
	target   string
	accepted chan *qualityProbeMuxWire
	errors   chan error
	loopDone chan struct{}
	mu       sync.Mutex
	closed   bool
	wires    []*qualityProbeMuxWire
}

type qualityProbeMuxWire struct {
	client, upstream gonet.Conn
	done             chan struct{}
	up, down         int64 // Read only after done closes.
}

func startQualityProbeMuxRelay(t *testing.T, target string) *qualityProbeMuxRelay {
	t.Helper()
	listener, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &qualityProbeMuxRelay{listener: listener, target: target,
		accepted: make(chan *qualityProbeMuxWire, 8), errors: make(chan error, 8), loopDone: make(chan struct{})}
	go r.serve()
	t.Cleanup(func() { r.close(t) })
	return r
}

func (r *qualityProbeMuxRelay) serve() {
	defer close(r.loopDone)
	for {
		client, err := r.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := gonet.DialTimeout("tcp", r.target, 3*time.Second)
		if err != nil {
			_ = client.Close()
			r.errors <- err
			continue
		}
		wire := &qualityProbeMuxWire{client: client, upstream: upstream, done: make(chan struct{})}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			_ = client.Close()
			_ = upstream.Close()
			return
		}
		r.wires = append(r.wires, wire)
		r.mu.Unlock()
		go wire.copy()
		r.accepted <- wire
	}
}

func (wire *qualityProbeMuxWire) copy() {
	type copied struct {
		up bool
		n  int64
	}
	results := make(chan copied, 2)
	go func() { n, _ := io.Copy(wire.upstream, wire.client); results <- copied{up: true, n: n} }()
	go func() { n, _ := io.Copy(wire.client, wire.upstream); results <- copied{n: n} }()
	first := <-results
	_ = wire.client.Close()
	_ = wire.upstream.Close()
	second := <-results
	for _, result := range []copied{first, second} {
		if result.up {
			wire.up = result.n
		} else {
			wire.down = result.n
		}
	}
	close(wire.done)
}

func (r *qualityProbeMuxRelay) next(t *testing.T) *qualityProbeMuxWire {
	t.Helper()
	select {
	case wire := <-r.accepted:
		return wire
	case err := <-r.errors:
		t.Fatalf("local TCP relay dial: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("no new physical TCP connection through local Mux relay")
	}
	return nil
}

func (r *qualityProbeMuxRelay) assertNoExtra(t *testing.T) {
	t.Helper()
	select {
	case <-r.accepted:
		t.Fatal("unexpected extra physical Mux connection")
	case err := <-r.errors:
		t.Fatalf("local TCP relay dial: %v", err)
	default:
	}
}

func (wire *qualityProbeMuxWire) waitDone(t *testing.T) {
	t.Helper()
	select {
	case <-wire.done:
	case <-time.After(3 * time.Second):
		t.Fatal("probe's isolated physical TCP connection did not finish")
	}
}

func (wire *qualityProbeMuxWire) assertOpen(t *testing.T) {
	t.Helper()
	select {
	case <-wire.done:
		t.Fatal("warm user's physical TCP connection ended before quality comparison")
	default:
	}
}

func (wire *qualityProbeMuxWire) assertTransferred(t *testing.T) {
	t.Helper()
	if wire.up <= 0 || wire.down < qualityProbeMuxBodyBytes {
		t.Fatalf("probe relay completed without request/full response wire bytes: up %d, down %d", wire.up, wire.down)
	}
}

func (r *qualityProbeMuxRelay) close(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	r.closed = true
	_ = r.listener.Close()
	for _, wire := range r.wires {
		_ = wire.client.Close()
		_ = wire.upstream.Close()
	}
	r.mu.Unlock()
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	select {
	case <-r.loopDone:
	case <-deadline.C:
		t.Error("local Mux relay accept loop did not finish during cleanup")
		return
	}
	r.mu.Lock()
	wires := append([]*qualityProbeMuxWire(nil), r.wires...)
	r.mu.Unlock()
	for _, wire := range wires {
		select {
		case <-wire.done:
		case <-deadline.C:
			t.Error("local Mux relay copy did not finish during cleanup")
			return
		}
	}
}
