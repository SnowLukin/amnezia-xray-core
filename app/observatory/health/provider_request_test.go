package health

import (
	gotls "crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	xerrors "github.com/xtls/xray-core/common/errors"
)

// requestOf starts a host refresh at now and returns its body as the backend
// gets it, and the job to end it with.
func requestOf(t *testing.T, te *testEngine, now time.Time) (map[string]any, []byte, *refreshJob) {
	t.Helper()
	te.requestRefresh("fresh")
	jobs, _ := te.refreshAt(now)
	if len(jobs) != 1 || jobs[0].request == nil {
		t.Fatalf("at %v: refreshes %v, want one with a body", now.Sub(t0), triggers(jobs))
	}
	o := &Observer{run: "4f1c0de2a9b87765"}
	body := o.requestBody(&providerRuntime{cfg: te.prov().cfg}, jobs[0])
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	return m, body, jobs[0]
}

// setEngine has the provider set x2, x1, x3 in this order of the answer on
// a balancer that started on x1, the cheapest.
func setEngine(t *testing.T) *testEngine {
	t.Helper()
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Limits.ExhaustedMinInterval = int64(time.Second)
	}), &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 3, "p-x3", 5), SwitchWithoutProbe: true},
		"p-x1", "p-x2", "p-x3")
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x2", "p-x1", "p-x3"}}, t0)
	return te
}

func TestRequestBodyDescribesTheSet(t *testing.T) {
	te := setEngine(t)
	te.networkID = "home"
	// x1 fails to dial and the balancer moves to x2.
	te.failN(3, "p-x1", failDialTimeout, at(10*time.Second))
	if te.sel.current() != "p-x2" {
		t.Fatalf("setup: active %q", te.sel.current())
	}
	x1 := te.path("p-x1")
	x1.stats.probes, x1.lastProbeEnd, x1.lastError, x1.lastClass = 1, at(30*time.Second), "Get \"https://probe.test/\": EOF", "eof"
	x2 := te.path("p-x2")
	x2.stats.connsOK, x2.stats.bytesDown, x2.stats.bytesUp = 412, 50<<20, 3<<10
	x2.stats.ttfb.add(180 * time.Millisecond)
	x2.stats.probes, x2.lastProbeEnd, x2.lastRTT = 1, at(20*time.Second), 180*time.Millisecond
	te.prov().version = "v7"

	now := at(time.Minute)
	m, _, _ := requestOf(t, te, now)
	if m["protocol"] != 1.0 || m["provider"] != "fresh" || m["refresh"] != 1.0 || m["trigger"] != "host" ||
		m["retry"] != 0.0 || m["version"] != "v7" || m["memoryLow"] != false || m["lastFailure"] != nil {
		t.Fatalf("head of the body %v", m)
	}
	if m["clientTime"] != now.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("clientTime %v, want %v", m["clientTime"], now.UTC())
	}
	net := m["network"].(map[string]any)
	if net["id"] != providerHash("https://provider.test/set", "home") || net["known"] != false || net["up"] != true {
		t.Fatalf("network %v", net)
	}
	b := m["balancer"].(map[string]any)
	if b["tag"] != "auto" || b["current"] != "x2" || b["own"] != true || b["cost"] != 3.0 || b["exhausted"] != false ||
		b["pinned"] != false || b["heldMs"] != 50000.0 {
		t.Fatalf("balancer %v", b)
	}
	outs := m["outbounds"].([]any)
	var tags []string
	for _, o := range outs {
		tags = append(tags, o.(map[string]any)["tag"].(string))
	}
	if strings.Join(tags, ",") != "x2,x1,x3" {
		t.Fatalf("outbounds %v, want the set without the prefix in the order of the answer", tags)
	}
	o2, o1, o3 := outs[0].(map[string]any), outs[1].(map[string]any), outs[2].(map[string]any)
	if o1["state"] != "down" || o1["reason"] != "traffic" || o1["stateMs"] != 50000.0 || o1["seenMs"] != 60000.0 ||
		o1["quarantineLevel"] != 1.0 || o1["quarantineMs"] == nil || fmt.Sprint(o1["failures"]) != "map[dial_timeout:3]" {
		t.Fatalf("x1 %v", o1)
	}
	if p := o1["probe"].(map[string]any); p["ok"] != false || p["error"] != "eof" || p["agoMs"] != 30000.0 || p["rttMs"] != nil {
		t.Fatalf("probe of x1 %v", p)
	}
	if o2["connsOk"] != 412.0 || o2["bytesDown"] != float64(50<<20) || o2["bytesUp"] != 3072.0 ||
		o2["ttfbP50Ms"] != 200.0 || o2["activeMs"] != 50000.0 || o2["failures"] != nil {
		t.Fatalf("x2 %v", o2)
	}
	if p := o2["probe"].(map[string]any); p["ok"] != true || p["rttMs"] != 180.0 || p["error"] != nil {
		t.Fatalf("probe of x2 %v", p)
	}
	if o3["state"] != "unknown" || o3["probe"] != nil || o3["connsOk"] != nil {
		t.Fatalf("x3 %v", o3)
	}
	if o1["activeMs"] != 10000.0 {
		t.Fatalf("x1 was chosen for 10 s, activeMs %v", o1["activeMs"])
	}
}

// The probe in the body is the last one that finished, dated by its result:
// a probe that starts in the tick that gathers the body does not make an old
// result look fresh.
func TestRequestProbeIsDatedByItsResult(t *testing.T) {
	te := setEngine(t)
	j := te.enqueue("p-x2", reasonConfirm, "auto", t0)
	te.begin(j, at(time.Second))
	te.onProbeResult(j, probeResult{ok: true, rtt: 180 * time.Millisecond}, at(3*time.Second))
	now := at(10 * time.Minute)
	te.begin(te.enqueue("p-x2", reasonFailover, "auto", now), now)
	m, _, _ := requestOf(t, te, now)
	x2 := m["outbounds"].([]any)[0].(map[string]any)
	if p := x2["probe"].(map[string]any); p["ok"] != true || p["rttMs"] != 180.0 ||
		p["agoMs"] != float64((10*time.Minute-3*time.Second).Milliseconds()) {
		t.Fatalf("probe of x2 while another one runs: %v", p)
	}
}

// The balancer on an outbound of another provider or of the config is no
// business of the backend: it learns only that its outbounds are not chosen.
func TestRequestBodyHidesAPathOfOthers(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("a", 1, "p-", 5)}, "a1", "p-x1")
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x1"}}, t0)
	m, body, _ := requestOf(t, te, at(time.Second))
	b := m["balancer"].(map[string]any)
	if b["own"] != false || b["current"] != nil || b["cost"] != 1.0 || strings.Contains(string(body), "a1") {
		t.Fatalf("balancer %v in %s", b, body)
	}
}

// Nothing in the body names where users go, the servers or the probe: only
// tags, states, classes and counters.
func TestRequestBodyHoldsNoDestination(t *testing.T) {
	te := setEngine(t)
	te.networkID = "Home Wi-Fi 5G"
	for i := 0; i < 4; i++ {
		te.fail("p-x1", failNoResponse, fmt.Sprintf("secret%d.example:443", i), at(10*time.Second))
	}
	te.path("p-x1").serverName = "203.0.113.9"
	x2 := te.path("p-x2")
	x2.stats.probes, x2.lastProbeEnd, x2.lastClass = 1, at(20*time.Second), "eof"
	x2.lastError = "Get \"https://probe.example/bytes/32768\": unexpected EOF"
	_, body, _ := requestOf(t, te, at(time.Minute))
	for _, s := range []string{"secret", ".example", "203.0.113.9", "bytes/32768", "Home", "4f1c0de2a9b87765", "p-x"} {
		if strings.Contains(string(body), s) {
			t.Errorf("the body holds %q: %s", s, body)
		}
	}
}

// The run and the network go out hashed per provider: one backend sees them
// stable, two backends cannot match them, and the raw run of the report is
// not among them.
func TestNetworkIDAndRunAreHashedPerProvider(t *testing.T) {
	ids := map[string]bool{}
	for _, u := range []string{"https://a.test/set", "https://b.test/set"} {
		te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) { p.Url = u }), nil, "a1")
		te.networkID = "home"
		m, body, j := requestOf(t, te, t0)
		te.onRefreshDone(j, refreshOutcome{ok: true}, t0)
		run, network := m["run"].(string), m["network"].(map[string]any)["id"].(string)
		if len(run) != 16 || len(network) != 16 || strings.Contains(string(body), "home") ||
			strings.Contains(string(body), "4f1c0de2a9b87765") {
			t.Fatalf("run %q, network %q in %s", run, network, body)
		}
		if again, _, _ := requestOf(t, te, at(time.Hour)); again["run"] != run {
			t.Fatalf("the run changed between requests: %v, %v", run, again["run"])
		}
		ids[run], ids[network] = true, true
	}
	if len(ids) != 4 {
		t.Fatalf("two providers got the same hash: %v", ids)
	}
}

// The next request tells the backend why the previous one failed: the reason
// a well-formed answer was refused, or the class of the failure.
func TestLastFailureCarriesTheRejection(t *testing.T) {
	te := setEngine(t)
	_, _, j := requestOf(t, te, t0)
	reason := `invalid answer: unknown key "dns"`
	te.onRefreshDone(j, refreshOutcome{err: reason, status: 200, class: reason}, at(time.Second))
	m, _, j := requestOf(t, te, at(time.Minute))
	if lf := m["lastFailure"].(map[string]any); lf["status"] != 200.0 || lf["error"] != reason || lf["agoMs"] != 59000.0 ||
		m["retry"] != 1.0 || m["refresh"] != 2.0 {
		t.Fatalf("after a refused answer: %v", m)
	}
	long := "outbound x1: " + strings.Repeat("é", 200)
	te.onRefreshDone(j, refreshOutcome{err: long, status: 200, class: long}, at(time.Minute))
	m, _, j = requestOf(t, te, at(2*time.Hour))
	if e := m["lastFailure"].(map[string]any)["error"].(string); len(e) > 200 || !strings.HasPrefix(long, e) {
		t.Fatalf("a long reason goes out as %d bytes: %q", len(e), e)
	}
	te.onRefreshDone(j, refreshOutcome{err: "timeout", class: "timeout"}, at(2*time.Hour))
	m, _, j = requestOf(t, te, at(4*time.Hour))
	if lf := m["lastFailure"].(map[string]any); lf["status"] != 0.0 || lf["error"] != "timeout" {
		t.Fatalf("after a timeout: %v", lf)
	}
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(4*time.Hour))
	if m, _, _ = requestOf(t, te, at(5*time.Hour)); m["lastFailure"] != nil || m["retry"] != 0.0 {
		t.Fatalf("after a success: %v", m)
	}
}

// The largest set the provider may have, with long errors and busy
// outbounds, stays well within what a backend takes in one request: errors
// go out as classes.
func TestRequestBodyBounded(t *testing.T) {
	var tags []string
	for i := 0; i < maxProviderOutbounds; i++ {
		tags = append(tags, fmt.Sprintf("p-de-fra-reality-%03d", i))
	}
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Costs: costs("p-", 1)}, tags...)
	te.applyProviderSet(te.prov(), providerChange{tags: tags}, t0)
	for _, tag := range tags {
		p := te.path(tag)
		p.status, p.reason, p.statusSince = statusDown, "upgrade_probe", t0
		p.quarantineLevel, p.quarantineUntil = 9, at(time.Hour)
		for _, k := range []failureKind{failDialTimeout, failNoResponse, failStall} {
			p.stats.failuresByKind[k] = 1_000_000
		}
		p.stats.probes, p.lastProbeEnd, p.lastError, p.lastClass = 1, t0, strings.Repeat("e", 4096), "status_503"
		p.stats.connsOK, p.stats.connsFailed, p.stats.bytesUp, p.stats.bytesDown = 1_000_000, 1_000_000, 1<<40, 1<<40
		p.stats.activeTime = 24 * time.Hour
		p.stats.ttfb.add(10 * time.Second)
	}
	te.prov().lastFailedAt, te.prov().lastClass = t0, strings.Repeat("r", 1<<20)
	te.prov().version = strings.Repeat("v", 256)
	_, body, _ := requestOf(t, te, at(time.Minute))
	t.Logf("%d outbounds: %d bytes, %d per outbound", len(tags), len(body), len(body)/len(tags))
	if len(body) > 64<<10 {
		t.Fatalf("the body of %d outbounds takes %d bytes", len(tags), len(body))
	}
}

func TestGetGathersNoBody(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) { p.Method = "get" }), nil, "a1")
	te.requestRefresh("fresh")
	if jobs, _ := te.refreshAt(t0); len(jobs) != 1 || jobs[0].request != nil {
		t.Fatalf("a GET provider gathered a body: %v", jobs)
	}
}

// The class of a probe error comes from its type; the text, which may name
// the URL of the probe, is never read.
func TestProbeErrorClass(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&url.Error{Op: "Get", URL: "https://probe.test/", Err: fmt.Errorf("read: %w", syscall.ECONNRESET)}, "reset"},
		{xerrors.New("dial failed").Base(os.NewSyscallError("connect", syscall.ECONNREFUSED)), "refused"},
		{&url.Error{Op: "Get", URL: "https://probe.test/", Err: io.EOF}, "eof"},
		{fmt.Errorf("body: %w", io.ErrUnexpectedEOF), "eof"},
		{gotls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}, "tls"},
		{&url.Error{Op: "Get", URL: "https://probe.test/", Err: x509.UnknownAuthorityError{}}, "tls"},
		{&url.Error{Op: "Get", URL: "https://probe.test/", Err: timeoutError{}}, "timeout"},
		{errors.New("connection reset by peer"), "other"},
	} {
		if got := errorClass(tc.err); got != tc.want {
			t.Errorf("%v: class %q, want %q", tc.err, got, tc.want)
		}
	}
	te := newTestEngine(t, nil, nil, "a1")
	j := te.enqueue("a1", reasonConfirm, "auto", t0)
	te.begin(j, t0)
	te.onProbeResult(j, probeResult{err: "status 404", class: "status_404"}, at(time.Second))
	if p := te.path("a1"); p.lastClass != "status_404" {
		t.Fatalf("a failed probe keeps class %q", p.lastClass)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
