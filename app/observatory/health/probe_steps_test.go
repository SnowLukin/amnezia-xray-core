package health

import (
	"bufio"
	"io"
	gonet "net"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func stepSettings(name string, optional bool, destinations ...string) probeStepSettings {
	return probeStepSettings{
		name: name, isOptional: optional, destinations: destinations, method: http.MethodGet,
		timeout: time.Second, progressTimeout: time.Second,
	}
}

func TestProbeStepsStopAfterMandatoryFailure(t *testing.T) {
	var paths []string
	d := &probePhaseDispatcher{reportDial: true, serve: func(c gonet.Conn) {
		req, err := http.ReadRequest(bufio.NewReader(c))
		if err != nil {
			return
		}
		req.Body.Close()
		paths = append(paths, req.URL.Path)
		io.WriteString(c, "HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n")
	}}
	o := probePhaseObserver(time.Second, d)
	defer o.stopRun()
	r := o.probe("a1", &probeSettings{name: "full", steps: []probeStepSettings{
		stepSettings("transport", false, "http://example.test/mandatory"),
		stepSettings("egress", true, "http://example.test/optional"),
	}})
	if r.ok || r.state != statusDown || r.class != "status_503" || r.profile != "full" ||
		len(r.steps) != 1 || r.steps[0].name != "transport" || r.steps[0].destination != -1 ||
		r.steps[0].class != "status_503" || r.steps[0].elapsed <= 0 ||
		!reflect.DeepEqual(paths, []string{"/mandatory"}) {
		t.Fatalf("mandatory failure: result %+v, paths %v", r, paths)
	}
}

func TestProbeStepsOptionalFailuresContinueAndKeepPathLive(t *testing.T) {
	var paths []string
	d := &probePhaseDispatcher{reportDial: true, serve: func(c gonet.Conn) {
		req, err := http.ReadRequest(bufio.NewReader(c))
		if err != nil {
			return
		}
		req.Body.Close()
		paths = append(paths, req.URL.Path)
		if req.URL.Path == "/egress" {
			io.WriteString(c, "HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n")
			return
		}
		io.WriteString(c, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
	}}
	o := probePhaseObserver(time.Second, d)
	defer o.stopRun()
	r := o.probe("a1", &probeSettings{name: "full", steps: []probeStepSettings{
		stepSettings("transport", false, "http://example.test/transport"),
		stepSettings("egress", true, "http://example.test/egress"),
		stepSettings("egress_ip", true, "http://example.test/egress_ip"),
	}})
	if !r.ok || r.state != statusDegraded || r.err != "" || r.class != "" || r.degradedReason() != "egress" ||
		len(r.steps) != 3 || !reflect.DeepEqual(paths, []string{"/transport", "/egress", "/egress_ip"}) {
		t.Fatalf("optional failure: result %+v, paths %v", r, paths)
	}
	for i, want := range []bool{true, false, true} {
		if r.steps[i].ok != want || r.steps[i].elapsed <= 0 || (want && r.steps[i].destination != 0) ||
			(!want && (r.steps[i].destination != -1 || r.steps[i].class != "status_503")) {
			t.Fatalf("step %d: %+v", i, r.steps[i])
		}
	}
}

func TestProbeStepsParallelWinnerClosesLoser(t *testing.T) {
	slowStarted := make(chan struct{})
	slowClosed := make(chan struct{})
	d := &probePhaseDispatcher{reportDial: true, serve: func(c gonet.Conn) {
		req, err := http.ReadRequest(bufio.NewReader(c))
		if err != nil {
			return
		}
		req.Body.Close()
		if req.URL.Path == "/slow" {
			close(slowStarted)
			io.Copy(io.Discard, c)
			close(slowClosed)
			return
		}
		<-slowStarted
		io.WriteString(c, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
	}}
	o := probePhaseObserver(time.Second, d)
	defer o.stopRun()
	start := time.Now()
	r := o.probe("a1", &probeSettings{name: "parallel", steps: []probeStepSettings{
		stepSettings("egress", false, "http://example.test/slow", "http://example.test/fast"),
	}})
	if !r.ok || r.state != statusUp || len(r.steps) != 1 || r.steps[0].destination != 1 ||
		r.steps[0].connect <= 0 || r.steps[0].elapsed <= 0 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("parallel winner: %+v after %v", r, time.Since(start))
	}
	select {
	case <-slowClosed:
	case <-time.After(time.Second):
		t.Fatal("losing connection stayed open")
	}
}

func TestProbeStepsLegacySettingsBecomeOneMandatoryStep(t *testing.T) {
	d := &probePhaseDispatcher{reportDial: true, serve: func(c gonet.Conn) {
		serveProbeChunks(c, 0, 0, 10)
	}}
	o := probePhaseObserver(time.Second, d)
	defer o.stopRun()
	ps := probePhaseSettings(time.Second, time.Second, 10)
	ps.name = "light"
	r := o.probe("a1", ps)
	if !r.ok || r.state != statusUp || r.profile != "light" || len(r.steps) != 1 ||
		r.steps[0].name != "light" || !r.steps[0].ok || r.steps[0].destination != 0 || r.rtt <= 0 {
		t.Fatalf("legacy profile: %+v", r)
	}
}

func TestProbeStepsAllDestinationsFail(t *testing.T) {
	d := &probePhaseDispatcher{reportDial: true, serve: func(c gonet.Conn) {
		req, err := http.ReadRequest(bufio.NewReader(c))
		if err != nil {
			return
		}
		req.Body.Close()
		if req.URL.Path == "/short" {
			io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx")
			return
		}
		io.WriteString(c, "HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n")
	}}
	o := probePhaseObserver(time.Second, d)
	defer o.stopRun()
	step := stepSettings("egress", false, "http://example.test/short", "http://example.test/status")
	step.minBytes = 10
	r := o.probe("a1", &probeSettings{name: "full", steps: []probeStepSettings{step}})
	if r.ok || r.state != statusDown || r.class != "short_body" || len(r.steps) != 1 ||
		r.steps[0].ok || r.steps[0].destination != -1 || r.steps[0].class != "short_body" {
		t.Fatalf("all destinations failed: %+v", r)
	}
}
