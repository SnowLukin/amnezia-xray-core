package health

import (
	"bufio"
	"io"
	gonet "net"
	"net/http"
	"reflect"
	"sync"
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

// The addresses of a step are fallbacks through one server: the next is
// asked only after the one before failed.
func TestProbeStepsAskFallbackAddressesInTurn(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first string
		want  []string
		dest  int
	}{
		{"first answers", "204 No Content", []string{"/first"}, 0},
		{"first fails", "503 Unavailable", []string{"/first", "/second"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var paths []string
			d := &probePhaseDispatcher{reportDial: true, serve: func(c gonet.Conn) {
				req, err := http.ReadRequest(bufio.NewReader(c))
				if err != nil {
					return
				}
				req.Body.Close()
				mu.Lock()
				paths = append(paths, req.URL.Path)
				mu.Unlock()
				if req.URL.Path == "/first" {
					// The second address must not be asked while the first
					// one is still answering.
					time.Sleep(100 * time.Millisecond)
					io.WriteString(c, "HTTP/1.1 "+tc.first+"\r\nContent-Length: 0\r\n\r\n")
					return
				}
				io.WriteString(c, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
			}}
			o := probePhaseObserver(time.Second, d)
			defer o.stopRun()
			r := o.probe("a1", &probeSettings{name: "fallback", steps: []probeStepSettings{
				stepSettings("egress", false, "http://example.test/first", "http://example.test/second"),
			}})
			mu.Lock()
			defer mu.Unlock()
			if !r.ok || len(r.steps) != 1 || r.steps[0].destination != tc.dest || !reflect.DeepEqual(paths, tc.want) {
				t.Fatalf("result %+v, asked %v, want %v", r, paths, tc.want)
			}
		})
	}
}

// Connectivity asks different servers: all at once, the first answer wins.
func TestConnectivityAddressesRace(t *testing.T) {
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
	step := stepSettings("connectivity", false, "http://example.test/slow", "http://example.test/fast")
	step.isRace = true
	r := o.probe("direct", &probeSettings{steps: []probeStepSettings{step}})
	if !r.ok || r.steps[0].destination != 1 {
		t.Fatalf("connectivity: %+v", r)
	}
	select {
	case <-slowClosed:
	case <-time.After(time.Second):
		t.Fatal("losing connection stayed open")
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
