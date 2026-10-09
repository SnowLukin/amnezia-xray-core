package outbound

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
)

type plainProxy struct{}

func (plainProxy) Process(context.Context, *transport.Link, internet.Dialer) error { return nil }

type sleepyProxy struct {
	plainProxy
	asleep bool
	calls  int
}

func (p *sleepyProxy) Sleep() bool {
	p.calls++
	return p.asleep
}

func TestHandlerSleepAsksProxy(t *testing.T) {
	for _, asleep := range []bool{true, false} {
		p := &sleepyProxy{asleep: asleep}
		h := &Handler{proxy: p}
		if got := h.Sleep(); got != asleep || p.calls != 1 {
			t.Errorf("Sleep() = %v after %d calls, want %v after 1", got, p.calls, asleep)
		}
	}
	// Such a proxy holds nothing to release.
	if !(&Handler{proxy: plainProxy{}}).Sleep() {
		t.Error("a handler whose proxy holds nothing refused to sleep")
	}
}

// processLog is an observer that keeps what the handler tells of the
// connections its proxy processes.
type processLog struct {
	mu     sync.Mutex
	events []string
}

func (l *processLog) Observes(string) bool { return true }

func (l *processLog) StartDial(context.Context, string, net.Destination, func()) outbound.DialTracker {
	return nil
}

func (l *processLog) ProcessStarted(tag string) { l.add("start " + tag) }

func (l *processLog) ProcessEnded(tag string) { l.add("end " + tag) }

func (l *processLog) add(e string) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *processLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.events, ", ")
}

// loggingSleeper can sleep and writes into the log while it processes.
type loggingSleeper struct {
	sleepyProxy
	log *processLog
}

func (p *loggingSleeper) Process(context.Context, *transport.Link, internet.Dialer) error {
	p.log.add("process")
	return nil
}

type loggingProxy struct{ log *processLog }

func (p loggingProxy) Process(context.Context, *transport.Link, internet.Dialer) error {
	p.log.add("process")
	return nil
}

// A proxy that can sleep may wake for a connection whose dials pass the
// handler by, so the handler tells the observer of every connection it
// hands such a proxy, before and after; of others it tells nothing.
func TestHandlerTellsTheObserverWhatASleeperProcesses(t *testing.T) {
	log := &processLog{}
	h := &Handler{tag: "w1", proxy: &loggingSleeper{log: log}, observer: log}
	if err := h.process(context.Background(), &transport.Link{}); err != nil {
		t.Fatal(err)
	}
	if got := log.String(); got != "start w1, process, end w1" {
		t.Fatalf("told %q", got)
	}
	log = &processLog{}
	h = &Handler{tag: "a1", proxy: loggingProxy{log: log}, observer: log}
	if err := h.process(context.Background(), &transport.Link{}); err != nil {
		t.Fatal(err)
	}
	if got := log.String(); got != "process" {
		t.Fatalf("a proxy that cannot sleep: told %q", got)
	}
}
