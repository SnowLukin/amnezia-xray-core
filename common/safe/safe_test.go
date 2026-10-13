package safe

import (
	"bytes"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRecoveryPrivacyAndCleanup(t *testing.T) {
	Reset()
	defer Reset()
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	closed := false
	Run(Connection, "test/connection.go:handler", func() { closed = true }, func() { panic("secret.example 1.2.3.4 payload") })
	count, events, broken := Snapshot(0)
	if count != 1 || len(events) != 1 || !closed || len(broken) != 0 {
		t.Fatalf("count %d events %v closed %v broken %v", count, events, closed, broken)
	}
	if strings.Contains(logs.String(), "secret") || strings.Contains(logs.String(), "1.2.3.4") || !strings.Contains(logs.String(), "safe_test.go:") {
		t.Fatal(logs.String())
	}
}
func TestConnectionIsolation(t *testing.T) {
	Reset()
	defer Reset()
	bad, badPeer := net.Pipe()
	good, goodPeer := net.Pipe()
	defer good.Close()
	defer goodPeer.Close()
	defer badPeer.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(Connection, "test/connection.go:bad", Closer(bad), func() { panic("bad connection") })
	}()
	<-done
	badPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := badPeer.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("bad still open: %v", err)
	}
	go good.Write([]byte("ok"))
	goodPeer.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 2)
	if _, err := io.ReadFull(goodPeer, b); err != nil || string(b) != "ok" {
		t.Fatalf("neighbor %q %v", b, err)
	}
}
func TestSubsystemStopsAndCountIsConcurrent(t *testing.T) {
	Reset()
	defer Reset()
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { Run(Connection, "test/service.go:run", nil, func() { panic("service") }) })
	}
	wg.Wait()
	Run(Subsystem, "test/service.go:run", nil, func() { panic("service") })
	n, _, broken := Snapshot(0)
	if n != 21 || len(broken) != 1 {
		t.Fatalf("%d %v", n, broken)
	}
	ran := false
	Run(Subsystem, "test/service.go:run", nil, func() { ran = true })
	if ran {
		t.Fatal("restarted broken subsystem")
	}
}
func TestErrorUnblocksWaiters(t *testing.T) {
	Reset()
	defer Reset()
	if err := Error(Connection, "test/task.go:worker", func() error { panic("worker") }); err != ErrPanic {
		t.Fatal(err)
	}
}
func TestRunLifecycleDoesNotResetLiveCursor(t *testing.T) {
	Reset()
	defer Reset()
	BeginRun()
	Run(Subsystem, "test/service.go:run", nil, func() { panic("one") })
	n, _, _ := Snapshot(0)
	BeginRun()
	after, _, broken := Snapshot(n)
	if after != n || len(broken) != 1 {
		t.Fatalf("reset live state %d %d %v", n, after, broken)
	}
	EndRun()
	EndRun()
	BeginRun()
	after, _, broken = Snapshot(n)
	if after != n || len(broken) != 0 {
		t.Fatalf("restart %d %v", after, broken)
	}
	EndRun()
}
