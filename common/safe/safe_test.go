package safe

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
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

// child runs action in a copy of the test binary and returns its output.
func child(t *testing.T, action func()) (output string, exit int, isChild bool) {
	if os.Getenv("SAFE_TEST_CHILD") == t.Name() {
		action()
		return "", 0, true
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1")
	cmd.Env = append(os.Environ(), "SAFE_TEST_CHILD="+t.Name())
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		exit = exitErr.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return string(out), exit, false
}

func crashingService() { panic("service failure") }

func TestSubsystemPanicOutsideObserverEndsProcess(t *testing.T) {
	out, exit, isChild := child(t, func() {
		go Run(Subsystem, "test/service.go:run", nil, crashingService)
		time.Sleep(2 * time.Second)
	})
	if isChild {
		return
	}
	if exit != 2 || !strings.Contains(out, "panic: service failure") || !strings.Contains(out, "goroutine ") || !strings.Contains(out, "safe.crashingService") {
		t.Fatalf("exit %d\n%s", exit, out)
	}
}

func TestObserverSubsystemPanicFreezesOnlyObserver(t *testing.T) {
	Reset()
	defer Reset()
	stopped := false
	Run(Subsystem, "app/observatory/health/health.go:o.loop", func() { stopped = true }, crashingService)
	if _, _, broken := Snapshot(0); !stopped || len(broken) != 1 || broken[0] != "app/observatory/health" {
		t.Fatalf("stopped %v broken %v", stopped, broken)
	}
	if Allowed(Subsystem, "app/observatory/health/probe.go:run", nil) {
		t.Fatal("broken observer restarted")
	}
}

func escapes(fn func()) (escaped bool) {
	defer func() { escaped = recover() != nil }()
	fn()
	return false
}

func TestSubsystemPanicPassesOuterBoundaries(t *testing.T) {
	Reset()
	defer Reset()
	nested := func() {
		Run(Connection, "test/connection.go:handler", nil, func() { Run(Subsystem, "test/service.go:run", nil, crashingService) })
	}
	if !escapes(nested) {
		t.Fatal("connection boundary swallowed a subsystem panic")
	}
	if _, _, broken := Snapshot(0); len(broken) != 0 || !Allowed(Subsystem, "test/service.go:run", nil) {
		t.Fatalf("subsystem marked broken %v", broken)
	}
	if !escapes(func() { _ = Error(Subsystem, "test/service.go:periodic", func() error { panic("cleanup") }) }) {
		t.Fatal("subsystem error boundary swallowed a panic")
	}
}

func TestRepeatedConnectionPanicEndsProcess(t *testing.T) {
	Reset()
	defer Reset()
	defer func(saved func() time.Time) { now = saved }(now)
	clock := time.Unix(1000, 0)
	now = func() time.Time { return clock }
	panicAt := func(where string) bool {
		return escapes(func() { Run(Connection, where, nil, func() { panic("parse") }) })
	}
	for i := 1; i < connectionPanicLimit; i++ {
		if panicAt("test/connection.go:handler") {
			t.Fatalf("escalated after %d panics", i)
		}
		clock = clock.Add(time.Second)
	}
	if panicAt("test/connection.go:other") {
		t.Fatal("unrelated site escalated")
	}
	if panicAt("app/observatory/health/probe.go:expire") {
		t.Fatal("observer site escalated")
	}
	if !panicAt("test/connection.go:handler") {
		t.Fatalf("no escalation after %d panics in a minute", connectionPanicLimit)
	}

	Reset()
	for i := 0; i < 3*connectionPanicLimit; i++ {
		if panicAt("test/connection.go:handler") {
			t.Fatalf("escalated at %d with panics spread over time", i)
		}
		clock = clock.Add(connectionPanicWindow / connectionPanicLimit * 2)
	}
}

func TestRecordGoesToCoreLog(t *testing.T) {
	Reset()
	defer Reset()
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	var got []string
	SetLog(func(message string) { got = append(got, message) })
	defer SetLog(nil)
	Run(Connection, "test/connection.go:handler", nil, func() { panic("secret.example") })
	if len(got) != 1 || !strings.HasPrefix(got[0], "panic_recovered scope=connection where=test/connection.go:handler") || strings.Contains(got[0], "secret") {
		t.Fatalf("%q", got)
	}
	if logs.Len() != 0 {
		t.Fatalf("stderr copy %q", logs.String())
	}
}

func TestEscalatedRecordIsWrittenBeforeExit(t *testing.T) {
	Reset()
	defer Reset()
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	SetLog(func(string) {})
	defer SetLog(nil)
	if !escapes(func() { Run(Subsystem, "test/service.go:run", nil, crashingService) }) {
		t.Fatal("subsystem panic was contained")
	}
	if !strings.Contains(logs.String(), "panic_recovered scope=subsystem where=test/service.go:run") {
		t.Fatalf("stderr %q", logs.String())
	}
}

func TestSubsystemStopsAndCountIsConcurrent(t *testing.T) {
	Reset()
	defer Reset()
	var wg sync.WaitGroup
	for i := range 20 {
		where := "test/service" + string(rune('a'+i)) + ".go:run"
		wg.Go(func() { Run(Connection, where, nil, func() { panic("service") }) })
	}
	wg.Wait()
	Run(Subsystem, "app/observatory/health/health.go:o.loop", nil, func() { panic("service") })
	n, _, broken := Snapshot(0)
	if n != 21 || len(broken) != 1 {
		t.Fatalf("%d %v", n, broken)
	}
	ran := false
	Run(Subsystem, "app/observatory/health/health.go:o.loop", nil, func() { ran = true })
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
	Run(Subsystem, "app/observatory/health/health.go:o.loop", nil, func() { panic("one") })
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
