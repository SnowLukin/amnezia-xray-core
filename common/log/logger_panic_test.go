package log_test

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/xtls/xray-core/common/log"
)

type panicWriter struct{}

func (panicWriter) Write(string) error { panic("log writer") }
func (panicWriter) Close() error       { return nil }

func TestLoggerPanicEndsProcess(t *testing.T) {
	if os.Getenv("LOG_TEST_CHILD") != "" {
		logger := NewLogger(func() Writer { return panicWriter{} })
		logger.Handle(&GeneralMessage{Severity: Severity_Error, Content: "first"})
		time.Sleep(2 * time.Second)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLoggerPanicEndsProcess$", "-test.count=1")
	cmd.Env = append(os.Environ(), "LOG_TEST_CHILD=1")
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 2 || !strings.Contains(string(out), "panic: log writer") {
		t.Fatalf("%v\n%s", err, out)
	}
}
