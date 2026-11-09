//go:build resilience_test

package safe

import (
	"os"
	"sync/atomic"
)

const TestPanicEnabled = true

var injected atomic.Bool

// TestPanic is only available in the isolated resilience_test binary. It fires
// once, or on every match when XRAY_RESILIENCE_PANIC_REPEAT is set.
func TestPanic(site, target string) {
	if os.Getenv("XRAY_RESILIENCE_PANIC") != site+":"+target {
		return
	}
	if os.Getenv("XRAY_RESILIENCE_PANIC_REPEAT") != "" || injected.CompareAndSwap(false, true) {
		panic("resilience test injection")
	}
}
