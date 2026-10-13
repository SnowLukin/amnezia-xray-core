//go:build resilience_test

package safe

import (
	"os"
	"sync/atomic"
)

var injected atomic.Bool

// TestPanic is only available in the isolated resilience_test binary.
func TestPanic(site, target string) {
	if os.Getenv("XRAY_RESILIENCE_PANIC") == site+":"+target && injected.CompareAndSwap(false, true) {
		panic("resilience test injection")
	}
}
