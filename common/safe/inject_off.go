//go:build !resilience_test

package safe

// TestPanicEnabled lets callers skip building TestPanic arguments in production.
const TestPanicEnabled = false

// TestPanic is compiled away from production builds.
func TestPanic(site, target string) {}
