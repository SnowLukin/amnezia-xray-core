//go:build !resilience_test

package safe

// TestPanic is compiled away from production builds.
func TestPanic(site, target string) {}
