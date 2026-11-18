package buf_test

import (
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"testing"
	"time"
)

// heapObjects is the heap that live objects take, after what is garbage is gone
func heapObjects() uint64 {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

// idleCost is how much of the heap each of n readers that wait takes, with start making them wait
func idleCost(t *testing.T, n int, start func(i int)) float64 {
	t.Helper()
	before := heapObjects()
	for i := range n {
		start(i)
	}
	time.Sleep(300 * time.Millisecond)
	return (float64(heapObjects()) - float64(before)) / float64(n)
}
