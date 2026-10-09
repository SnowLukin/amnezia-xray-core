package wireguard

import (
	"context"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features/outbound"
)

// TestMeasureClients logs what client outbounds hold over their life: idle,
// after one connection, asleep, awake again and closed.
func TestMeasureClients(t *testing.T) {
	if os.Getenv("AWG_MEASURE") != "1" {
		t.Skip("set AWG_MEASURE=1 to measure client outbounds")
	}
	const n = 3

	privs := make([]string, n)
	addrs := make([]netip.Addr, n)
	peers := make([]testPeer, n)
	for i := range n {
		var pub string
		privs[i], pub = testKeys(t)
		addrs[i] = netip.AddrFrom4([4]byte{10, 9, 0, byte(2 + i)})
		peers[i] = testPeer{pub, addrs[i]}
	}
	s := startTestServer(t, testParams, peers...)
	inst := newTestInstance(t)

	var first runtime.MemStats
	measure := func(label string) {
		// Stopped stacks and devices end some goroutines asynchronously.
		for range 3 {
			runtime.GC()
			time.Sleep(300 * time.Millisecond)
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if first.HeapInuse == 0 {
			first = m
		}
		kib := func(v, base uint64) string {
			return strconv.FormatFloat(float64(int64(v)-int64(base))/1024, 'f', 0, 64)
		}
		t.Logf("%-26s goroutines %4d  HeapInuse %+7s KiB  HeapAlloc %+7s KiB  StackInuse %+6s KiB",
			label, runtime.NumGoroutine(),
			kib(m.HeapInuse, first.HeapInuse), kib(m.HeapAlloc, first.HeapAlloc), kib(m.StackInuse, first.StackInuse))
	}

	// The first log line starts a writer goroutine; count it from the start.
	errors.LogWarning(context.Background(), "measuring ", n, " outbounds")
	t.Logf("NumCPU %d, GOMAXPROCS %d, %d outbounds; KiB relative to the first line", runtime.NumCPU(), runtime.GOMAXPROCS(0), n)
	measure("before the outbounds")
	ohs := make([]outbound.Handler, n)
	hs := make([]*Handler, n)
	for i := range n {
		ohs[i], hs[i] = addTestClient(t, inst, "wg"+strconv.Itoa(i), s, privs[i], addrs[i], testParams)
	}
	measure("outbounds created")
	for _, oh := range ohs {
		roundTrip(t, oh, s)
	}
	measure("one connection each")

	// Not outbound.Sleeper: the same measurement runs on revisions without
	// Sleep to compare.
	type sleeper interface{ Sleep() bool }
	if _, ok := any(hs[0]).(sleeper); ok {
		for _, h := range hs {
			if !any(h).(sleeper).Sleep() {
				t.Fatal("an idle outbound refused to sleep")
			}
		}
		measure("asleep")
		time.Sleep(handshakeGap)
		for _, oh := range ohs {
			roundTrip(t, oh, s)
		}
		measure("awake, one connection each")
	}

	for _, h := range hs {
		h.Close()
	}
	measure("closed")
}
