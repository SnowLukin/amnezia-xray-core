//go:build linux

package wireguard

import (
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/vishvananda/netlink"
)

// requireKernelTun skips tests that need to create kernel interfaces. Run
// them as root in a network namespace of their own.
func requireKernelTun(t *testing.T) {
	t.Helper()
	if ok, _ := KernelTunSupported(); !ok || os.Geteuid() != 0 {
		t.Skip("needs CAP_NET_ADMIN to create kernel TUN interfaces")
	}
}

func (h *Handler) kernelName() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ifName
}

func TestNewInterfaceNameSkipsKeptNames(t *testing.T) {
	createMu.Lock()
	a := newInterfaceNameLocked()
	b := newInterfaceNameLocked()
	createMu.Unlock()
	if a == b {
		t.Fatalf("two outbounds got the name %s", a)
	}
	if interfaceExists(a) || interfaceExists(b) {
		t.Fatalf("got the name of an existing interface: %s, %s", a, b)
	}
	releaseKernelTunName(a)
	releaseKernelTunName(b)
	createMu.Lock()
	defer createMu.Unlock()
	if len(keptNames) != 0 {
		t.Fatalf("released names are still kept: %v", keptNames)
	}
}

// TestKernelClientKeepsItsName wakes kernel TUN outbounds in turn. Each must
// come back under its own name, and take another only when an interface of
// the host took it meanwhile.
func TestKernelClientKeepsItsName(t *testing.T) {
	requireKernelTun(t)
	privA, pubA := testKeys(t)
	privB, pubB := testKeys(t)
	addrA, addrB := netip.MustParseAddr("10.9.0.2"), netip.MustParseAddr("10.9.0.3")
	s := startTestServer(t, testParams, testPeer{pubA, addrA}, testPeer{pubB, addrB})
	inst := newTestInstance(t)
	base := goroutineBase(t)
	confA := testClientConf(s, privA, addrA, testParams)
	confA.NoKernelTun = false
	confB := testClientConf(s, privB, addrB, testParams)
	confB.NoKernelTun = false
	ohA, a := addTestClientConf(t, inst, "a", confA)
	ohB, b := addTestClientConf(t, inst, "b", confB)

	roundTrip(t, ohA, s)
	roundTrip(t, ohB, s)
	nameA, nameB := a.kernelName(), b.kernelName()
	if nameA == "" || nameA == nameB {
		t.Fatalf("outbounds got the names %q and %q", nameA, nameB)
	}
	if !a.Sleep() || interfaceExists(nameA) {
		t.Fatal("Sleep left the interface")
	}
	// B wakes while the name of A is free on the host.
	if !b.Sleep() {
		t.Fatal("an idle outbound refused to sleep")
	}
	time.Sleep(handshakeGap)
	roundTrip(t, ohB, s)
	roundTrip(t, ohA, s)
	if a.kernelName() != nameA || b.kernelName() != nameB {
		t.Fatalf("names changed from %s, %s to %s, %s", nameA, nameB, a.kernelName(), b.kernelName())
	}

	// An interface of the host takes the name of A while A sleeps.
	if !a.Sleep() {
		t.Fatal("an idle outbound refused to sleep")
	}
	host := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: nameA}}
	if err := netlink.LinkAdd(host); err != nil {
		t.Fatal(err)
	}
	defer netlink.LinkDel(host)
	time.Sleep(handshakeGap)
	roundTrip(t, ohA, s)
	if n := a.kernelName(); n == nameA || n == nameB {
		t.Fatalf("A woke as %s", n)
	}

	a.Close()
	b.Close()
	createMu.Lock()
	kept := len(keptNames)
	createMu.Unlock()
	if kept != 0 {
		t.Fatalf("closed outbounds keep %d names", kept)
	}
	waitGoroutines(t, base)
}

// TestOpenKernelTunTakenName checks that a failed attempt on a taken name
// leaves no descriptor open.
func TestOpenKernelTunTakenName(t *testing.T) {
	requireKernelTun(t)
	createMu.Lock()
	name := newInterfaceNameLocked()
	createMu.Unlock()
	defer releaseKernelTunName(name)
	first, err := openKernelTun(name, 1420)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	fds := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	before := fds()
	for range 3 {
		if dup, err := openKernelTun(name, 1420); err == nil {
			dup.Close()
			t.Fatal("opened a TUN under the name of another")
		}
	}
	if after := fds(); after != before {
		t.Fatalf("%d descriptors open after failed attempts, %d before", after, before)
	}
}
