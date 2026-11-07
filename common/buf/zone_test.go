//go:build unix

package buf

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func TestZoneNameOfInterface(t *testing.T) {
	ifis, err := net.Interfaces()
	if err != nil || len(ifis) == 0 {
		t.Skip("no interfaces: ", err)
	}
	for range 2 {
		if got := zoneName(ifis[0].Index); got != ifis[0].Name {
			t.Fatalf("zone %d is named %q, not %q", ifis[0].Index, got, ifis[0].Name)
		}
	}
	const missing = 1 << 30
	if got := zoneName(missing); got != strconv.Itoa(missing) {
		t.Fatalf("a zone without an interface is named %q", got)
	}
	if _, ok := zoneNames.Load(missing); ok {
		t.Fatal("a zone without an interface is kept as a name")
	}
}

func TestZoneNameIsLookedUpAgainAfterAMinute(t *testing.T) {
	ifis, err := net.Interfaces()
	if err != nil || len(ifis) == 0 {
		t.Skip("no interfaces: ", err)
	}
	index := ifis[0].Index
	zoneNames.Store(index, zoneNameEntry{"renamed", time.Now()})
	if got := zoneName(index); got != "renamed" {
		t.Fatalf("a name looked up just now is looked up again: %q", got)
	}
	zoneNames.Store(index, zoneNameEntry{"renamed", time.Now().Add(-2 * zoneNameTTL)})
	if got := zoneName(index); got != ifis[0].Name {
		t.Fatalf("a name looked up long ago stays %q instead of %q", got, ifis[0].Name)
	}
	const missing = 1 << 30
	zoneNames.Store(missing, zoneNameEntry{"gone", time.Now().Add(-2 * zoneNameTTL)})
	zoneName(missing)
	if _, ok := zoneNames.Load(missing); ok {
		t.Fatal("the name of an interface that is gone is kept")
	}
}
