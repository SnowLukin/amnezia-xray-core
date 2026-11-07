//go:build unix

package buf

import (
	"net"
	"strconv"
	"testing"
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
