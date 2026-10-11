package health

import (
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestQualityDarwinTCPInfoOffsets(t *testing.T) {
	var info unix.TCPConnectionInfo
	for name, offsets := range map[string][2]uintptr{
		"Srtt":              {unsafe.Offsetof(info.Srtt), qualityDarwinRTTOffset},
		"Txbytes":           {unsafe.Offsetof(info.Txbytes), qualityDarwinTXTotalOffset},
		"Txretransmitbytes": {unsafe.Offsetof(info.Txretransmitbytes), qualityDarwinTXRetransOffset},
		"Rxbytes":           {unsafe.Offsetof(info.Rxbytes), qualityDarwinRXTotalOffset},
		"Rxoutoforderbytes": {unsafe.Offsetof(info.Rxoutoforderbytes), qualityDarwinRXOOOOffset},
	} {
		if offsets[0] != offsets[1] {
			t.Errorf("%s offset = %d, want %d", name, offsets[0], offsets[1])
		}
	}
}
