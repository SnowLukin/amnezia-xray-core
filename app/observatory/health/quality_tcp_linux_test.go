package health

import (
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestQualityLinuxTCPInfoOffsets(t *testing.T) {
	var info unix.TCPInfo
	for name, offsets := range map[string][2]uintptr{
		"Rtt":           {unsafe.Offsetof(info.Rtt), qualityLinuxRTTOffset},
		"Rcv_rtt":       {unsafe.Offsetof(info.Rcv_rtt), qualityLinuxLoadRTTOffset},
		"Min_rtt":       {unsafe.Offsetof(info.Min_rtt), qualityLinuxMinRTTOffset},
		"Data_segs_in":  {unsafe.Offsetof(info.Data_segs_in), qualityLinuxRXTotalOffset},
		"Bytes_sent":    {unsafe.Offsetof(info.Bytes_sent), qualityLinuxTXTotalOffset},
		"Bytes_retrans": {unsafe.Offsetof(info.Bytes_retrans), qualityLinuxTXRetransOffset},
		"Rcv_ooopack":   {unsafe.Offsetof(info.Rcv_ooopack), qualityLinuxRXOOOOffset},
	} {
		if offsets[0] != offsets[1] {
			t.Errorf("%s offset = %d, want %d", name, offsets[0], offsets[1])
		}
	}
}
