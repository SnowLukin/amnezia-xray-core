package health

import (
	"encoding/binary"
	"time"
)

const (
	qualityHasRTT uint8 = 1 << iota
	qualityHasMinRTT
	qualityHasLoadRTT
	qualityHasRX
	qualityHasTX
	qualityHasRXBytes
	qualityHasTXAcked
)

// tcpQualitySnapshot keeps presence separate from value: no retransmissions is
// a known zero, whereas a field beyond the returned getsockopt length is absent.
// RX counters are packets on Linux and bytes on Darwin; TX counters are bytes.
// rxBytes is the payload the kernel received, as the network delivered it;
// txAcked is the payload the network delivered the other way.
type tcpQualitySnapshot struct {
	rtt, minRTT, loadRTT  time.Duration
	rxOutOfOrder, rxTotal uint64
	txRetransmit, txTotal uint64
	rxBytes, txAcked      uint64
	present               uint8
}

// These offsets come from the public Linux tcp_info ABI. The uint64 sequence
// starts at offset 104, so the required offsets also agree on linux/386.
// https://github.com/torvalds/linux/blob/master/include/uapi/linux/tcp.h
const (
	qualityLinuxRTTOffset       = 68
	qualityLinuxLoadRTTOffset   = 92
	qualityLinuxTXAckedOffset   = 120
	qualityLinuxRXBytesOffset   = 128
	qualityLinuxMinRTTOffset    = 148
	qualityLinuxRXTotalOffset   = 152
	qualityLinuxTXTotalOffset   = 200
	qualityLinuxTXRetransOffset = 208
	qualityLinuxRXOOOOffset     = 224
)

// Darwin's public tcp_connection_info has a TFO bitfield at offset 52, before
// its aligned uint64 counters. Srtt is milliseconds, not Linux microseconds.
// https://github.com/apple-oss-distributions/xnu/blob/main/bsd/netinet/tcp.h
const (
	qualityDarwinRTTOffset       = 44
	qualityDarwinTXTotalOffset   = 64
	qualityDarwinTXRetransOffset = 72
	qualityDarwinRXTotalOffset   = 88
	qualityDarwinRXOOOOffset     = 96
)

// decodeLinuxTCPQuality reads only the prefix returned by getsockopt. Linux
// copies min(requested length, sizeof(tcp_info)); old kernels therefore keep
// early metrics available without inventing zero values for newer counters.
// https://github.com/torvalds/linux/blob/master/net/ipv4/tcp.c
func decodeLinuxTCPQuality(b []byte) tcpQualitySnapshot {
	var s tcpQualitySnapshot
	if len(b) >= qualityLinuxRTTOffset+4 {
		s.rtt = time.Duration(binary.NativeEndian.Uint32(b[qualityLinuxRTTOffset:])) * time.Microsecond
		s.present |= qualityHasRTT
	}
	if len(b) >= qualityLinuxMinRTTOffset+4 {
		v := binary.NativeEndian.Uint32(b[qualityLinuxMinRTTOffset:])
		if v != ^uint32(0) {
			s.minRTT = time.Duration(v) * time.Microsecond
			s.present |= qualityHasMinRTT
		}
	}
	if len(b) >= qualityLinuxLoadRTTOffset+4 {
		v := binary.NativeEndian.Uint32(b[qualityLinuxLoadRTTOffset:])
		if v != 0 {
			s.loadRTT = time.Duration(v) * time.Microsecond
			s.present |= qualityHasLoadRTT
		}
	}
	// Bytes_acked and Bytes_received arrived in Linux 4.1, Bytes_sent and
	// Bytes_retrans in 4.19, Rcv_ooopack in 5.4. Check returned field
	// boundaries, because vendor kernels may backport them.
	if len(b) >= qualityLinuxTXAckedOffset+8 {
		s.txAcked = binary.NativeEndian.Uint64(b[qualityLinuxTXAckedOffset:])
		s.present |= qualityHasTXAcked
	}
	if len(b) >= qualityLinuxRXBytesOffset+8 {
		s.rxBytes = binary.NativeEndian.Uint64(b[qualityLinuxRXBytesOffset:])
		s.present |= qualityHasRXBytes
	}
	if len(b) >= qualityLinuxRXOOOOffset+4 && len(b) >= qualityLinuxRXTotalOffset+4 {
		s.rxOutOfOrder = uint64(binary.NativeEndian.Uint32(b[qualityLinuxRXOOOOffset:]))
		s.rxTotal = uint64(binary.NativeEndian.Uint32(b[qualityLinuxRXTotalOffset:]))
		s.present |= qualityHasRX
	}
	if len(b) >= qualityLinuxTXRetransOffset+8 && len(b) >= qualityLinuxTXTotalOffset+8 {
		s.txRetransmit = binary.NativeEndian.Uint64(b[qualityLinuxTXRetransOffset:])
		s.txTotal = binary.NativeEndian.Uint64(b[qualityLinuxTXTotalOffset:])
		s.present |= qualityHasTX
	}
	return s
}

func decodeDarwinTCPQuality(b []byte) tcpQualitySnapshot {
	var s tcpQualitySnapshot
	if len(b) >= qualityDarwinRTTOffset+4 {
		s.rtt = time.Duration(binary.NativeEndian.Uint32(b[qualityDarwinRTTOffset:])) * time.Millisecond
		// The caller uses minRTT only from S1 and loadRTT only from S2.
		s.minRTT, s.loadRTT = s.rtt, s.rtt
		s.present |= qualityHasRTT | qualityHasMinRTT | qualityHasLoadRTT
	}
	if len(b) >= qualityDarwinRXOOOOffset+8 && len(b) >= qualityDarwinRXTotalOffset+8 {
		s.rxOutOfOrder = binary.NativeEndian.Uint64(b[qualityDarwinRXOOOOffset:])
		s.rxTotal = binary.NativeEndian.Uint64(b[qualityDarwinRXTotalOffset:])
		s.rxBytes = s.rxTotal
		s.present |= qualityHasRX | qualityHasRXBytes
	}
	if len(b) >= qualityDarwinTXRetransOffset+8 && len(b) >= qualityDarwinTXTotalOffset+8 {
		s.txRetransmit = binary.NativeEndian.Uint64(b[qualityDarwinTXRetransOffset:])
		s.txTotal = binary.NativeEndian.Uint64(b[qualityDarwinTXTotalOffset:])
		s.present |= qualityHasTX
		// Darwin counts no acknowledged bytes. Bytes sent once moved at the
		// pace the congestion window allowed, which is what a peak needs.
		if s.txTotal >= s.txRetransmit {
			s.txAcked = s.txTotal - s.txRetransmit
			s.present |= qualityHasTXAcked
		}
	}
	return s
}
