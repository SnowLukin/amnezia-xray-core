package health

import (
	"encoding/binary"
	"strconv"
	"testing"
	"time"
)

func qualityLinuxFixture() []byte {
	b := make([]byte, 280)
	binary.NativeEndian.PutUint32(b[qualityLinuxRTTOffset:], 123456)
	binary.NativeEndian.PutUint32(b[qualityLinuxMinRTTOffset:], 40000)
	binary.NativeEndian.PutUint32(b[qualityLinuxLoadRTTOffset:], 654321)
	binary.NativeEndian.PutUint64(b[qualityLinuxRXBytesOffset:], 3<<30)
	binary.NativeEndian.PutUint64(b[qualityLinuxTXAckedOffset:], 5<<30)
	binary.NativeEndian.PutUint32(b[qualityLinuxRXTotalOffset:], 1024)
	binary.NativeEndian.PutUint32(b[qualityLinuxRXOOOOffset:], 7)
	binary.NativeEndian.PutUint64(b[qualityLinuxTXTotalOffset:], 1<<40)
	binary.NativeEndian.PutUint64(b[qualityLinuxTXRetransOffset:], 1<<33)
	return b
}

func qualityDarwinFixture() []byte {
	b := make([]byte, 112)
	binary.NativeEndian.PutUint32(b[qualityDarwinRTTOffset:], 123)
	binary.NativeEndian.PutUint64(b[qualityDarwinRXTotalOffset:], 1<<42)
	binary.NativeEndian.PutUint64(b[qualityDarwinRXOOOOffset:], 1<<32)
	binary.NativeEndian.PutUint64(b[qualityDarwinTXTotalOffset:], 1<<40)
	binary.NativeEndian.PutUint64(b[qualityDarwinTXRetransOffset:], 1<<33)
	return b
}

func TestDecodeLinuxTCPQuality(t *testing.T) {
	want := tcpQualitySnapshot{
		rtt:          123456 * time.Microsecond,
		minRTT:       40000 * time.Microsecond,
		loadRTT:      654321 * time.Microsecond,
		rxOutOfOrder: 7,
		rxTotal:      1024,
		txRetransmit: 1 << 33,
		txTotal:      1 << 40,
		rxBytes:      3 << 30,
		txAcked:      5 << 30,
		present:      qualityHasRTT | qualityHasMinRTT | qualityHasLoadRTT | qualityHasRX | qualityHasTX | qualityHasRXBytes | qualityHasTXAcked,
	}
	if got := decodeLinuxTCPQuality(qualityLinuxFixture()); got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

func TestDecodeLinuxTCPQualityShortStructure(t *testing.T) {
	b := qualityLinuxFixture()
	for _, n := range []int{0, 71, 72, 95, 96, 104, 127, 128, 135, 136, 151, 152, 155, 156, 160, 207, 208, 215, 216, 224, 227, 228} {
		t.Run(qualityLengthName(n), func(t *testing.T) {
			got := decodeLinuxTCPQuality(b[:n])
			var want uint8
			if n >= 72 {
				want |= qualityHasRTT
			}
			if n >= 96 {
				want |= qualityHasLoadRTT
			}
			if n >= 128 {
				want |= qualityHasTXAcked
			}
			if n >= 136 {
				want |= qualityHasRXBytes
			}
			if n >= 152 {
				want |= qualityHasMinRTT
			}
			if n >= 216 {
				want |= qualityHasTX
			}
			if n >= 228 {
				want |= qualityHasRX
			}
			if got.present != want {
				t.Fatalf("presence = %07b, want %07b at returned length %d", got.present, want, n)
			}
			if want&qualityHasRX == 0 && (got.rxTotal != 0 || got.rxOutOfOrder != 0) {
				t.Fatalf("incomplete RX pair was decoded: %+v", got)
			}
			if want&qualityHasTX == 0 && (got.txTotal != 0 || got.txRetransmit != 0) {
				t.Fatalf("incomplete TX pair was decoded: %+v", got)
			}
			if want&qualityHasRXBytes == 0 && got.rxBytes != 0 || want&qualityHasTXAcked == 0 && got.txAcked != 0 {
				t.Fatalf("incomplete byte counters were decoded: %+v", got)
			}
		})
	}
}

func TestDecodeLinuxTCPQualityZeroAndAbsent(t *testing.T) {
	b := make([]byte, 280)
	want := qualityHasRTT | qualityHasMinRTT | qualityHasRX | qualityHasTX | qualityHasRXBytes | qualityHasTXAcked
	if got := decodeLinuxTCPQuality(b); got.present != want || got.txRetransmit != 0 || got.rxOutOfOrder != 0 {
		t.Fatalf("known zero counters or absent receiver RTT were lost: %+v", got)
	}
	if got := decodeLinuxTCPQuality(nil); got != (tcpQualitySnapshot{}) {
		t.Fatalf("empty structure has known values: %+v", got)
	}
	binary.NativeEndian.PutUint32(b[qualityLinuxMinRTTOffset:], ^uint32(0))
	if got := decodeLinuxTCPQuality(b); got.present != want&^qualityHasMinRTT || got.minRTT != 0 {
		t.Fatalf("unknown Min_rtt sentinel was decoded: %+v", got)
	}
	binary.NativeEndian.PutUint32(b[qualityLinuxLoadRTTOffset:], 1)
	if got := decodeLinuxTCPQuality(b); got.present&qualityHasLoadRTT == 0 || got.loadRTT != time.Microsecond {
		t.Fatalf("receiver RTT sample was not decoded: %+v", got)
	}
}

func TestDecodeDarwinTCPQuality(t *testing.T) {
	want := tcpQualitySnapshot{
		rtt:          123 * time.Millisecond,
		minRTT:       123 * time.Millisecond,
		loadRTT:      123 * time.Millisecond,
		rxOutOfOrder: 1 << 32,
		rxTotal:      1 << 42,
		txRetransmit: 1 << 33,
		txTotal:      1 << 40,
		rxBytes:      1 << 42,
		txAcked:      1<<40 - 1<<33,
		present:      qualityHasRTT | qualityHasMinRTT | qualityHasLoadRTT | qualityHasRX | qualityHasTX | qualityHasRXBytes | qualityHasTXAcked,
	}
	if got := decodeDarwinTCPQuality(qualityDarwinFixture()); got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

func TestDecodeDarwinTCPQualityShortStructure(t *testing.T) {
	b := qualityDarwinFixture()
	for _, n := range []int{0, 47, 48, 71, 72, 79, 80, 95, 96, 103, 104} {
		t.Run(qualityLengthName(n), func(t *testing.T) {
			got := decodeDarwinTCPQuality(b[:n])
			var want uint8
			if n >= 48 {
				want |= qualityHasRTT | qualityHasMinRTT | qualityHasLoadRTT
			}
			if n >= 80 {
				want |= qualityHasTX | qualityHasTXAcked
			}
			if n >= 104 {
				want |= qualityHasRX | qualityHasRXBytes
			}
			if got.present != want {
				t.Fatalf("presence = %07b, want %07b at returned length %d", got.present, want, n)
			}
			if want&qualityHasRX == 0 && (got.rxTotal != 0 || got.rxOutOfOrder != 0) {
				t.Fatalf("incomplete RX pair was decoded: %+v", got)
			}
			if want&qualityHasTX == 0 && (got.txTotal != 0 || got.txRetransmit != 0) {
				t.Fatalf("incomplete TX pair was decoded: %+v", got)
			}
		})
	}
}

func TestDecodeDarwinTCPQualityZeroAndUnits(t *testing.T) {
	b := make([]byte, 112)
	want := qualityHasRTT | qualityHasMinRTT | qualityHasLoadRTT | qualityHasRX | qualityHasTX | qualityHasRXBytes | qualityHasTXAcked
	if got := decodeDarwinTCPQuality(b); got.present != want || got.rtt != 0 || got.txRetransmit != 0 {
		t.Fatalf("known zero values were lost: %+v", got)
	}
	if got := decodeDarwinTCPQuality(nil); got != (tcpQualitySnapshot{}) {
		t.Fatalf("empty structure has known values: %+v", got)
	}
	// Conversion must happen in time.Duration, not uint32 microseconds.
	binary.NativeEndian.PutUint32(b[qualityDarwinRTTOffset:], ^uint32(0))
	if got := decodeDarwinTCPQuality(b); got.rtt != time.Duration(^uint32(0))*time.Millisecond {
		t.Fatalf("millisecond conversion overflowed: %+v", got)
	}
}

func qualityLengthName(n int) string {
	// Keep fixtures and their length tests independent of either OS package.
	return "length_" + strconv.Itoa(n)
}
