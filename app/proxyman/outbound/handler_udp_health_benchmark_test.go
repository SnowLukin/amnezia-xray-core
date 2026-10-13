package outbound

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport"
)

// Keep the reference reader construction outside the loop's inlining boundary.
//go:noinline
func udpBenchmarkLink(packet *buf.Buffer) *transport.Link {
	return &transport.Link{Reader: &udpFixedReader{mb: buf.MultiBuffer{packet}}, Writer: &udpAcceptWriter{}}
}

// This measures the steady wrapper and health tracker cost without transport I/O.
func BenchmarkObservedUDPLinkRealHealthUplink(b *testing.B) {
	b.StopTimer()
	instance, err := core.New(&core.Config{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		b.StopTimer()
		if err := instance.Close(); err != nil {
			b.Error(err)
		}
	})
	object, err := core.CreateObject(instance, &health.Config{
		Paths: []*health.PathDefinition{{Tag: "bench", Balancer: "auto", Cost: 1}},
		Report: &health.ReportConfig{QuietShare: 1},
        Features: &health.FeatureConfig{Report: benchmarkBool(true), UdpReplies: benchmarkBool(true)},
	})
	if err != nil {
		b.Fatal(err)
	}
	observer, ok := object.(*health.Observer)
	if !ok || observer == nil {
		b.Fatalf("expected *health.Observer, got %T", object)
	}
	b.Cleanup(func() {
		b.StopTimer()
		if err := observer.Close(); err != nil {
			b.Error(err)
		}
	})
	flow := observer.StartUDPFlow(context.Background(), "bench", 53)
	if flow == nil {
		b.Fatal("StartUDPFlow returned nil for configured path bench")
	}
	packet := udpTestBuffer(strings.Repeat("x", 1200), 53)
	link := udpBenchmarkLink(packet)
	observeUDPLink(link, flow)
	b.Cleanup(func() {
		b.StopTimer()
		defer packet.Release()
		link.Reader.(interface{ Interrupt() }).Interrupt()
		if err := link.Writer.(interface{ Close() error }).Close(); err != nil {
			b.Error(err)
		}
		var report struct {
			Paths map[string]struct {
				Flows      int64 `json:"udp_flows"`
				Unanswered int64 `json:"udp_flows_unanswered"`
				BytesUp    int64 `json:"udp_bytes_up"`
				BytesDown  int64 `json:"udp_bytes_down"`
				Writes     int64 `json:"udp_writes"`
				Reads      int64 `json:"udp_reads"`
			} `json:"paths"`
		}
		if err := json.Unmarshal(observer.TakeReport(true), &report); err != nil {
			b.Fatal(err)
		}
		got, ok := report.Paths["bench"]
		writes := int64(b.N) + 1
		if !ok || got.Flows != 1 || got.Unanswered != 1 || got.BytesUp != writes*1200 ||
			got.Writes != writes || got.BytesDown != 0 || got.Reads != 0 {
			b.Fatalf("benchmark did not account for its measured loop and warmup: %+v", got)
		}
	})
	// The first uplink records time; only subsequent reads belong in the steady loop.
	if _, err := link.Reader.ReadMultiBuffer(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(packet.Len()))
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = link.Reader.ReadMultiBuffer()
	}
	b.StopTimer()
}

func BenchmarkUnobservedUDPLinkUplinkReference(b *testing.B) {
	b.StopTimer()
	packet := udpTestBuffer(strings.Repeat("x", 1200), 53)
	link := udpBenchmarkLink(packet)
	b.Cleanup(func() {
		b.StopTimer()
		link.Reader.(interface{ Interrupt() }).Interrupt()
		if err := link.Writer.(interface{ Close() error }).Close(); err != nil {
			b.Error(err)
		}
		packet.Release()
	})
	if _, err := link.Reader.ReadMultiBuffer(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(packet.Len()))
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = link.Reader.ReadMultiBuffer()
	}
	b.StopTimer()
}

func benchmarkBool(v bool) *bool { return &v }
