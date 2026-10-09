package command

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	proxymanOutbound "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/transport"
)

// countingHandler is an outbound that counts how often it was closed.
type countingHandler struct {
	tag    string
	closed int
}

func (h *countingHandler) Start() error                              { return nil }
func (h *countingHandler) Close() error                              { h.closed++; return nil }
func (h *countingHandler) Tag() string                               { return h.tag }
func (h *countingHandler) Dispatch(context.Context, *transport.Link) {}
func (h *countingHandler) SenderSettings() *serial.TypedMessage      { return nil }
func (h *countingHandler) ProxySettings() *serial.TypedMessage       { return nil }

// An outbound removed through the API is closed, as an inbound is: the
// outbound manager only forgets it, and a WireGuard outbound would keep its
// tunnel and its goroutines. The other outbounds stay as they are, and a tag
// without an outbound closes nothing.
func TestRemoveOutboundClosesIt(t *testing.T) {
	ctx := context.Background()
	m, err := proxymanOutbound.New(ctx, &proxyman.OutboundConfig{})
	if err != nil {
		t.Fatal(err)
	}
	gone, kept := &countingHandler{tag: "gone"}, &countingHandler{tag: "kept"}
	for _, h := range []*countingHandler{gone, kept} {
		if err := m.AddHandler(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
	s := &handlerServer{ohm: m}
	if _, err := s.RemoveOutbound(ctx, &RemoveOutboundRequest{Tag: "gone"}); err != nil {
		t.Fatal(err)
	}
	if m.GetHandler("gone") != nil || gone.closed != 1 || kept.closed != 0 {
		t.Fatalf("after the removal: registered %v, removed closed %d times, other closed %d times",
			m.GetHandler("gone") != nil, gone.closed, kept.closed)
	}
	if _, err := s.RemoveOutbound(ctx, &RemoveOutboundRequest{Tag: "gone"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RemoveOutbound(ctx, &RemoveOutboundRequest{}); err == nil {
		t.Fatal("an empty tag removed")
	}
	if gone.closed != 1 || kept.closed != 0 || m.GetHandler("kept") != kept {
		t.Fatalf("after removing it again: closed %d and %d times", gone.closed, kept.closed)
	}
}
