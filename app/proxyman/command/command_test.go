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

// Closing an outbound closes its proxy, which ends the connections still
// going through it, such as those inside a WireGuard tunnel. The API only
// takes the outbound out of routing and leaves them to finish.
func TestRemoveOutboundLeavesItOpen(t *testing.T) {
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
	if m.GetHandler("gone") != nil || m.GetHandler("kept") != kept {
		t.Fatal("the removal did not take exactly the requested outbound out of the manager")
	}
	if gone.closed != 0 || kept.closed != 0 {
		t.Fatalf("closed by the removal: removed %d times, other %d times", gone.closed, kept.closed)
	}
	if _, err := s.RemoveOutbound(ctx, &RemoveOutboundRequest{}); err == nil {
		t.Fatal("an empty tag removed")
	}
}
