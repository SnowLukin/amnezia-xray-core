package health

import "time"

// requestQuality has the same measured values and presence as qualityPathView,
// with protocol-1 request names. Each projection owns its scalar pointers.
type requestQuality struct {
	BytesDown            *int64   `json:"bytesDown,omitempty"`
	BytesDownOverflow    bool     `json:"bytesDownOverflow,omitempty"`
	TTFBCount            uint64   `json:"ttfbCount,omitempty"`
	TTFBP50Ms            *int64   `json:"ttfbP50Ms,omitempty"`
	TTFBP90Ms            *int64   `json:"ttfbP90Ms,omitempty"`
	DialCount            uint64   `json:"dialCount,omitempty"`
	DialP50Ms            *int64   `json:"dialP50Ms,omitempty"`
	DialP90Ms            *int64   `json:"dialP90Ms,omitempty"`
	TCPCount             uint64   `json:"tcpCount,omitempty"`
	TCPRTTP50Ms          *int64   `json:"tcpRttP50Ms,omitempty"`
	TCPRTTP90Ms          *int64   `json:"tcpRttP90Ms,omitempty"`
	TCPRTTMinMs          *int64   `json:"tcpRttMinMs,omitempty"`
	BulkCount            uint64   `json:"bulkCount,omitempty"`
	BulkKbpsP50          *int64   `json:"bulkKbpsP50,omitempty"`
	BulkKbpsP90          *int64   `json:"bulkKbpsP90,omitempty"`
	BulkKbpsMax          *int64   `json:"bulkKbpsMax,omitempty"`
	BulkRTTP50Ms         *int64   `json:"bulkRttP50Ms,omitempty"`
	BulkSplicedCount     *uint64  `json:"bulkSplicedCount,omitempty"`
	RXOutOfOrderFraction *float64 `json:"rxOutOfOrderFraction,omitempty"`
	TXRetransmitFraction *float64 `json:"txRetransmitFraction,omitempty"`
}

func requestQualityOf(v qualityPathView) *requestQuality {
	v = cloneQualityPath(v)
	return &requestQuality{
		BytesDown: v.BytesDown, BytesDownOverflow: v.BytesDownOverflow,
		TTFBCount: v.TTFBCount, TTFBP50Ms: v.TTFBP50Ms, TTFBP90Ms: v.TTFBP90Ms,
		DialCount: v.DialCount, DialP50Ms: v.DialP50Ms, DialP90Ms: v.DialP90Ms,
		TCPCount: v.TCPCount, TCPRTTP50Ms: v.TCPRTTP50Ms, TCPRTTP90Ms: v.TCPRTTP90Ms, TCPRTTMinMs: v.TCPRTTMinMs,
		BulkCount: v.BulkCount, BulkKbpsP50: v.BulkKbpsP50, BulkKbpsP90: v.BulkKbpsP90, BulkKbpsMax: v.BulkKbpsMax,
		BulkRTTP50Ms: v.BulkRTTP50Ms, BulkSplicedCount: v.BulkSplicedCount,
		RXOutOfOrderFraction: v.RXOutOfOrderFraction, TXRetransmitFraction: v.TXRetransmitFraction,
	}
}

// Projection reads only. In particular, it never creates a policy path for a
// measurement whose logical tag has been retired or forgotten by the engine.
func (e *engine) currentQuality(now time.Time) qualitySegmentView {
	if e.quality != nil {
		return e.quality.currentView(now)
	}
	typ, operator := parseQualityNetwork(e.networkID)
	return qualitySegmentView{NetworkType: typ, NetworkOperator: operator, IsNetworkKnown: e.networkKnown}
}
