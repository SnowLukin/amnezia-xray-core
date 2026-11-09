package health

import "time"

// requestQuality has the same measured values and presence as qualityPathView,
// with protocol-1 request names. Each projection owns its scalar pointers.
// Histogram buckets stay in the report: they serve sums over many reports,
// and every outbound of a provider request would pay for them.
type requestQuality struct {
	BytesDown              *int64   `json:"bytesDown,omitempty"`
	BytesDownOverflow      bool     `json:"bytesDownOverflow,omitempty"`
	TTFBCount              uint64   `json:"ttfbCount,omitempty"`
	TTFBP50Ms              *int64   `json:"ttfbP50Ms,omitempty"`
	TTFBP90Ms              *int64   `json:"ttfbP90Ms,omitempty"`
	DialCount              uint64   `json:"dialCount,omitempty"`
	DialP50Ms              *int64   `json:"dialP50Ms,omitempty"`
	DialP90Ms              *int64   `json:"dialP90Ms,omitempty"`
	TCPCount               uint64   `json:"tcpCount,omitempty"`
	TCPRTTP50Ms            *int64   `json:"tcpRttP50Ms,omitempty"`
	TCPRTTP90Ms            *int64   `json:"tcpRttP90Ms,omitempty"`
	TCPRTTMinMs            *int64   `json:"tcpRttMinMs,omitempty"`
	BulkCount              uint64   `json:"bulkCount,omitempty"`
	BulkKbpsP50            *int64   `json:"bulkKbpsP50,omitempty"`
	BulkKbpsP90            *int64   `json:"bulkKbpsP90,omitempty"`
	BulkKbpsMax            *int64   `json:"bulkKbpsMax,omitempty"`
	BulkPeakCount          uint64   `json:"bulkPeakCount,omitempty"`
	BulkPeakKbpsP50        *int64   `json:"bulkPeakKbpsP50,omitempty"`
	BulkPeakKbpsP90        *int64   `json:"bulkPeakKbpsP90,omitempty"`
	BulkSaturatedCount     *uint64  `json:"bulkSaturatedCount,omitempty"`
	BulkUnsaturatedCount   *uint64  `json:"bulkUnsaturatedCount,omitempty"`
	BulkSaturatedKbpsP50   *int64   `json:"bulkSaturatedKbpsP50,omitempty"`
	BulkSaturatedKbpsP90   *int64   `json:"bulkSaturatedKbpsP90,omitempty"`
	BulkPolicedCount       *uint64  `json:"bulkPolicedCount,omitempty"`
	BulkPolicedKbpsP50     *int64   `json:"bulkPolicedKbpsP50,omitempty"`
	BulkPolicedKbpsP90     *int64   `json:"bulkPolicedKbpsP90,omitempty"`
	UploadPeakCount        uint64   `json:"uploadPeakCount,omitempty"`
	UploadPeakKbpsP50      *int64   `json:"uploadPeakKbpsP50,omitempty"`
	UploadPeakKbpsP90      *int64   `json:"uploadPeakKbpsP90,omitempty"`
	UploadSaturatedCount   *uint64  `json:"uploadSaturatedCount,omitempty"`
	UploadUnsaturatedCount *uint64  `json:"uploadUnsaturatedCount,omitempty"`
	UploadSaturatedKbpsP50 *int64   `json:"uploadSaturatedKbpsP50,omitempty"`
	UploadSaturatedKbpsP90 *int64   `json:"uploadSaturatedKbpsP90,omitempty"`
	UploadPolicedCount     *uint64  `json:"uploadPolicedCount,omitempty"`
	UploadPolicedKbpsP50   *int64   `json:"uploadPolicedKbpsP50,omitempty"`
	UploadPolicedKbpsP90   *int64   `json:"uploadPolicedKbpsP90,omitempty"`
	BulkRTTP50Ms           *int64   `json:"bulkRttP50Ms,omitempty"`
	BulkSplicedCount       *uint64  `json:"bulkSplicedCount,omitempty"`
	RXOutOfOrderFraction   *float64 `json:"rxOutOfOrderFraction,omitempty"`
	TXRetransmitFraction   *float64 `json:"txRetransmitFraction,omitempty"`
	SpeedLimitDownKbps     *int64   `json:"speedLimitDownKbps,omitempty"`
	SpeedLimitUpKbps       *int64   `json:"speedLimitUpKbps,omitempty"`
}

func requestQualityOf(v qualityPathView) *requestQuality {
	v = cloneQualityPath(v)
	return &requestQuality{
		BytesDown: v.BytesDown, BytesDownOverflow: v.BytesDownOverflow,
		TTFBCount: v.TTFBCount, TTFBP50Ms: v.TTFBP50Ms, TTFBP90Ms: v.TTFBP90Ms,
		DialCount: v.DialCount, DialP50Ms: v.DialP50Ms, DialP90Ms: v.DialP90Ms,
		TCPCount: v.TCPCount, TCPRTTP50Ms: v.TCPRTTP50Ms, TCPRTTP90Ms: v.TCPRTTP90Ms, TCPRTTMinMs: v.TCPRTTMinMs,
		BulkCount: v.BulkCount, BulkKbpsP50: v.BulkKbpsP50, BulkKbpsP90: v.BulkKbpsP90, BulkKbpsMax: v.BulkKbpsMax,
		BulkPeakCount: v.BulkPeakCount, BulkPeakKbpsP50: v.BulkPeakKbpsP50, BulkPeakKbpsP90: v.BulkPeakKbpsP90,
		BulkSaturatedCount: v.BulkSaturatedCount, BulkUnsaturatedCount: v.BulkUnsaturatedCount,
		BulkSaturatedKbpsP50: v.BulkSaturatedKbpsP50, BulkSaturatedKbpsP90: v.BulkSaturatedKbpsP90,
		BulkPolicedCount: v.BulkPolicedCount, BulkPolicedKbpsP50: v.BulkPolicedKbpsP50, BulkPolicedKbpsP90: v.BulkPolicedKbpsP90,
		UploadPeakCount: v.UploadPeakCount, UploadPeakKbpsP50: v.UploadPeakKbpsP50, UploadPeakKbpsP90: v.UploadPeakKbpsP90,
		UploadSaturatedCount: v.UploadSaturatedCount, UploadUnsaturatedCount: v.UploadUnsaturatedCount,
		UploadSaturatedKbpsP50: v.UploadSaturatedKbpsP50, UploadSaturatedKbpsP90: v.UploadSaturatedKbpsP90,
		UploadPolicedCount: v.UploadPolicedCount, UploadPolicedKbpsP50: v.UploadPolicedKbpsP50, UploadPolicedKbpsP90: v.UploadPolicedKbpsP90,
		BulkRTTP50Ms: v.BulkRTTP50Ms, BulkSplicedCount: v.BulkSplicedCount,
		RXOutOfOrderFraction: v.RXOutOfOrderFraction, TXRetransmitFraction: v.TXRetransmitFraction,
		SpeedLimitDownKbps: v.SpeedLimitDownKbps, SpeedLimitUpKbps: v.SpeedLimitUpKbps,
	}
}

// Projection reads only. In particular, it never creates a policy path for a
// measurement whose logical tag has been retired or forgotten by the engine.
func (e *engine) currentQuality(now time.Time) qualitySegmentView {
	if e.quality != nil {
		return e.quality.currentView(now)
	}
	return qualitySegmentView{IsNetworkKnown: e.networkKnown}
}
