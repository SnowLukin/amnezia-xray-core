package health

import (
	"maps"
	"math"
	"math/bits"
	"sort"
	"strings"
	"time"
)

const (
	qualityCollectionPaths    = 64
	qualityCollectionTagBytes = 16 * 1024
	qualitySegmentPaths       = 4
	qualityReportSegments     = 4
)

var qualityTimeBounds = [...]time.Duration{
	10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond,
	50 * time.Millisecond, 60 * time.Millisecond, 80 * time.Millisecond, 100 * time.Millisecond,
	125 * time.Millisecond, 150 * time.Millisecond, 200 * time.Millisecond, 250 * time.Millisecond,
	300 * time.Millisecond, 400 * time.Millisecond, 500 * time.Millisecond, 750 * time.Millisecond,
	time.Second, 1500 * time.Millisecond, 2 * time.Second, 3 * time.Second, 5 * time.Second, 8 * time.Second,
}

// Decimal Kbit/s, including the decimal Mbit/s bounds in the specification.
var qualitySpeedBounds = [...]int64{
	128, 256, 512, 1000, 2000, 3000, 5000, 8000, 12000,
	20000, 30000, 50000, 80000, 120000, 200000, 300000, 500000,
}

type qualityTimeHistogram struct {
	counts [len(qualityTimeBounds) + 1]uint64
	total  uint64
}

func (h *qualityTimeHistogram) add(d time.Duration) {
	d = max(d, 0)
	i := sort.Search(len(qualityTimeBounds), func(i int) bool { return d <= qualityTimeBounds[i] })
	h.counts[i]++
	h.total++
}

func (h *qualityTimeHistogram) quantile(q float64) int64 {
	if h.total == 0 {
		return -1
	}
	i := qualityQuantileBucket(h.counts[:], h.total, q)
	return qualityTimeBounds[min(i, len(qualityTimeBounds)-1)].Milliseconds()
}

type qualitySpeedHistogram struct {
	counts [len(qualitySpeedBounds) + 1]uint64
	total  uint64
}

func (h *qualitySpeedHistogram) add(kbps int64) {
	kbps = max(kbps, 0)
	i := sort.Search(len(qualitySpeedBounds), func(i int) bool { return kbps <= qualitySpeedBounds[i] })
	h.counts[i]++
	h.total++
}

func (h *qualitySpeedHistogram) quantile(q float64) int64 {
	if h.total == 0 {
		return -1
	}
	i := qualityQuantileBucket(h.counts[:], h.total, q)
	return qualitySpeedBounds[min(i, len(qualitySpeedBounds)-1)]
}

// Match the legacy histogram: round(q*N), with rank at least one. The
// unbounded last bucket reports the last finite bound, not a made-up maximum.
// Callers only request a quantile when total is nonzero.
func qualityQuantileBucket(counts []uint64, total uint64, q float64) int {
	want := max(uint64(q*float64(total)+0.5), 1)
	var seen uint64
	for i, count := range counts {
		seen += count
		if seen >= want {
			return i
		}
	}
	return len(counts) - 1
}

// Socket counters are uint64 individually. Keep their sums in two words so
// combining connections does not wrap at the limit of one socket counter.
type qualityCounterSum struct {
	low, high uint64
}

func (s *qualityCounterSum) add(n uint64) {
	var carry uint64
	s.low, carry = bits.Add64(s.low, n, 0)
	s.high += carry
}

func (s qualityCounterSum) nonzero() bool { return s.low != 0 || s.high != 0 }

func (s qualityCounterSum) number() float64 {
	return math.Ldexp(float64(s.high), 64) + float64(s.low)
}

// qualityPathView is a scalar snapshot. Presence is separate from value for
// measurements that can honestly be zero. Counts accompany sampled histograms;
// a histogram without samples contributes neither its count nor its quantiles.
type qualityPathView struct {
	ProbeCount           uint64            `json:"probe_count,omitempty"`
	ProbeFailedCount     uint64            `json:"probe_failed_count,omitempty"`
	ProbeConnectP50Ms    *int64            `json:"probe_connect_p50_ms,omitempty"`
	ProbeReasons         map[string]uint64 `json:"probe_reasons,omitempty"`
	BytesDown            *int64            `json:"bytes_down,omitempty"`
	BytesDownOverflow    bool              `json:"bytes_down_overflow,omitempty"`
	TTFBCount            uint64            `json:"ttfb_count,omitempty"`
	TTFBP50Ms            *int64            `json:"ttfb_p50_ms,omitempty"`
	TTFBP90Ms            *int64            `json:"ttfb_p90_ms,omitempty"`
	DialCount            uint64            `json:"dial_count,omitempty"`
	DialP50Ms            *int64            `json:"dial_p50_ms,omitempty"`
	DialP90Ms            *int64            `json:"dial_p90_ms,omitempty"`
	TCPCount             uint64            `json:"tcp_count,omitempty"`
	TCPRTTP50Ms          *int64            `json:"tcp_rtt_p50_ms,omitempty"`
	TCPRTTP90Ms          *int64            `json:"tcp_rtt_p90_ms,omitempty"`
	TCPRTTMinMs          *int64            `json:"tcp_rtt_min_ms,omitempty"`
	BulkCount            uint64            `json:"bulk_count,omitempty"`
	BulkKbpsP50          *int64            `json:"bulk_kbps_p50,omitempty"`
	BulkKbpsP90          *int64            `json:"bulk_kbps_p90,omitempty"`
	BulkKbpsMax          *int64            `json:"bulk_kbps_max,omitempty"`
	BulkRTTP50Ms         *int64            `json:"bulk_rtt_p50_ms,omitempty"`
	BulkSplicedCount     *uint64           `json:"bulk_spliced_count,omitempty"`
	RXOutOfOrderFraction *float64          `json:"rx_out_of_order_fraction,omitempty"`
	TXRetransmitFraction *float64          `json:"tx_retransmit_fraction,omitempty"`
}

type qualitySegmentView struct {
	// Kept separately from JSON so path metadata is emitted once per report,
	// including measured paths whose provider handler has already been removed.
	pathLabels     map[string]map[string]string
	Network        *networkLabelsView         `json:"network,omitempty"`
	IsNetworkKnown bool                       `json:"is_network_known"`
	DurationMs     int64                      `json:"duration_ms"`
	Paths          map[string]qualityPathView `json:"paths,omitempty"`
	Witness        *qualityPathView           `json:"witness,omitempty"`
	// Only admitted measured tags outside Paths. When SamplesDropped is
	// positive, the number of additional distinct rejected tags is unknown.
	PathsDropped   int    `json:"paths_dropped,omitempty"`
	SamplesDropped uint64 `json:"samples_dropped,omitempty"`
}

type qualityAccumulator struct {
	labels                      map[string]string
	probeConnect                qualityTimeHistogram
	probes, probesFailed        uint64
	probeReasons                map[string]uint64
	ttfb, dial, tcpRTT, bulkRTT qualityTimeHistogram
	bulk                        qualitySpeedHistogram
	bytesDown                   int64
	minRTT                      time.Duration
	maxBulkKbps                 int64
	bulkSpliced                 uint64
	rxOutOfOrder, rxTotal       qualityCounterSum
	txRetransmit, txTotal       qualityCounterSum
	hasBytes, hasMinRTT         bool
	hasBulkSpliced              bool
	bytesDownOverflow           bool
}

func meaningfulQualityInitial(s qualityInitialSample) bool {
	return s.hasTTFB || s.hasDial || s.tcp.present&(qualityHasRTT|qualityHasMinRTT) != 0
}

func qualityRXDelta(s qualityEndSample) (outOfOrder, total uint64, ok bool) {
	if s.s1.present&qualityHasRX == 0 || s.s2.present&qualityHasRX == 0 ||
		s.s2.rxOutOfOrder < s.s1.rxOutOfOrder || s.s2.rxTotal < s.s1.rxTotal {
		// A reset/wrap cannot be distinguished here. Discard the whole pair
		// instead of interpreting a decreasing counter as a large delta.
		return 0, 0, false
	}
	return s.s2.rxOutOfOrder - s.s1.rxOutOfOrder, s.s2.rxTotal - s.s1.rxTotal, true
}

func meaningfulQualityEnd(s qualityEndSample) bool {
	if s.hasBytes || s.bytesDown > 0 || s.hasBulk || s.spliced || s.s2.present&qualityHasLoadRTT != 0 ||
		(s.lastTCP.present&qualityHasTX != 0 && s.lastTCP.txTotal > 0) {
		return true
	}
	_, total, ok := qualityRXDelta(s)
	return ok && total > 0
}

func (a *qualityAccumulator) addInitial(s qualityInitialSample) {
	if s.hasTTFB {
		a.ttfb.add(s.ttfb)
	}
	if s.hasDial {
		a.dial.add(s.dial)
	}
	if s.tcp.present&qualityHasRTT != 0 {
		a.tcpRTT.add(s.tcp.rtt)
	}
	if s.tcp.present&qualityHasMinRTT != 0 {
		rtt := max(s.tcp.minRTT, 0)
		if !a.hasMinRTT || rtt < a.minRTT {
			a.minRTT = rtt
		}
		a.hasMinRTT = true
	}
}

func (a *qualityAccumulator) addEnd(s qualityEndSample) {
	bytesDown := max(s.bytesDown, 0)
	if bytesDown > math.MaxInt64-a.bytesDown {
		// Preserve a nonnegative lower bound and advertise its loss of exact
		// value/ranking, instead of wrapping the busiest path below zero.
		a.bytesDown, a.bytesDownOverflow = math.MaxInt64, true
	} else {
		a.bytesDown += bytesDown
	}
	a.hasBytes = true
	if s.hasBulk {
		kbps := max(s.bulkKbps, 0)
		a.bulk.add(kbps)
		a.maxBulkKbps = max(a.maxBulkKbps, kbps)
	}
	if s.hasBulk || s.spliced || s.bytesDown >= int64(qualityMilestones[0]) {
		a.hasBulkSpliced = true
		if s.spliced {
			a.bulkSpliced++
		}
	}
	if s.s2.present&qualityHasLoadRTT != 0 {
		a.bulkRTT.add(s.s2.loadRTT)
	}
	if outOfOrder, total, ok := qualityRXDelta(s); ok && total > 0 {
		a.rxOutOfOrder.add(outOfOrder)
		a.rxTotal.add(total)
	}
	// Use S2 when available, otherwise S1, as selected by the connection
	// tracker. These cumulative counters are included once, at completion.
	if s.lastTCP.present&qualityHasTX != 0 && s.lastTCP.txTotal > 0 {
		a.txRetransmit.add(s.lastTCP.txRetransmit)
		a.txTotal.add(s.lastTCP.txTotal)
	}
}

func qualityScalar[T any](v T) *T { return &v }

func (a *qualityAccumulator) view() qualityPathView {
	v := qualityPathView{}
	v.ProbeCount, v.ProbeFailedCount = a.probes, a.probesFailed
	v.ProbeReasons = maps.Clone(a.probeReasons)
	if a.probeConnect.total > 0 {
		v.ProbeConnectP50Ms = qualityScalar(a.probeConnect.quantile(0.5))
	}
	if a.hasBytes {
		v.BytesDown = qualityScalar(a.bytesDown)
		v.BytesDownOverflow = a.bytesDownOverflow
	}
	if a.ttfb.total > 0 {
		v.TTFBCount = a.ttfb.total
		v.TTFBP50Ms, v.TTFBP90Ms = qualityScalar(a.ttfb.quantile(0.5)), qualityScalar(a.ttfb.quantile(0.9))
	}
	if a.dial.total > 0 {
		v.DialCount = a.dial.total
		v.DialP50Ms, v.DialP90Ms = qualityScalar(a.dial.quantile(0.5)), qualityScalar(a.dial.quantile(0.9))
	}
	if a.tcpRTT.total > 0 {
		v.TCPCount = a.tcpRTT.total
		v.TCPRTTP50Ms, v.TCPRTTP90Ms = qualityScalar(a.tcpRTT.quantile(0.5)), qualityScalar(a.tcpRTT.quantile(0.9))
	}
	if a.hasMinRTT {
		v.TCPRTTMinMs = qualityScalar(a.minRTT.Milliseconds())
	}
	if a.bulk.total > 0 {
		v.BulkCount = a.bulk.total
		v.BulkKbpsP50, v.BulkKbpsP90 = qualityScalar(a.bulk.quantile(0.5)), qualityScalar(a.bulk.quantile(0.9))
		v.BulkKbpsMax = qualityScalar(a.maxBulkKbps)
	}
	if a.bulkRTT.total > 0 {
		v.BulkRTTP50Ms = qualityScalar(a.bulkRTT.quantile(0.5))
	}
	if a.hasBulkSpliced {
		v.BulkSplicedCount = qualityScalar(a.bulkSpliced)
	}
	if a.rxTotal.nonzero() {
		v.RXOutOfOrderFraction = qualityScalar(a.rxOutOfOrder.number() / a.rxTotal.number())
	}
	if a.txTotal.nonzero() {
		v.TXRetransmitFraction = qualityScalar(a.txRetransmit.number() / a.txTotal.number())
	}
	return v
}

type qualityNetworkMetadata struct {
	labels *networkLabelsView
	known  bool
}

type qualityCollection struct {
	started        time.Time
	network        qualityNetworkMetadata
	paths          map[string]*qualityAccumulator
	witness        *qualityAccumulator
	tagBytes       int
	samplesDropped uint64
}

func (c *qualityCollection) nonempty() bool {
	return len(c.paths) > 0 || c.witness != nil || c.samplesDropped > 0
}

// Admission is measurement-only. Keep the complete history of the first 64
// measured logical tags, with at most 16 KiB of copied original tag bytes.
// Retirement/GC never frees a slot within this scope: a returning tag must
// retain its earlier byte totals and histogram samples. Exact key equality
// also avoids changing tag identity through truncation or hash collisions.
//
// Reject each meaningful initial/end phase for an unadmitted tag once either
// budget is exhausted. SamplesDropped describes incomplete candidate coverage;
// output top four is then exact only among admitted tags, not globally. The
// distinct rejected-tag count cannot be recovered from this bounded state.
// Witness is a separate aggregate and consumes neither tag budget.
func (c *qualityCollection) accumulator(tag string, witness bool) *qualityAccumulator {
	if witness {
		if c.witness == nil {
			c.witness = &qualityAccumulator{}
		}
		return c.witness
	}
	if a := c.paths[tag]; a != nil {
		return a
	}
	if len(c.paths) >= qualityCollectionPaths || len(tag) > qualityCollectionTagBytes-c.tagBytes {
		c.samplesDropped++
		return nil
	}
	if c.paths == nil {
		c.paths = make(map[string]*qualityAccumulator)
	}
	key := strings.Clone(tag)
	a := &qualityAccumulator{}
	c.paths[key] = a
	c.tagBytes += len(key)
	return a
}

func (c *qualityCollection) addInitial(tag string, witness bool, sample qualityInitialSample) {
	if a := c.accumulator(tag, witness); a != nil {
		a.addInitial(sample)
	}
}

func (c *qualityCollection) addEnd(tag string, witness bool, sample qualityEndSample) {
	if a := c.accumulator(tag, witness); a != nil {
		a.addEnd(sample)
	}
}

func (c *qualityCollection) view(now time.Time, limit int) qualitySegmentView {
	v := qualitySegmentView{
		IsNetworkKnown: c.network.known, Network: cloneNetworkLabels(c.network.labels),
		DurationMs: wholeMilliseconds(now.Round(0).Sub(c.started)), SamplesDropped: c.samplesDropped,
	}
	if c.witness != nil {
		w := c.witness.view()
		v.Witness = &w
	}
	if len(c.paths) == 0 {
		return v
	}
	tags := make([]string, 0, len(c.paths))
	for tag := range c.paths {
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool {
		a, b := c.paths[tags[i]], c.paths[tags[j]]
		if a.bytesDown != b.bytesDown {
			return a.bytesDown > b.bytesDown
		}
		return tags[i] < tags[j]
	})
	shown := min(len(tags), limit)
	v.Paths = make(map[string]qualityPathView, shown)
	for _, tag := range tags[:shown] {
		v.Paths[tag] = c.paths[tag].view()
		if labels := c.paths[tag].labels; len(labels) > 0 {
			if v.pathLabels == nil {
				v.pathLabels = make(map[string]map[string]string)
			}
			v.pathLabels[tag] = maps.Clone(labels)
		}
	}
	v.PathsDropped = len(tags) - shown
	return v
}

// qualitySummary is guarded by the caller's observer mutex. It reads no clock
// and has no connection to routing state, probes, quarantine or path lifetime.
// Only the open segment and current network keep histograms; closed segments
// retain bounded scalar top-four views. The two mutable scopes are independent.
type qualitySummary struct {
	reporting bool
	epoch     uint64
	current   qualityCollection
	open      qualityCollection
	closed    []qualitySegmentView
	dropped   uint64
}

func newQualitySummary(now time.Time) *qualitySummary {
	metadata := qualityNetworkMetadata{}
	c := qualityCollection{started: now.Round(0), network: metadata}
	return &qualitySummary{current: c, open: c, reporting: true}
}

func (q *qualitySummary) addInitial(epoch uint64, tag string, witness bool, sample qualityInitialSample) {
	if q == nil || epoch != q.epoch || !meaningfulQualityInitial(sample) {
		return
	}
	q.current.addInitial(tag, witness, sample)
	if q.reporting {
		q.open.addInitial(tag, witness, sample)
		q.trimSegments()
	}
}

func (q *qualitySummary) addEnd(epoch uint64, tag string, witness bool, sample qualityEndSample) {
	if q == nil || epoch != q.epoch || !meaningfulQualityEnd(sample) {
		return
	}
	q.current.addEnd(tag, witness, sample)
	if q.reporting {
		q.open.addEnd(tag, witness, sample)
		q.trimSegments()
	}
}

// There may be four closed segments while the open one is empty. Its first
// meaningful phase makes it the fourth/latest retained segment instead.
func (q *qualitySummary) trimSegments() {
	kept := len(q.closed)
	if q.open.nonempty() {
		kept++
	}
	for kept > qualityReportSegments {
		copy(q.closed, q.closed[1:])
		q.closed[len(q.closed)-1] = qualitySegmentView{}
		q.closed = q.closed[:len(q.closed)-1]
		q.dropped++
		kept--
	}
}

func (q *qualitySummary) networkChanged(_ string, known bool, now time.Time) uint64 {
	if q == nil {
		return 0
	}
	if q.reporting && q.open.nonempty() {
		q.closed = append(q.closed, q.open.view(now, qualitySegmentPaths))
	}
	q.epoch++
	c := qualityCollection{started: now.Round(0), network: qualityNetworkMetadata{known: known}}
	q.current = c
	if q.reporting {
		q.open = c
		q.trimSegments()
	}
	return q.epoch
}

// report snapshots the latest nonempty segments. A take starts a new report
// window in this same network without changing epoch/current-network history.
// The caller performs it only when the enclosing report is actually taken.
func (q *qualitySummary) report(now time.Time, take bool) ([]qualitySegmentView, uint64) {
	if q == nil || !q.reporting {
		return nil, 0
	}
	var views []qualitySegmentView
	for _, closed := range q.closed {
		views = append(views, cloneQualitySegment(closed))
	}
	if q.open.nonempty() {
		views = append(views, q.open.view(now, qualitySegmentPaths))
	}
	dropped := q.dropped
	if take {
		q.closed = nil
		q.dropped = 0
		q.open = qualityCollection{started: now.Round(0), network: q.current.network}
	}
	return views, dropped
}

// Status/provider need all admitted tags, unlike the top-four report view.
func (q *qualitySummary) currentView(now time.Time) qualitySegmentView {
	if q == nil {
		return qualitySegmentView{}
	}
	return q.current.view(now, qualityCollectionPaths)
}

func cloneQualityPointer[T any](p *T) *T {
	if p == nil {
		return nil
	}
	return qualityScalar(*p)
}

func cloneQualityPath(v qualityPathView) qualityPathView {
	v.ProbeConnectP50Ms = cloneQualityPointer(v.ProbeConnectP50Ms)
	v.ProbeReasons = maps.Clone(v.ProbeReasons)
	v.BytesDown = cloneQualityPointer(v.BytesDown)
	v.TTFBP50Ms, v.TTFBP90Ms = cloneQualityPointer(v.TTFBP50Ms), cloneQualityPointer(v.TTFBP90Ms)
	v.DialP50Ms, v.DialP90Ms = cloneQualityPointer(v.DialP50Ms), cloneQualityPointer(v.DialP90Ms)
	v.TCPRTTP50Ms, v.TCPRTTP90Ms = cloneQualityPointer(v.TCPRTTP50Ms), cloneQualityPointer(v.TCPRTTP90Ms)
	v.TCPRTTMinMs = cloneQualityPointer(v.TCPRTTMinMs)
	v.BulkKbpsP50, v.BulkKbpsP90 = cloneQualityPointer(v.BulkKbpsP50), cloneQualityPointer(v.BulkKbpsP90)
	v.BulkKbpsMax = cloneQualityPointer(v.BulkKbpsMax)
	v.BulkRTTP50Ms = cloneQualityPointer(v.BulkRTTP50Ms)
	v.BulkSplicedCount = cloneQualityPointer(v.BulkSplicedCount)
	v.RXOutOfOrderFraction = cloneQualityPointer(v.RXOutOfOrderFraction)
	v.TXRetransmitFraction = cloneQualityPointer(v.TXRetransmitFraction)
	return v
}

func cloneQualitySegment(v qualitySegmentView) qualitySegmentView {
	if v.pathLabels != nil {
		labels := make(map[string]map[string]string, len(v.pathLabels))
		for tag, values := range v.pathLabels {
			labels[tag] = maps.Clone(values)
		}
		v.pathLabels = labels
	}
	v.Network = cloneNetworkLabels(v.Network)
	if v.Paths != nil {
		paths := make(map[string]qualityPathView, len(v.Paths))
		for tag, path := range v.Paths {
			paths[tag] = cloneQualityPath(path)
		}
		v.Paths = paths
	}
	if v.Witness != nil {
		witness := cloneQualityPath(*v.Witness)
		v.Witness = &witness
	}
	return v
}
