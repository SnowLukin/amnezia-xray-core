package health

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

const labelRetry = 10 * time.Minute

type labelRequestSettings struct {
	outbound, destination string
	labels                map[string]string
	timeout               time.Duration
	requestsPerHour       int
}

type labelNetwork struct {
	labels   map[string]string
	attempts int
	retryAt  time.Time
}

type labelJob struct {
	network *labelNetwork
	epoch   uint64
	cancel  context.CancelFunc
	ctx     context.Context
}

func (c *Config) ValidateMeasurementContext() error {
	if err := ValidateLabels(c.GetSessionLabels(), 8); err != nil {
		return fmt.Errorf("report.labels: %w", err)
	}
	for _, p := range c.GetPaths() {
		if err := ValidateLabels(p.GetLabels(), 6); err != nil {
			return fmt.Errorf("path.labels: %w", err)
		}
	}
	if c.GetLabelRequest() != nil || c.GetFeatures().GetLabelRequest() {
		lr := c.GetLabelRequest()
		if lr == nil {
			return fmt.Errorf("network.labelRequest requires settings")
		}
		u, err := url.Parse(lr.Destination)
		if lr.Destination != "" && (err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.Fragment != "") {
			return fmt.Errorf("network.labelRequest.destination must be an HTTP(S) URL without credentials or fragment")
		}
		if (c.GetFeatures().GetLabelRequest() && (lr.Outbound == "" || lr.Destination == "" || len(lr.Labels) == 0)) || len(lr.Labels) > 6 {
			return fmt.Errorf("network.labelRequest needs outbound and 1 to 6 labels")
		}
		if lr.Timeout < 0 || lr.Timeout > int64(30*time.Second) || lr.RequestsPerHour < 0 || lr.RequestsPerHour > 60 {
			return fmt.Errorf("network.labelRequest: timeout must be at most 30s and requestsPerHour from 1 to 60")
		}
		for key, header := range lr.Labels {
			if !validLabelName(key) || CheckHeaderName(header) != nil {
				return fmt.Errorf("network.labelRequest: invalid label/header %q", key)
			}
		}
	}
	return nil
}

func (e *engine) cancelLabelRequest(now time.Time) {
	if j := e.labelRunning; j != nil {
		j.cancel()
		if j.network.attempts == 1 {
			j.network.retryAt = now.Add(labelRetry)
		}
		e.labelRunning = nil
	}
}

// Independent of routing's network memory: even no_memory retains bounded
// measurement metadata. Empty ids intentionally never share a cache entry.
func (e *engine) changeLabelNetwork(id string, now time.Time) {
	e.cancelLabelRequest(now)
	e.labelProof = false
	e.labelNetwork = nil
	if e.s.labelRequest == nil {
		return
	}
	if id == "" {
		e.labelNetwork = &labelNetwork{}
		return
	}
	if n := e.labelCache[id]; n != nil {
		for i, key := range e.labelOrder {
			if key == id {
				e.labelOrder = append(e.labelOrder[:i], e.labelOrder[i+1:]...)
				break
			}
		}
		e.labelOrder = append(e.labelOrder, id)
		e.labelNetwork = n
		return
	}
	if e.labelCache == nil {
		e.labelCache = make(map[string]*labelNetwork)
	}
	n := &labelNetwork{}
	e.labelCache[id] = n
	e.labelOrder = append(e.labelOrder, id)
	for len(e.labelOrder) > e.s.maxNetworks {
		delete(e.labelCache, e.labelOrder[0])
		e.labelOrder = e.labelOrder[1:]
	}
	e.labelNetwork = n
}

// Called in the existing loop. The request neither records demand nor uses
// the probe scheduler. Eligibility cannot affect controller state or timing.
func (e *engine) nextLabelRequest(parent context.Context, now time.Time) (*labelJob, time.Time) {
	cfg, n := e.s.labelRequest, e.labelNetwork
	if cfg == nil || n == nil || !e.labelProof || e.idle(now) || e.networkDown || e.memoryLow || e.labelRunning != nil || len(n.labels) > 0 || n.attempts >= 2 {
		return nil, time.Time{}
	}
	if now.Before(n.retryAt) {
		return nil, n.retryAt
	}
	kept := e.labelAttempts[:0]
	for _, at := range e.labelAttempts {
		if now.Sub(at) < time.Hour {
			kept = append(kept, at)
		}
	}
	e.labelAttempts = kept
	if len(kept) >= cfg.requestsPerHour {
		return nil, kept[0].Add(time.Hour)
	}
	ctx, cancel := context.WithCancel(parent)
	j := &labelJob{network: n, epoch: e.qualityEpoch, ctx: ctx, cancel: cancel}
	e.labelRunning = j
	n.attempts++
	e.labelAttempts = append(e.labelAttempts, now)
	return j, time.Time{}
}

func (e *engine) labelResult(j *labelJob, labels map[string]string, class string, now time.Time) {
	if e.labelRunning != j || j.epoch != e.qualityEpoch || j.network != e.labelNetwork {
		return
	}
	e.labelRunning = nil
	j.cancel()
	if class == "" {
		j.network.labels = filterLabels(labels, 6)
	} else if j.network.attempts == 1 {
		j.network.retryAt = now.Add(labelRetry)
	}
	e.updateQualityLabels()
	e.emit(now, "network_labels", map[string]any{"is_ok": class == "", "error": class, "labels": maps.Clone(j.network.labels)})
}

func (o *Observer) runLabelRequest(j *labelJob) {
	labels, class := o.fetchNetworkLabels(j.ctx)
	o.mu.Lock()
	o.e.labelResult(j, labels, class, o.now())
	o.mu.Unlock()
	o.wake()
}

func (o *Observer) fetchNetworkLabels(parent context.Context) (map[string]string, string) {
	cfg := o.s.labelRequest
	var wire atomic.Int64
	ctx, cancel := context.WithTimeout(withProbe(parent, &wire), cfg.timeout)
	defer cancel()
	tr := &http.Transport{DisableCompression: true, MaxResponseHeaderBytes: 16 << 10, DialContext: func(_ context.Context, network, address string) (net.Conn, error) {
		dest, err := net.ParseDestination(network + ":" + address)
		if err != nil {
			return nil, err
		}
		return tagged.Dialer(ctx, o.dispatcher, dest, cfg.outbound)
	}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.destination, nil)
	if err != nil {
		return nil, "request"
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, "timeout"
		}
		return nil, "network"
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "status_" + strconv.Itoa(resp.StatusCode)
	}
	labels := make(map[string]string)
	for key, header := range cfg.labels {
		if value := resp.Header.Get(header); validLabelName(key) && validLabelValue(value) {
			labels[key] = value
		}
	}
	// No more than 4 KiB of response body is read and it is never retained.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if len(labels) == 0 {
		return nil, "no_labels"
	}
	return labels, ""
}

// noteLabelSuccess is independent of controller proof. A late reply from a
// previous network must not authorize a request, even under routing IGNORE.
func (e *engine) noteLabelSuccess(epoch uint64) bool {
	if e.s.labelRequest == nil || epoch != e.qualityEpoch || e.labelProof {
		return false
	}
	e.labelProof = true
	return true
}

func (o *Observer) labelSucceeded(epoch uint64) {
	if o.s.labelRequest == nil {
		return
	}
	o.mu.Lock()
	fresh := o.e.noteLabelSuccess(epoch)
	o.mu.Unlock()
	if fresh {
		o.wake()
	}
}
