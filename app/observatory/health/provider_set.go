package health

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport/internet/tagged"
	"google.golang.org/protobuf/proto"
)

// providerRuntime holds the outbounds of one provider. The decision state is
// in the engine; this part touches the outbound manager.
type providerRuntime struct {
	p   *provider
	cfg *providerSettings

	// Guarded by Observer.applyMu.
	version string
	// Config of every outbound this provider added, by tag, to tell an
	// unchanged outbound from a changed one.
	built    map[string]*core.OutboundHandlerConfig
	draining map[outbound.Handler]*drainingOutbound
	drains   uint64
}

type drainingOutbound struct {
	tag     string
	handler outbound.Handler
	// Still registered under its tag and only out of the balancers. A
	// replaced outbound is already unregistered and only finishes its
	// connections.
	registered bool
	timer      *time.Timer
	// Order of the drain among the drains of the provider, oldest first.
	seq uint64
	// Connections through the tag that started later went to the handler
	// that replaced this one.
	since int64
}

// providerAnswer is a parsed provider response in protocol 1:
//
//	{"protocol": 1, "outbounds": [...], "costs": {"tag": 1}, "nextRefreshIn": "2h", "version": "..."}
type providerAnswer struct {
	// The outbounds as sent, handed to the config loader in one piece.
	outbounds     json.RawMessage
	heads         []outboundHead
	costs         map[string]float64
	nextRefreshIn time.Duration
	version       string
	// Rules, balancers and the domain strategy in the format of the config
	// file; withStrategy tells whether the strategy was there at all.
	routing      json.RawMessage
	withStrategy bool
}

type outboundHead struct {
	Tag      string `json:"tag"`
	Protocol string `json:"protocol"`
}

// Keys of an answer in protocol 1. The set keys make up the set, and an
// answer without a version is named by a hash of them. The meta keys only
// say how to take the set: an answer that differs in them alone is the same
// set. Any other key refuses the whole answer, so a backend never takes a
// key for applied that the core threw away; a new key is protocol 2.
var (
	setKeys  = map[string]bool{"outbounds": true, "costs": true, "routing": true}
	metaKeys = map[string]bool{"protocol": true, "version": true, "hash": true, "nextRefreshIn": true}
)

// answerError is an answer that is a JSON object but breaks protocol 1. The
// backend itself answered, so no other via is tried: it leads to the same
// backend.
type answerError struct{ msg string }

func (e *answerError) Error() string { return "invalid answer: " + e.msg }

func answerErrorf(format string, args ...any) error {
	return &answerError{msg: fmt.Sprintf(format, args...)}
}

func isAnswerError(err error) bool {
	var ae *answerError
	return errors.As(err, &ae)
}

// parseAnswer decodes the body once into its sections; everything else reads
// a section, and the outbounds stay one piece of the answer until they are
// built. A body that is not a JSON object is a broken answer, which another
// via may bring whole; an object against the protocol is an answerError.
func parseAnswer(body []byte) (*providerAnswer, error) {
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(body, &sections); err != nil {
		return nil, err
	}
	if sections == nil {
		return nil, errors.New("not a JSON object")
	}
	for _, key := range slices.Sorted(maps.Keys(sections)) {
		switch {
		case key == "healthObservatory":
			return nil, answerErrorf("healthObservatory is not accepted in an answer")
		case !setKeys[key] && !metaKeys[key]:
			return nil, answerErrorf("unknown key %q", cut(key))
		}
	}
	if p := sections["protocol"]; !isNull(p) {
		var n float64
		if p[0] == '"' || json.Unmarshal(p, &n) != nil || n != providerProtocol {
			return nil, answerErrorf("protocol %s is not supported", cut(string(p)))
		}
	}
	a := &providerAnswer{outbounds: sections["outbounds"]}
	if isNull(a.outbounds) {
		return nil, answerErrorf("no outbounds")
	}
	if err := json.Unmarshal(a.outbounds, &a.heads); err != nil {
		return nil, answerErrorf("outbounds: %v", err)
	}
	if len(a.heads) == 0 {
		return nil, answerErrorf("no outbounds")
	}
	if c := sections["costs"]; !isNull(c) {
		if err := json.Unmarshal(c, &a.costs); err != nil {
			return nil, answerErrorf("costs: %v", err)
		}
	}
	next, err := parseRefreshIn(sections["nextRefreshIn"])
	if err != nil {
		return nil, answerErrorf("%v", err)
	}
	a.nextRefreshIn = next
	for _, key := range []string{"version", "hash"} {
		if v := sections[key]; a.version == "" && !isNull(v) {
			if err := json.Unmarshal(v, &a.version); err != nil {
				return nil, answerErrorf("%s: %v", key, err)
			}
		}
	}
	if r := sections["routing"]; !isNull(r) {
		var parts map[string]json.RawMessage
		if err := json.Unmarshal(r, &parts); err != nil {
			return nil, answerErrorf("routing: %v", err)
		}
		for _, key := range slices.Sorted(maps.Keys(parts)) {
			if key != "rules" && key != "balancers" && key != "domainStrategy" {
				return nil, answerErrorf("routing: unknown key %q", cut(key))
			}
		}
		if ds := parts["domainStrategy"]; !isNull(ds) {
			// The config loader takes a strategy it does not know for AsIs.
			var name string
			if json.Unmarshal(ds, &name) != nil || !slices.ContainsFunc(domainStrategies, func(s string) bool {
				return strings.EqualFold(s, name)
			}) {
				return nil, answerErrorf("routing: unknown domainStrategy %s", cut(string(ds)))
			}
			a.withStrategy = true
		}
		a.routing = r
	}
	if a.version == "" {
		a.version = setHash(sections)
	}
	return a, nil
}

var domainStrategies = []string{"AsIs", "IPIfNonMatch", "IPOnDemand"}

// cut shortens a key or a value of an answer for an error, so that one odd
// answer cannot fill the event log.
func cut(s string) string {
	const limit = 64
	if len(s) > limit {
		return s[:limit] + "..."
	}
	return s
}

func isNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

// parseRefreshIn accepts a duration string such as "90m" or seconds.
func parseRefreshIn(raw json.RawMessage) (time.Duration, error) {
	if isNull(raw) {
		return 0, nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("nextRefreshIn: %w", err)
		}
		return max(d, 0), nil
	}
	var secs float64
	if err := json.Unmarshal(raw, &secs); err != nil {
		return 0, errors.New("nextRefreshIn must be a duration or seconds")
	}
	return max(time.Duration(secs*float64(time.Second)), 0), nil
}

// setHash names an answer by its set keys, so the same set sent again,
// however formatted and whatever its meta keys say, is known as the applied
// one.
func setHash(sections map[string]json.RawMessage) string {
	keys := make([]string, 0, len(sections))
	for key := range sections {
		if setKeys[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		h.Write([]byte(key))
		h.Write([]byte{0})
		writeCompact(h, sections[key])
		h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))[:16]
}

// writeCompact writes valid JSON without the whitespace between its tokens,
// as json.Compact does, without a copy of it.
func writeCompact(w io.Writer, v []byte) {
	inString, escaped := false, false
	from := 0
	for i, c := range v {
		switch {
		case escaped:
			escaped = false
		case inString:
			switch c {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
		case c == '"':
			inString = true
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			w.Write(v[from:i])
			from = i + 1
		}
	}
	w.Write(v[from:])
}

// loadOutbounds builds outbound configs with the JSON config loader of the
// core, the same code that reads the config file.
func loadOutbounds(raw json.RawMessage) ([]*core.OutboundHandlerConfig, error) {
	c, err := core.LoadConfig("json", io.MultiReader(
		strings.NewReader(`{"outbounds":`), bytes.NewReader(raw), strings.NewReader(`}`)))
	if err != nil {
		return nil, err
	}
	return c.Outbound, nil
}

// loadRouting builds rules and balancers with the JSON config loader of the
// core, like loadOutbounds.
func loadRouting(raw json.RawMessage) (*router.Config, error) {
	// The loader accepts leastCost balancers only next to an observatory that
	// observes their selectors; the real check is checkRouting.
	var head struct {
		Balancers []struct {
			Selector []string `json:"selector"`
		} `json:"balancers"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, err
	}
	observe := []string{}
	for _, b := range head.Balancers {
		observe = append(observe, b.Selector...)
	}
	health := []byte{}
	if len(observe) > 0 {
		subjects, _ := json.Marshal(observe)
		health = append(append([]byte(`,"healthObservatory":{"subjectSelector":`), subjects...), '}')
	}
	c, err := core.LoadConfig("json", io.MultiReader(
		strings.NewReader(`{"routing":`), bytes.NewReader(raw), bytes.NewReader(health), strings.NewReader(`}`)))
	if err != nil {
		return nil, err
	}
	for _, app := range c.App {
		if inst, err := app.GetInstance(); err == nil {
			if rc, ok := inst.(*router.Config); ok {
				return rc, nil
			}
		}
	}
	return nil, errors.New("no routing in the answer")
}

// checkRouting refuses rules that would send traffic to an outbound or a
// balancer that does not exist: Xray would take its default outbound for
// them, which may be outside the tunnel. It refuses webhooks too: a webhook
// posts every matching connection, with its destination, from a goroutine
// of its own.
func (o *Observer) checkRouting(rt *providerRuntime, rc *router.Config, setTags []string) error {
	exists := func(tag string) bool {
		return slices.Contains(setTags, tag) || (o.ohm != nil && o.ohm.GetHandler(tag) != nil)
	}
	balancers := map[string]*router.BalancingRule{}
	for _, b := range rc.BalancingRule {
		balancers[b.Tag] = b
		if b.FallbackTag != "" && !exists(b.FallbackTag) {
			return fmt.Errorf("balancer %s falls back to unknown outbound %s", b.Tag, b.FallbackTag)
		}
		if !strings.EqualFold(b.Strategy, "leastcost") {
			continue
		}
		for _, sel := range b.OutboundSelector {
			if !o.observedSelector(sel) {
				return fmt.Errorf("balancer %s chooses %s, which nothing observes", b.Tag, sel)
			}
		}
	}
	if b := balancers[rt.cfg.balancer]; b == nil || !strings.EqualFold(b.Strategy, "leastcost") {
		return fmt.Errorf("routing drops the leastCost balancer %s the provider feeds", rt.cfg.balancer)
	}
	for i, r := range rc.Rule {
		if r.GetWebhook() != nil {
			return fmt.Errorf("rule %d has a webhook", i+1)
		}
		if tag := r.GetTag(); tag != "" && !exists(tag) {
			return fmt.Errorf("rule %d goes to unknown outbound %s", i+1, tag)
		}
		if tag := r.GetBalancingTag(); tag != "" && balancers[tag] == nil {
			return fmt.Errorf("rule %d goes to unknown balancer %s", i+1, tag)
		}
		if _, err := r.BuildCondition(); err != nil {
			return fmt.Errorf("rule %d: %w", i+1, err)
		}
	}
	return nil
}

func (o *Observer) observedSelector(sel string) bool {
	for _, p := range o.s.subjects {
		if strings.HasPrefix(sel, p) {
			return true
		}
	}
	for _, rt := range o.providers {
		if strings.HasPrefix(sel, rt.cfg.prefix) {
			return true
		}
	}
	return false
}

func (o *Observer) runtimeFor(p *provider) *providerRuntime {
	for _, rt := range o.providers {
		if rt.p == p {
			return rt
		}
	}
	return nil
}

func (o *Observer) emit(typ string, fields map[string]any) {
	o.mu.Lock()
	o.e.emit(o.now(), typ, fields)
	o.mu.Unlock()
}

func (o *Observer) runRefresh(j *refreshJob) {
	rt := o.runtimeFor(j.p)
	var r refreshOutcome
	if a, err := o.fetchSet(rt, j); err != nil {
		r.err = err.Error()
	} else if changed, err := o.applySet(rt, a, j); err != nil {
		r.err = err.Error()
	} else {
		r.ok = true
		r.changed = changed
		r.nextRefreshIn = a.nextRefreshIn
	}
	o.mu.Lock()
	o.e.onRefreshDone(j, r, o.now())
	o.mu.Unlock()
	o.wake()
}

// fetchSet requests the set through the via outbounds in turn until one
// brings a well-formed answer, or the backend answers against the protocol:
// another via leads to the same backend. Every attempt is an event.
func (o *Observer) fetchSet(rt *providerRuntime, j *refreshJob) (*providerAnswer, error) {
	via := rt.cfg.via
	if len(via) == 0 {
		via = []string{""}
	}
	var lastErr error
	for i, v := range via {
		o.mu.Lock()
		tag, skip := o.e.resolveVia(v, o.now())
		low := o.e.memoryLow
		o.mu.Unlock()
		if skip == "" && tag != "" && o.ohm.GetHandler(tag) == nil {
			skip = "no such outbound"
		}
		f := map[string]any{"provider": rt.cfg.tag, "trigger": j.trigger.String(), "retry": j.retry,
			"attempt": i + 1, "via": v, "outbound": tag}
		if skip != "" {
			f["ok"] = false
			f["error"] = skip
			o.emit("provider_fetch", f)
			lastErr = errors.New(skip)
			continue
		}
		start := time.Now()
		a, status, n, err := o.fetchOnce(rt.cfg, tag)
		// An answer against the protocol still came from the backend: the
		// request did its job and the refusal is the apply's, so the report
		// counts it once, as rejected.
		refused := isAnswerError(err)
		f["ok"] = err == nil || refused
		f["status"] = status
		f["ms"] = time.Since(start).Milliseconds()
		f["bytes"] = n
		if err != nil && !refused {
			f["error"] = err.Error()
		}
		o.emit("provider_fetch", f)
		if err == nil {
			return a, nil
		}
		if refused {
			o.emit("provider_apply", map[string]any{"provider": rt.cfg.tag, "trigger": j.trigger.String(),
				"ok": false, "error": err.Error()})
			return nil, err
		}
		lastErr = err
		if low {
			// One request at a time is enough; the backoff brings the next.
			break
		}
	}
	return nil, lastErr
}

func (o *Observer) fetchOnce(cfg *providerSettings, tag string) (*providerAnswer, int, int, error) {
	// Counted like a probe, so the request is not taken for user traffic.
	var wire atomic.Int64
	ctx, cancel := context.WithTimeout(withProbe(o.runCtx, &wire), cfg.timeout)
	defer cancel()
	// Keep-alive stays on: a server told to close the connection after the
	// answer may lose its last bytes on some transports. The connection is
	// closed here once the answer is read.
	transport := &http.Transport{
		DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
			dest, err := net.ParseDestination(network + ":" + addr)
			if err != nil {
				return nil, err
			}
			return tagged.Dialer(ctx, o.dispatcher, dest, tag)
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.url, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	for k, v := range cfg.headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, 0, 0, errors.New("timeout")
		}
		// Events go to analytics; the URL may name more than it should.
		if ue, ok := err.(*url.Error); ok {
			err = ue.Err
		}
		return nil, 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, cfg.maxBytes+1))
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			err = errors.New("timeout")
		}
		return nil, resp.StatusCode, len(body), err
	}
	if int64(len(body)) > cfg.maxBytes {
		return nil, resp.StatusCode, len(body), fmt.Errorf("answer larger than %d bytes", cfg.maxBytes)
	}
	a, err := parseAnswer(body)
	if err != nil && !isAnswerError(err) {
		err = fmt.Errorf("invalid answer: %w", err)
	}
	if err != nil {
		return nil, resp.StatusCode, len(body), err
	}
	return a, resp.StatusCode, len(body), nil
}

type outboundChange struct {
	tag    string
	config *core.OutboundHandlerConfig
	// The outbound registered under the tag now, nil for a new tag.
	old      outbound.Handler
	oldDrain *drainingOutbound
	handler  outbound.Handler
}

// applySet replaces the outbounds of the provider with the answer. Nothing
// changes unless every outbound builds; outbounds that stay as they were are
// not touched, so their connections go on.
func (o *Observer) applySet(rt *providerRuntime, a *providerAnswer, j *refreshJob) (bool, error) {
	o.applyMu.Lock()
	defer o.applyMu.Unlock()
	fields := map[string]any{"provider": rt.cfg.tag, "trigger": j.trigger.String(), "version": a.version}
	fail := func(err error) (bool, error) {
		fields["ok"] = false
		fields["error"] = err.Error()
		o.emit("provider_apply", fields)
		return false, err
	}
	if o.applyClosed {
		return fail(errors.New("closed"))
	}
	if a.version == rt.version {
		fields["ok"] = true
		fields["changed"] = false
		fields["unchanged"] = len(a.heads)
		o.emit("provider_apply", fields)
		return false, nil
	}
	changes, unchanged, err := o.buildSet(rt, a)
	if err != nil {
		return fail(err)
	}
	var routing *router.Config
	if len(a.routing) > 0 {
		if !rt.cfg.routing || o.replaceRules == nil {
			// Whether to take routing from a provider is the choice of the
			// client: the set applies without it.
			fields["routing_ignored"] = true
		} else {
			setTags := make([]string, 0, len(changes)+len(unchanged))
			for _, c := range changes {
				setTags = append(setTags, c.tag)
			}
			setTags = append(setTags, unchanged...)
			if routing, err = o.parseRouting(a.routing); err == nil {
				err = o.checkRouting(rt, routing, setTags)
			}
			if err != nil {
				for _, c := range changes {
					common.Close(c.handler)
				}
				return fail(fmt.Errorf("routing: %w", err))
			}
		}
	}

	owned := map[string]outbound.Handler{}
	for _, h := range o.ohm.ListHandlers(o.ctx) {
		if t := h.Tag(); t != "" && strings.HasPrefix(t, rt.cfg.prefix) {
			owned[t] = h
		}
	}
	retiring := map[string]*drainingOutbound{}
	for _, d := range rt.draining {
		if d.registered {
			retiring[d.tag] = d
		}
	}
	inSet := map[string]bool{}
	tags := make([]string, 0, len(changes)+len(unchanged))
	var added, replaced []*outboundChange
	for _, c := range changes {
		inSet[c.tag] = true
		tags = append(tags, c.tag)
		c.old = owned[c.tag]
		c.oldDrain = retiring[c.tag]
		if c.old != nil {
			replaced = append(replaced, c)
		} else {
			added = append(added, c)
		}
	}
	var revive []string
	for _, tag := range unchanged {
		inSet[tag] = true
		tags = append(tags, tag)
		if retiring[tag] != nil {
			revive = append(revive, tag)
		}
	}
	var removed []string
	for tag := range owned {
		if !inSet[tag] && retiring[tag] == nil {
			removed = append(removed, tag)
		}
	}
	sort.Strings(removed)

	var reset []string
	for _, c := range replaced {
		reset = append(reset, c.tag)
	}
	o.mu.Lock()
	o.e.replacingTags(reset)
	o.mu.Unlock()
	if err := o.registerSet(added, replaced); err != nil {
		o.mu.Lock()
		clear(o.e.replacing)
		o.mu.Unlock()
		return fail(err)
	}
	for _, c := range changes {
		rt.built[c.tag] = c.config
	}
	for _, c := range replaced {
		if c.oldDrain != nil {
			c.oldDrain.registered = false
			revive = append(revive, c.tag)
		} else {
			o.startDrain(rt, c.tag, c.old, false)
		}
	}
	for _, tag := range revive {
		if d := retiring[tag]; d != nil && d.registered {
			d.timer.Stop()
			delete(rt.draining, d.handler)
		}
	}
	for _, tag := range removed {
		o.startDrain(rt, tag, owned[tag], true)
	}
	rt.version = a.version
	// Outside the observer lock: rebuilt leastCost balancers ask the observer
	// for their selectors.
	if routing != nil {
		if err := o.replaceRules(routing, a.withStrategy); err != nil {
			fields["routing_error"] = err.Error()
		} else {
			fields["rules"] = len(routing.Rule)
			keep := map[string]bool{}
			for _, b := range routing.BalancingRule {
				keep[b.Tag] = true
			}
			o.mu.Lock()
			o.e.keepSelectors(keep)
			o.mu.Unlock()
		}
	}

	costs := map[string]float64{}
	for tag, c := range a.costs {
		costs[rt.cfg.prefix+tag] = c
	}
	now := o.now()
	o.mu.Lock()
	exhausted := false
	if s := o.e.selectors[rt.cfg.balancer]; s != nil {
		exhausted = s.exhausted
	}
	addedTags := make([]string, len(added))
	for i, c := range added {
		addedTags[i] = c.tag
	}
	o.e.applyProviderSet(rt.p, providerChange{
		tags: tags, costs: costs, added: addedTags, retire: removed, revive: revive, reset: reset,
	}, now)
	rt.p.version = a.version
	rt.p.draining = len(rt.draining)
	fields["ok"] = true
	fields["changed"] = true
	fields["added"] = len(added)
	fields["replaced"] = len(replaced)
	fields["removed"] = len(removed)
	fields["unchanged"] = len(unchanged)
	fields["exhausted"] = exhausted
	o.e.emit(now, "provider_apply", fields)
	o.mu.Unlock()
	o.settleDrains(rt)
	return true, nil
}

// buildSet validates the answer and creates handlers for outbounds that are
// new or changed. It returns them and the tags of outbounds that stay as
// they are. On error every created handler is closed.
func (o *Observer) buildSet(rt *providerRuntime, a *providerAnswer) ([]*outboundChange, []string, error) {
	if len(a.heads) > rt.cfg.maxOutbounds {
		return nil, nil, fmt.Errorf("set of %d outbounds, at most %d allowed", len(a.heads), rt.cfg.maxOutbounds)
	}
	for tag, c := range a.costs {
		if math.IsNaN(c) || math.IsInf(c, 0) || c <= 0 {
			return nil, nil, fmt.Errorf("cost of %s must be a number above zero", tag)
		}
	}
	tags := make([]string, len(a.heads))
	seen := map[string]bool{}
	for i, head := range a.heads {
		if head.Tag == "" {
			return nil, nil, fmt.Errorf("outbound %d has no tag", i+1)
		}
		if !slices.Contains(rt.cfg.protocols, strings.ToLower(head.Protocol)) {
			return nil, nil, fmt.Errorf("outbound %s: protocol %q is not allowed", head.Tag, head.Protocol)
		}
		if seen[head.Tag] {
			return nil, nil, fmt.Errorf("duplicate tag %s", head.Tag)
		}
		seen[head.Tag] = true
		tags[i] = rt.cfg.prefix + head.Tag
	}
	configs, err := o.parseOutbounds(a.outbounds)
	if err != nil {
		return nil, nil, fmt.Errorf("build: %w", err)
	}
	if len(configs) != len(tags) {
		return nil, nil, fmt.Errorf("build: %d outbounds of %d", len(configs), len(tags))
	}
	var changes []*outboundChange
	var unchanged []string
	closeAll := func() {
		for _, c := range changes {
			common.Close(c.handler)
		}
	}
	for i, cfg := range configs {
		cfg.Tag = tags[i]
		if h := o.ohm.GetHandler(cfg.Tag); h != nil && sameOutbound(h, rt.built[cfg.Tag], cfg) {
			unchanged = append(unchanged, cfg.Tag)
			continue
		}
		h, err := o.newHandler(cfg)
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("build %s: %w", cfg.Tag, err)
		}
		changes = append(changes, &outboundChange{tag: cfg.Tag, config: cfg, handler: h})
	}
	return changes, unchanged, nil
}

func (o *Observer) createHandler(cfg *core.OutboundHandlerConfig) (outbound.Handler, error) {
	inst := core.FromContext(o.ctx)
	if inst == nil {
		return nil, errors.New("no core instance")
	}
	raw, err := core.CreateObject(inst, cfg)
	if err != nil {
		return nil, err
	}
	h, ok := raw.(outbound.Handler)
	if !ok {
		common.Close(raw)
		return nil, errors.New("not an outbound handler")
	}
	return h, nil
}

// registerSet puts new and changed outbounds into the outbound manager, or
// puts everything back and closes the new handlers when one fails.
func (o *Observer) registerSet(added, replaced []*outboundChange) error {
	var undo []func()
	rollback := func(err error) error {
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
		for _, c := range added {
			common.Close(c.handler)
		}
		for _, c := range replaced {
			common.Close(c.handler)
		}
		return err
	}
	for _, c := range replaced {
		if err := o.ohm.RemoveHandler(o.ctx, c.tag); err != nil {
			return rollback(err)
		}
		undo = append(undo, func() { _ = o.ohm.AddHandler(o.ctx, c.old) })
		if err := o.ohm.AddHandler(o.ctx, c.handler); err != nil {
			return rollback(err)
		}
		undo = append(undo, func() { _ = o.ohm.RemoveHandler(o.ctx, c.tag) })
	}
	for _, c := range added {
		if err := o.ohm.AddHandler(o.ctx, c.handler); err != nil {
			return rollback(err)
		}
		undo = append(undo, func() { _ = o.ohm.RemoveHandler(o.ctx, c.tag) })
	}
	return nil
}

func sameOutbound(h outbound.Handler, built, next *core.OutboundHandlerConfig) bool {
	proxy, sender := h.ProxySettings(), h.SenderSettings()
	if built != nil {
		proxy, sender = built.ProxySettings, built.SenderSettings
	}
	return sameMessage(proxy, next.ProxySettings) && sameMessage(sender, next.SenderSettings)
}

func sameMessage(a, b interface {
	GetInstance() (proto.Message, error)
}) bool {
	ai, err := a.GetInstance()
	if err != nil {
		return false
	}
	bi, err := b.GetInstance()
	if err != nil {
		return false
	}
	return proto.Equal(ai, bi)
}

// startDrain closes the outbound after the drain time. A registered one is
// taken out of the outbound manager first.
func (o *Observer) startDrain(rt *providerRuntime, tag string, h outbound.Handler, registered bool) {
	rt.drains++
	d := &drainingOutbound{tag: tag, handler: h, registered: registered, seq: rt.drains, since: o.nowNano()}
	rt.draining[h] = d
	d.timer = time.AfterFunc(rt.cfg.drain, func() { o.finishDrain(rt, d) })
}

// settleDrains ends at once the drains nothing waits for: outbounds without
// open connections that no balancer chooses. Beyond maxOutbounds draining
// outbounds the oldest drains end early, so sets replaced one after another
// do not pile up handlers. The caller holds applyMu.
func (o *Observer) settleDrains(rt *providerRuntime) {
	open := o.oldestConnections()
	var idle []*drainingOutbound
	o.mu.Lock()
	for _, d := range rt.draining {
		started, ok := open[d.tag]
		// Connections through a replaced tag that started later went to the
		// new handler.
		busy := ok && (d.registered || started <= d.since)
		if !busy && !(d.registered && o.e.active(d.tag)) {
			idle = append(idle, d)
		}
	}
	o.mu.Unlock()
	for _, d := range idle {
		o.endDrain(rt, d)
	}
	if over := len(rt.draining) - rt.cfg.maxOutbounds; over > 0 {
		oldest := slices.SortedFunc(maps.Values(rt.draining), func(a, b *drainingOutbound) int { return cmp.Compare(a.seq, b.seq) })
		for _, d := range oldest[:over] {
			o.endDrain(rt, d)
		}
	}
}

// oldestConnections returns, per outbound, when its oldest open user
// connection started.
func (o *Observer) oldestConnections() map[string]int64 {
	o.trackMu.Lock()
	defer o.trackMu.Unlock()
	m := map[string]int64{}
	for t := range o.trackers {
		if s, ok := m[t.tag]; !ok || t.started < s {
			m[t.tag] = t.started
		}
	}
	return m
}

// settleAllDrains ends the drains nothing waits for any more, of every provider.
func (o *Observer) settleAllDrains() {
	o.applyMu.Lock()
	defer o.applyMu.Unlock()
	if o.applyClosed {
		return
	}
	for _, rt := range o.providers {
		o.settleDrains(rt)
	}
}

func (o *Observer) finishDrain(rt *providerRuntime, d *drainingOutbound) {
	o.applyMu.Lock()
	defer o.applyMu.Unlock()
	if o.applyClosed {
		return
	}
	o.endDrain(rt, d)
}

// endDrain removes and closes a draining outbound. The caller holds applyMu.
func (o *Observer) endDrain(rt *providerRuntime, d *drainingOutbound) {
	if rt.draining[d.handler] != d {
		return
	}
	d.timer.Stop()
	delete(rt.draining, d.handler)
	registered := d.registered && o.ohm.GetHandler(d.tag) == d.handler
	if registered {
		_ = o.ohm.RemoveHandler(o.ctx, d.tag)
		delete(rt.built, d.tag)
		// The handler registered its traffic counters by tag; they go with it.
		if o.stats != nil {
			for _, dir := range [...]string{"uplink", "downlink"} {
				_ = o.stats.UnregisterCounter("outbound>>>" + d.tag + ">>>traffic>>>" + dir)
			}
		}
	}
	o.mu.Lock()
	if registered {
		o.e.dropProviderOutbound(d.tag, o.now())
	}
	rt.p.draining = len(rt.draining)
	o.e.emit(o.now(), "provider_drained", map[string]any{"provider": rt.cfg.tag, "outbound": d.tag, "replaced": !d.registered})
	o.mu.Unlock()
	common.Close(d.handler)
}

// closeProviders stops drains on shutdown. Registered outbounds are closed
// by the outbound manager; replaced ones only this observer knows about.
func (o *Observer) closeProviders() {
	o.applyMu.Lock()
	defer o.applyMu.Unlock()
	o.applyClosed = true
	for _, rt := range o.providers {
		for _, d := range rt.draining {
			d.timer.Stop()
			if !d.registered {
				common.Close(d.handler)
			}
		}
		clear(rt.draining)
	}
}

// RequestRefresh asks the provider with the tag, or every provider when the
// tag is empty, to fetch its set as soon as its limits allow. It returns
// false when there is no such provider.
func (o *Observer) RequestRefresh(provider string) bool {
	o.mu.Lock()
	ok := o.e.requestRefresh(provider)
	o.mu.Unlock()
	if ok {
		o.wake()
	}
	return ok
}
