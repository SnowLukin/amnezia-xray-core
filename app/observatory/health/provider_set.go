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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
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
	// Its time ran out while a balancer was pinned to it: it ends once no
	// balancer is.
	overdue bool
}

// providerAnswer is a parsed provider response in protocol 1:
//
//	{"protocol": 1, "outbounds": [...], "groups": [...], "nextRefreshIn": "2h", "version": "...",
//	 "expiresAt": {"tag": "2026-10-09T12:00:00Z"}, "serverTime": "2026-10-08T12:00:00Z",
//	 "headers": {"Authorization": "Bearer ..."}, "baseVersion": "..."}
type providerAnswer struct {
	// The outbounds as sent, handed to the config loader in one piece.
	outbounds        json.RawMessage
	heads            []outboundHead
	groupsRaw        json.RawMessage
	paths            []*PathDefinition
	groups           []*PathGroup
	nextRefreshIn    time.Duration
	hasNextRefreshIn bool
	version          string
	// Rules, balancers and the domain strategy in the format of the config
	// file; withStrategy tells whether the strategy was there at all.
	routing      json.RawMessage
	withStrategy bool
	// When the leases of outbounds end, by the tag as sent, and the clock of
	// the backend they count from.
	expiresAt  map[string]time.Time
	serverTime time.Time
	// The Date of the HTTP answer plus its Age, and when it arrived with
	// the monotonic reading; zero for an answer that came another way.
	date     time.Time
	received time.Time
	// Headers to put over those of the config, by canonical name; nil
	// without the key.
	headers map[string]string
	// The base config the backend serves now; empty without the key.
	baseVersion string
	// The answer is the initial set of the config. A set the host saved
	// has receivedAt, the wall clock it arrived by, and is age old.
	initial    bool
	receivedAt time.Time
	age        time.Duration
	// The answer as it came, for ProviderSet once it applied. Parsing
	// copies nothing more of it.
	body []byte
}

type outboundHead struct {
	Tag       string          `json:"tag"`
	Protocol  string          `json:"protocol"`
	Selection json.RawMessage `json:"selection"`
}

// Keys of an answer in protocol 1. The set keys make up the set, and an
// answer without a version is named by a hash of them. The meta keys only
// say how to take the set: an answer that differs in them alone is the same
// set. Any other key refuses the whole answer, so a backend never takes a
// key for applied that the core threw away; a new key is protocol 2.
var (
	setKeys  = map[string]bool{"outbounds": true, "groups": true}
	metaKeys = map[string]bool{"protocol": true, "version": true, "nextRefreshIn": true,
		"expiresAt": true, "serverTime": true, "headers": true, "baseVersion": true}
)

// answerError is an answer that is a JSON object but breaks protocol 1. The
// backend itself answered, so no other via is tried: it leads to the same
// backend.
type answerError struct{ msg string }

const answerErrorPrefix = "invalid answer: "

func (e *answerError) Error() string { return answerErrorPrefix + e.msg }

// answerErrorf words the refusal of an answer. A reason may quote a value of
// the answer, which may be as large as the answer; it goes into events,
// Status and the next request, so it is cut to maxFailureText in all.
func answerErrorf(format string, args ...any) error {
	return &answerError{msg: cutText(fmt.Sprintf(format, args...), maxFailureText-len(answerErrorPrefix))}
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
	return readAnswer(body, false)
}

// parseInitial reads the initial set of the config: an answer, which may
// add receivedAt when the host saved it.
func parseInitial(body []byte) (*providerAnswer, error) {
	return readAnswer(body, true)
}

func readAnswer(body []byte, initial bool) (*providerAnswer, error) {
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(body, &sections); err != nil {
		return nil, err
	}
	if sections == nil {
		return nil, errors.New("not a JSON object")
	}
	// The protocol first: an answer of a later one brings keys of its own,
	// and the backend has to learn which protocol the core speaks, not which
	// of those keys it does not know.
	if p := sections["protocol"]; !isNull(p) {
		var n float64
		if p[0] == '"' || json.Unmarshal(p, &n) != nil || n != providerProtocol {
			return nil, answerErrorf("protocol %s is not supported", cut(string(p)))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(sections)) {
		switch {
		case key == "healthObservatory":
			return nil, answerErrorf("healthObservatory is not accepted in an answer")
		case key == "receivedAt" && initial:
		case !setKeys[key] && !metaKeys[key]:
			return nil, answerErrorf("unknown key %q", cut(key))
		}
	}
	a := &providerAnswer{outbounds: sections["outbounds"], initial: initial}
	if isNull(a.outbounds) {
		return nil, answerErrorf("no outbounds")
	}
	if err := json.Unmarshal(a.outbounds, &a.heads); err != nil {
		return nil, answerErrorf("outbounds: %v", err)
	}
	if len(a.heads) == 0 {
		return nil, answerErrorf("no outbounds")
	}
	a.groupsRaw = sections["groups"]
	next, hasNext, err := parseRefreshIn(sections["nextRefreshIn"])
	if err != nil {
		return nil, answerErrorf("%v", err)
	}
	a.nextRefreshIn = next
	a.hasNextRefreshIn = hasNext
	// A time the core cannot read refuses the answer: a lease taken for
	// none would keep an outbound the backend meant to end.
	if raw := sections["expiresAt"]; !isNull(raw) {
		var ends map[string]json.RawMessage
		if err := json.Unmarshal(raw, &ends); err != nil {
			return nil, answerErrorf("expiresAt: %v", err)
		}
		a.expiresAt = make(map[string]time.Time, len(ends))
		for _, tag := range slices.Sorted(maps.Keys(ends)) {
			end, ok := answerTime(ends[tag])
			if !ok {
				return nil, answerErrorf("expiresAt of %s: %s is not a time in RFC 3339 with a zone", cut(tag), cut(string(ends[tag])))
			}
			a.expiresAt[tag] = end
		}
	}
	if raw := sections["serverTime"]; !isNull(raw) {
		t, ok := answerTime(raw)
		if !ok {
			return nil, answerErrorf("serverTime: %s is not a time in RFC 3339 with a zone", cut(string(raw)))
		}
		a.serverTime = t
	}
	if raw := sections["receivedAt"]; initial && !isNull(raw) {
		t, ok := answerTime(raw)
		if !ok {
			return nil, answerErrorf("receivedAt: %s is not a time in RFC 3339 with a zone", cut(string(raw)))
		}
		a.receivedAt = t
	}
	if raw := sections["headers"]; !isNull(raw) {
		h, err := parseHeaders(raw)
		if err != nil {
			return nil, err
		}
		a.headers = h
	}
	if raw := sections["baseVersion"]; !isNull(raw) {
		// Only a name: the core compares it with its base, nothing more.
		if json.Unmarshal(raw, &a.baseVersion) != nil || len(a.baseVersion) > maxBaseVersion {
			return nil, answerErrorf("baseVersion must be a string of at most %d bytes", maxBaseVersion)
		}
	}
	if v := sections["version"]; !isNull(v) {
		if json.Unmarshal(v, &a.version) != nil || len(a.version) > maxBaseVersion {
			return nil, answerErrorf("version must be a string of at most %d bytes", maxBaseVersion)
		}
	}

	if a.version == "" {
		a.version = setHash(sections)
	}
	a.body = body
	return a, nil
}

var domainStrategies = []string{"AsIs", "IPIfNonMatch", "IPOnDemand"}

// answerTime reads a JSON string in RFC 3339 with a zone, fractions of a
// second allowed.
func answerTime(raw json.RawMessage) (time.Time, bool) {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

// parseAge reads the Age a cache adds to an answer it kept, in whole
// seconds; anything else counts as none.
func parseAge(value string) time.Duration {
	secs, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(min(secs, int64(maxLease/time.Second))) * time.Second
}

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

// parseRefreshIn accepts a Go duration string such as "90m".
func parseRefreshIn(raw json.RawMessage) (time.Duration, bool, error) {
	if len(raw) == 0 {
		return 0, false, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, false, errors.New("nextRefreshIn must be a duration string")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, false, errors.New("nextRefreshIn must be a duration string")
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, false, fmt.Errorf("nextRefreshIn: %w", err)
	}
	return max(d, 0), true, nil
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
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		for key := range entry {
			if strings.EqualFold(key, "selection") {
				delete(entry, key)
			}
		}
	}
	handlerOutbounds, err := json.Marshal(entries)
	if err != nil {
		return nil, err
	}
	c, err := core.LoadConfig("json", io.MultiReader(
		strings.NewReader(`{"outbounds":`), bytes.NewReader(handlerOutbounds), strings.NewReader(`}`)))
	if err != nil {
		return nil, err
	}
	return c.Outbound, nil
}

// loadRouting builds rules and balancers with the JSON config loader of the
// core, like loadOutbounds.
func loadRouting(raw json.RawMessage) (*router.Config, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	var balancers []map[string]json.RawMessage
	if len(doc["balancers"]) > 0 {
		if err := json.Unmarshal(doc["balancers"], &balancers); err != nil {
			return nil, err
		}
	}
	// The config loader needs a path for each leastCost balancer. These
	// placeholders are replaced with the declared paths before routing applies.
	fake := make([]map[string]any, 0, len(balancers))
	seenTags := map[string]bool{}
	for i, b := range balancers {
		var tag string
		if err := json.Unmarshal(b["tag"], &tag); err != nil || tag == "" {
			return nil, fmt.Errorf("balancer %d needs tag", i+1)
		}
		if seenTags[tag] {
			return nil, fmt.Errorf("duplicate balancer tag %s", tag)
		}
		seenTags[tag] = true
		var strategy struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(b["strategy"], &strategy); err != nil {
			return nil, err
		}
		if !strings.EqualFold(strategy.Type, "leastcost") {
			continue
		}
		if _, exists := b["selector"]; exists {
			return nil, fmt.Errorf("leastCost selector is not supported")
		}
		fake = append(fake, map[string]any{"tag": fmt.Sprintf("__routing_check_%d", i), "protocol": "freedom", "path": map[string]any{"balancer": tag, "cost": 1}})
	}
	wrapper := map[string]any{"routing": json.RawMessage(raw), "outbounds": fake, "healthObservatory": map[string]any{}}
	body, err := json.Marshal(wrapper)
	if err != nil {
		return nil, err
	}
	c, err := core.LoadConfig("json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for _, app := range c.App {
		if inst, err := app.GetInstance(); err == nil {
			if rc, ok := inst.(*router.Config); ok {
				for _, b := range rc.BalancingRule {
					if strings.EqualFold(b.Strategy, "leastcost") {
						b.OutboundSelector = nil
					}
				}
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
func (o *Observer) checkRouting(rt *providerRuntime, a *providerAnswer, rc *router.Config, setTags []string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	exists := func(tag string) bool {
		return slices.Contains(setTags, tag) || (o.ohm != nil && o.ohm.GetHandler(tag) != nil)
	}
	balancers := map[string]*router.BalancingRule{}
	for _, b := range rc.BalancingRule {
		if _, exists := balancers[b.Tag]; exists {
			return fmt.Errorf("duplicate balancer tag %s", b.Tag)
		}
		balancers[b.Tag] = b
		if b.FallbackTag != "" && !exists(b.FallbackTag) {
			return fmt.Errorf("balancer %s falls back to unknown outbound %s", b.Tag, b.FallbackTag)
		}
		if !strings.EqualFold(b.Strategy, "leastcost") {
			continue
		}
		if len(b.OutboundSelector) > 0 {
			return fmt.Errorf("balancer %s: leastCost selector is not supported", b.Tag)
		}
		var base *SelectorConfig
		if selected := o.e.selectors[b.Tag]; selected != nil {
			base = selected.baseRaw
		}
		var settings *SelectorConfig
		if b.StrategySettings != nil {
			inst, err := b.StrategySettings.GetInstance()
			if err != nil {
				return fmt.Errorf("balancer %s: %w", b.Tag, err)
			}
			settings, _ = inst.(*SelectorConfig)
		}
		if settings == nil && base != nil {
			settings = proto.Clone(base).(*SelectorConfig)
		}
		if settings == nil {
			settings = &SelectorConfig{}
		}
		settings.Paths, settings.Groups = nil, nil
		if base != nil {
			settings.Paths = append(settings.Paths, base.Paths...)
			settings.Groups = append(settings.Groups, base.Groups...)
		}
		for _, p := range o.e.providers {
			if p.cfg.balancer != b.Tag {
				continue
			}
			if p == rt.p {
				settings.Paths = append(settings.Paths, a.paths...)
				settings.Groups = append(settings.Groups, a.groups...)
			} else {
				settings.Paths = append(settings.Paths, p.pathDefs...)
				settings.Groups = append(settings.Groups, p.groupDefs...)
			}
			b.OutboundSelector = append(b.OutboundSelector, p.cfg.prefix)
		}
		for _, path := range settings.Paths {
			if path.Balancer == b.Tag {
				b.OutboundSelector = append(b.OutboundSelector, path.Tag)
			}
		}
		if len(b.OutboundSelector) == 0 {
			return fmt.Errorf("balancer %s has no paths", b.Tag)
		}
		b.StrategySettings = serial.ToTypedMessage(settings)
	}
	if b := balancers[rt.cfg.balancer]; b == nil || !strings.EqualFold(b.Strategy, "leastcost") {
		return fmt.Errorf("routing drops the leastCost balancer %s the provider feeds", rt.cfg.balancer)
	}
	ruleTags := map[string]bool{}
	for i, r := range rc.Rule {
		if tag := r.GetRuleTag(); tag != "" {
			if ruleTags[tag] {
				return fmt.Errorf("duplicate ruleTag %s", tag)
			}
			ruleTags[tag] = true
		}
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
	found := false
	o.s.paths.Range(func(key, _ any) bool {
		if strings.HasPrefix(key.(string), sel) {
			found = true
			return false
		}
		return true
	})
	if found {
		return true
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
	body, bodyErr := o.requestBody(rt, j)
	if bodyErr != nil {
		r.err = "provider request exceeds 256 KiB"
		r.class = "request_too_large"
		r.code = "request_too_large"
		o.emit("provider_fetch", map[string]any{"provider": rt.cfg.tag, "trigger": j.trigger.String(), "is_ok": false, "code": r.code})
	} else if res, err := o.fetchSet(rt, j, body); err != nil {
		r.err = err.Error()
		r.retryAfter = res.retryAfter
		r.status = res.status
		r.code = res.code
		r.via = res.via
		// A refused answer is told by its reason, a failed request by class.
		r.class = cmp.Or(res.class, "invalid_answer")
	} else if changed, err := o.applySet(rt, res.answer, j); err != nil {
		r.err = err.Error()
		r.status = res.status
		r.class = "invalid_set"
	} else {
		r.ok = true
		r.changed = changed
		r.nextRefreshIn = res.answer.nextRefreshIn
		r.hasNextRefreshIn = res.answer.hasNextRefreshIn
	}
	o.mu.Lock()
	o.e.onRefreshDone(j, r, o.now())
	o.mu.Unlock()
	o.wake()
}

// fetchResult is what one request through one via brought.
type fetchResult struct {
	answer *providerAnswer
	status int
	bytes  int
	// The pause a busy backend asked for with Retry-After, at most
	// backoff.max; zero without one.
	retryAfter time.Duration
	// The code of a protocol error body.
	code string
	// The via entry of the attempt.
	via string
	// The Date of the answer; zero without one.
	date time.Time
	// The backend itself answered, in the protocol or by asking to wait.
	// Another via leads to the same backend, so the attempts end.
	backend bool
	// What failed, for the next request to tell the backend: timeout,
	// network, status, too_large or broken_json; empty for an answer
	// against the protocol, which is told by its reason.
	class string
}

// fetchSet requests the set through the via outbounds in turn until one
// brings a well-formed answer or the backend itself answers. An answer that
// a firewall, a portal or a server on the way may have given goes on to the
// next via. Every attempt is an event and sends the same body.
func (o *Observer) fetchSet(rt *providerRuntime, j *refreshJob, body []byte) (fetchResult, error) {
	via := rt.cfg.via
	if len(via) == 0 {
		via = []string{""}
	}
	var last fetchResult
	var lastErr error
	asked := false
	for i, v := range via {
		o.mu.Lock()
		tag, skip := o.e.resolveVia(v, o.now())
		low := o.e.memoryLow
		o.mu.Unlock()
		if skip == "" && tag != "" && o.ohm.GetHandler(tag) == nil {
			skip = "no such outbound"
		}
		f := map[string]any{"provider": rt.cfg.tag, "trigger": j.trigger.String(), "retry": j.retry,
			"attempt": i + 1, "via": v, "outbound": tag, "method": rt.cfg.method, "refresh": j.refresh}
		if skip != "" {
			f["is_ok"] = false
			f["error"] = skip
			o.emit("provider_fetch", f)
			// What a via that was asked met tells the next request more than
			// an entry skipped after it.
			if !asked {
				last, lastErr = fetchResult{class: "network"}, errors.New(skip)
			}
			continue
		}
		asked = true
		o.fetchingVia(tag, 1)
		start := time.Now()
		r, err := o.fetchOnce(rt.cfg, j.headers, tag, body)
		o.fetchingVia(tag, -1)
		r.via = v
		// An answer against the protocol still came from the backend: the
		// request did its job and the refusal is the apply's, so the report
		// counts it once, as rejected.
		refused := isAnswerError(err)
		f["is_ok"] = err == nil || refused
		f["status"] = r.status
		f["ms"] = time.Since(start).Milliseconds()
		f["bytes"] = r.bytes
		if r.code != "" {
			f["code"] = r.code
		}
		if r.retryAfter > 0 {
			f["retry_after_ms"] = r.retryAfter.Milliseconds()
		}
		if err != nil && !refused {
			f["error"] = err.Error()
		}
		o.emit("provider_fetch", f)
		if err == nil {
			return r, nil
		}
		if refused {
			o.emit("provider_apply", map[string]any{"provider": rt.cfg.tag, "trigger": j.trigger.String(),
				"is_ok": false, "error": err.Error()})
		}
		last, lastErr = r, err
		if r.backend {
			break
		}
		if low {
			// One request at a time is enough; the backoff brings the next.
			break
		}
	}
	return last, lastErr
}

// fetchingVia counts a provider request through an outbound while it runs,
// so the outbound does not sleep under it; a request without via goes the
// way of the routing.
func (o *Observer) fetchingVia(tag string, delta int) {
	if tag == "" {
		return
	}
	o.mu.Lock()
	o.e.fetchingVia(tag, delta, o.now())
	o.mu.Unlock()
}

func (o *Observer) fetchOnce(cfg *providerSettings, headers map[string]string, tag string, body []byte) (fetchResult, error) {
	r := fetchResult{class: "network"}
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
	// Every attempt reads the same bytes afresh.
	var payload io.Reader
	if cfg.method == http.MethodPost {
		payload = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, cfg.method, cfg.url, payload)
	if err != nil {
		return r, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			r.class = "timeout"
			return r, errors.New("timeout")
		}
		// Events go to analytics; the URL may name more than it should.
		if ue, ok := err.(*url.Error); ok {
			err = ue.Err
		}
		return r, err
	}
	defer resp.Body.Close()
	r.status = resp.StatusCode
	r.class = "status"
	if d, err := http.ParseTime(resp.Header.Get("Date")); err == nil {
		r.date = d
	}
	if resp.StatusCode != http.StatusOK {
		// Only the protocol and the code of an error body are read: its
		// message may echo the request and is kept nowhere. A portal or a
		// proxy may send a status and then a body that never ends; it must
		// not hold the next via for the whole timeout, so the read ends
		// errorBodyWait after the status, with whatever came by then.
		wait := time.AfterFunc(errorBodyWait, cancel)
		errorBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		wait.Stop()
		r.bytes = len(errorBody)
		r.code = errorCode(errorBody)
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
			if ra := parseRetryAfter(resp.Header.Get("Retry-After"), r.date, o.now()); ra > 0 {
				r.retryAfter = min(ra, cfg.backoff.max)
			}
		}
		r.backend = r.code != "" || r.retryAfter > 0
		return r, fmt.Errorf("status %d", resp.StatusCode)
	}
	answer, err := io.ReadAll(io.LimitReader(resp.Body, cfg.maxBytes+1))
	received := o.now()
	r.bytes = len(answer)
	if err != nil {
		r.class = "network"
		if ctx.Err() == context.DeadlineExceeded {
			r.class = "timeout"
			err = errors.New("timeout")
		}
		return r, err
	}
	if int64(len(answer)) > cfg.maxBytes {
		r.class = "too_large"
		return r, fmt.Errorf("answer larger than %d bytes", cfg.maxBytes)
	}
	r.answer, err = parseAnswer(answer)
	if err == nil {
		err = checkRotation(r.answer, cfg)
	}
	switch {
	case isAnswerError(err):
		r.backend = true
		r.class = ""
	case err != nil:
		r.class = "broken_json"
		err = fmt.Errorf("invalid answer: %w", err)
	default:
		r.class = ""
		// The leases count from the clock of the answer, as it arrived.
		r.answer.received = received
		if !r.date.IsZero() {
			r.answer.date = r.date.Add(parseAge(resp.Header.Get("Age")))
		}
	}
	return r, err
}

// errorCode returns the code of a protocol error body,
// {"protocol": 1, "code": "<code>", "message": "..."}, or "" when the body
// is not one: a firewall or a portal answers with a status as well, but not
// in the protocol.
func errorCode(body []byte) string {
	var e struct {
		Protocol *float64 `json:"protocol"`
		Code     string   `json:"code"`
	}
	if json.Unmarshal(body, &e) != nil || e.Protocol == nil || *e.Protocol < 1 || *e.Protocol != math.Trunc(*e.Protocol) {
		return ""
	}
	// The code goes into events as it is, so it has to look like one.
	if len(e.Code) == 0 || len(e.Code) > 32 {
		return ""
	}
	for _, c := range e.Code {
		if (c < 'a' || c > 'z') && c != '_' {
			return ""
		}
	}
	return e.Code
}

// parseRetryAfter reads Retry-After as seconds or as an HTTP date. A date
// counts from the Date of the same answer, so a wrong clock of the device
// does not stretch or cut the pause, or from the wall clock without one.
// Zero, a date gone by and anything unreadable count as no header.
func parseRetryAfter(value string, date, wall time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(value, 10, 64); err == nil {
		// Beyond a day it is the backoff ceiling anyway; the cap keeps the
		// product from overflowing.
		return time.Duration(min(max(secs, 0), int64(24*time.Hour/time.Second))) * time.Second
	}
	at, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	if date.IsZero() {
		date = wall
	}
	return max(at.Sub(date), 0)
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
		// Building quotes tags, protocols and what the config loader found
		// wrong, all from the answer: cut like the reason of an answerError.
		err = errors.New(cutText(err.Error(), maxFailureText))
		fields["is_ok"] = false
		fields["error"] = err.Error()
		o.emit("provider_apply", fields)
		return false, err
	}
	if o.applyClosed {
		return fail(errors.New("closed"))
	}
	o.mu.Lock()
	err := o.e.checkHeaderRoom(rt.p, a, j.headersGen)
	o.mu.Unlock()
	if err != nil {
		return fail(err)
	}
	if _, err := checkHeads(rt.cfg, a); err != nil {
		return fail(err)
	}
	o.mu.Lock()
	err = o.checkGroupOwnership(rt, a)
	o.mu.Unlock()
	if err != nil {
		return fail(err)
	}
	o.mu.Lock()
	modeChanged := false
	for _, path := range a.paths {
		if o.e.isProbeOnly(path.Tag) != path.IsProbeOnly {
			modeChanged = true
			break
		}
	}
	o.mu.Unlock()
	if a.version == rt.version && !modeChanged {
		// The version names the set, not the answer: the most common answer
		// while leases run is the same set with later ends, which renews
		// them without rebuilding anything. Its outbounds are checked all
		// the same, short of building: the answer is kept for the next
		// start, which would refuse it.
		if _, err := checkHeads(rt.cfg, a); err != nil {
			return fail(err)
		}
		if err := validateAnswerProbes(o.s, a); err != nil {
			return fail(err)
		}
		kept := compactAnswer(a.body)
		now := o.now()
		o.mu.Lock()
		leases, moved := o.e.acceptLeases(rt.p, rt.p.tags, leasesOf(a, rt.cfg.prefix), now)
		if moved {
			o.e.refreshCandidates(now)
		}
		gen := rt.p.headersGen
		names, stale := o.e.takeAnswerHeaders(rt.p, a.headers, j.headersGen, headersBy(j), now)
		based := o.e.takeBase(rt.p, a, now)
		o.e.keepAnswer(rt.p, a, kept, leases.ends || rt.p.headersGen != gen || based, now)
		fields["is_ok"] = true
		fields["is_changed"] = false
		fields["unchanged"] = len(rt.p.tags)
		leaseFields(fields, leases, rt.p)
		headerFields(fields, names, stale)
		baseFields(fields, a)
		o.e.emit(now, "provider_apply", fields)
		o.mu.Unlock()
		return false, nil
	}
	if err := validateAnswerProbes(o.s, a); err != nil {
		return fail(err)
	}
	var staged []string
	// An outbound handler binds its observer when constructed, before the
	// provider set is committed. Only new exact tags need temporary visibility.
	for _, path := range a.paths {
		if _, present := o.s.paths.Load(path.Tag); present {
			continue
		}
		o.s.paths.Store(path.Tag, pathSettings{isProbeOnly: path.IsProbeOnly})
		staged = append(staged, path.Tag)
	}
	defer func() {
		for _, tag := range staged {
			o.s.paths.Delete(tag)
		}
	}()
	changes, unchanged, err := o.buildSet(rt, a)
	if err != nil {
		return fail(err)
	}
	var routing *router.Config
	if len(a.routing) > 0 {
		if !rt.cfg.routing || o.replaceRules == nil {
			// Whether to take routing from a provider is the choice of the
			// client: the set applies without it.
			fields["is_routing_ignored"] = true
		} else {
			setTags := make([]string, 0, len(changes)+len(unchanged))
			for _, c := range changes {
				setTags = append(setTags, c.tag)
			}
			setTags = append(setTags, unchanged...)
			if routing, err = o.parseRouting(a.routing); err == nil {
				err = o.checkRouting(rt, a, routing, setTags)
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
	// In the order of the answer, as the request tells the backend its set.
	tags := make([]string, 0, len(a.heads))
	for _, h := range a.heads {
		tags = append(tags, rt.cfg.prefix+h.Tag)
	}
	var added, replaced []*outboundChange
	for _, c := range changes {
		inSet[c.tag] = true
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
	demoted := map[string]pathSettings{}
	wasRetiring := map[string]bool{}
	o.mu.Lock()
	o.e.replacingTags(reset, o.now())
	for _, path := range a.paths {
		metadata, exists := o.s.paths.Load(path.Tag)
		if !exists || !path.IsProbeOnly {
			continue
		}
		old := metadata.(pathSettings)
		if old.isProbeOnly {
			continue
		}
		demoted[path.Tag] = old
		wasRetiring[path.Tag] = o.e.retiring[path.Tag]
		// Select reads the mode without a lock, before the handler can be replaced.
		o.s.paths.Store(path.Tag, pathSettings{probe: old.probe, isProbeOnly: true, labels: old.labels})
		o.e.retiring[path.Tag] = true
	}
	if len(demoted) > 0 {
		o.e.refreshCandidates(o.now())
	}
	o.mu.Unlock()
	if err := o.registerSet(added, replaced); err != nil {
		o.mu.Lock()
		clear(o.e.replacing)
		for tag, old := range demoted {
			o.s.paths.Store(tag, old)
			if wasRetiring[tag] {
				o.e.retiring[tag] = true
			} else {
				delete(o.e.retiring, tag)
			}
		}
		if len(demoted) > 0 {
			o.e.refreshCandidates(o.now())
		}
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
	for _, path := range a.paths {
		if _, ok := demoted[path.Tag]; ok {
			revive = append(revive, path.Tag)
		}
	}
	rt.version = a.version
	// Outside the observer lock: rebuilt leastCost balancers ask the observer
	// for their selectors.
	if routing != nil {
		if err := o.replaceRules(routing, a.withStrategy); err != nil {
			fields["routing_error"] = cutText(err.Error(), maxFailureText)
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

	kept := compactAnswer(a.body)
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
	// A HandlerService replacement can finish an old drain before this
	// answer arrives. Every tag included again belongs to the live set.
	for _, tag := range tags {
		if o.e.retiring[tag] && !slices.Contains(revive, tag) {
			revive = append(revive, tag)
		}
	}
	// Before the balancers see the set: an outbound whose lease ended on
	// arrival is never taken as new.
	for _, tag := range rt.p.tags {
		l := rt.p.leases[tag]
		if !inSet[tag] && (o.e.expired[tag] || (l != nil && l.left(now) <= 0)) {
			if rt.p.retiredExpired == nil {
				rt.p.retiredExpired = map[string]bool{}
			}
			rt.p.retiredExpired[tag] = true
		}
	}
	for _, tag := range tags {
		delete(rt.p.retiredExpired, tag)
	}
	leases, _ := o.e.acceptLeases(rt.p, tags, leasesOf(a, rt.cfg.prefix), now)
	o.e.applyProviderSet(rt.p, providerChange{
		tags: tags, paths: a.paths, groups: a.groups, added: addedTags, retire: removed, revive: revive, reset: reset,
	}, now)
	staged = nil
	rt.p.version = a.version
	rt.p.draining = len(rt.draining)
	names, stale := o.e.takeAnswerHeaders(rt.p, a.headers, j.headersGen, headersBy(j), now)
	o.e.takeBase(rt.p, a, now)
	o.e.keepAnswer(rt.p, a, kept, true, now)
	fields["is_ok"] = true
	fields["is_changed"] = true
	fields["added"] = len(added)
	fields["replaced"] = len(replaced)
	fields["removed"] = len(removed)
	fields["unchanged"] = len(unchanged)
	fields["was_exhausted"] = exhausted
	leaseFields(fields, leases, rt.p)
	headerFields(fields, names, stale)
	baseFields(fields, a)
	o.e.emit(now, "provider_apply", fields)
	o.mu.Unlock()
	o.settleDrains(rt)
	return true, nil
}

// leasesOf gives the leases of an answer by full tag. A lease of a tag the
// answer does not list is ignored.
func leasesOf(a *providerAnswer, prefix string) leaseAnswer {
	la := leaseAnswer{serverTime: a.serverTime, date: a.date, at: a.received, initial: a.initial}
	if !a.receivedAt.IsZero() {
		// The leases ran on while the set was kept: they count from the
		// clock of the backend when it arrived, plus the time since.
		la.clock = "saved"
		la.age = a.age
		la.kept = true
		base := a.serverTime
		if base.IsZero() {
			base = a.receivedAt
		}
		la.serverTime = base.Add(a.age)
	}
	for _, h := range a.heads {
		if end, ok := a.expiresAt[h.Tag]; ok {
			if la.ends == nil {
				la.ends = map[string]time.Time{}
			}
			la.ends[prefix+h.Tag] = end
		}
	}
	return la
}

// leaseFields tells in provider_apply what the answer did to the leases,
// when it named any.
func leaseFields(f map[string]any, c leaseCounts, p *provider) {
	if c.leases == 0 {
		return
	}
	f["leases"] = c.leases
	f["renewed"] = c.renewed
	f["expired"] = c.expired
	f["skew_ms"] = p.skew.Milliseconds()
	f["clock"] = p.clock
}

// checkHeads checks what the answer says of its outbounds without building
// them: their number, tags, protocols and costs. It returns their full tags.
func checkHeads(cfg *providerSettings, a *providerAnswer) ([]string, error) {
	if len(a.heads) > cfg.maxOutbounds {
		return nil, fmt.Errorf("set of %d outbounds, at most %d allowed", len(a.heads), cfg.maxOutbounds)
	}
	if err := validateAnswerPaths(cfg, a); err != nil {
		return nil, err
	}
	tags := make([]string, len(a.heads))
	seen := map[string]bool{}
	for i, head := range a.heads {
		if head.Tag == "" {
			return nil, fmt.Errorf("outbound %d has no tag", i+1)
		}
		if len(head.Tag) > 128 || len(cfg.prefix+head.Tag) > 128 {
			return nil, fmt.Errorf("outbound %d needs a tag of at most 128 bytes including its prefix", i+1)
		}
		if !slices.Contains(cfg.protocols, strings.ToLower(head.Protocol)) {
			return nil, fmt.Errorf("outbound %s: protocol %q is not allowed", head.Tag, head.Protocol)
		}
		if seen[head.Tag] {
			return nil, fmt.Errorf("duplicate tag %s", head.Tag)
		}
		seen[head.Tag] = true
		tags[i] = cfg.prefix + head.Tag
	}
	return tags, nil
}

func (o *Observer) checkGroupOwnership(rt *providerRuntime, a *providerAnswer) error {
	target := o.e.selectors[rt.cfg.balancer]
	if target == nil {
		return fmt.Errorf("unknown provider balancer")
	}
	flags := target.cfg.raw.GetFeatures()
	if flags == nil {
		flags = &SelectorFeatureConfig{}
	}
	if len(a.groups) > 0 && !featureValue(flags.Groups, false) {
		return fmt.Errorf("provider groups require groups.isEnabled")
	}
	for _, path := range a.paths {
		if path.ReturnIsEnabled != nil && *path.ReturnIsEnabled && !featureValue(flags.ReturnToCheaper, false) {
			return fmt.Errorf("selection returnToCheaper requires balancer returnToCheaper.isEnabled")
		}
	}
	for _, group := range a.groups {
		if group.ReturnIsEnabled != nil && *group.ReturnIsEnabled && !featureValue(flags.ReturnToCheaper, false) {
			return fmt.Errorf("group returnToCheaper requires balancer returnToCheaper.isEnabled")
		}
	}
	for _, g := range a.groups {
		if s := o.e.selectors[rt.cfg.balancer]; s != nil && s.baseRaw != nil {
			for _, base := range s.baseRaw.Groups {
				if base.Name == g.Name {
					return fmt.Errorf("group %s belongs to the base config", g.Name)
				}
			}
		}
		for _, other := range o.e.providers {
			if other == rt.p || other.cfg.balancer != rt.cfg.balancer {
				continue
			}
			for _, owned := range other.groupDefs {
				if owned.Name == g.Name {
					return fmt.Errorf("group %s belongs to provider %s", g.Name, other.cfg.tag)
				}
			}
		}
	}
	return nil
}

// buildSet validates the answer and creates handlers for outbounds that are
// new or changed. It returns them and the tags of outbounds that stay as
// they are. On error every created handler is closed.
func (o *Observer) buildSet(rt *providerRuntime, a *providerAnswer) ([]*outboundChange, []string, error) {
	tags, err := checkHeads(rt.cfg, a)
	if err != nil {
		return nil, nil, err
	}
	configs, err := o.parseOutbounds(a.outbounds)
	if err != nil {
		return nil, nil, fmt.Errorf("build: %w", err)
	}
	if len(configs) != len(tags) {
		return nil, nil, fmt.Errorf("build: %d outbounds of %d", len(configs), len(tags))
	}
	if err := validateAnswerProbes(o.s, a); err != nil {
		return nil, nil, err
	}
	modes := make(map[string]bool, len(a.paths))
	for _, path := range a.paths {
		modes[path.Tag] = path.IsProbeOnly
	}
	o.mu.Lock()
	changedModes := make(map[string]bool, len(a.paths))
	for tag, mode := range modes {
		changedModes[tag] = o.e.isProbeOnly(tag) != mode
	}
	o.mu.Unlock()
	var changes []*outboundChange
	var unchanged []string
	closeAll := func() {
		for _, c := range changes {
			common.Close(c.handler)
		}
	}
	for i, cfg := range configs {
		cfg.Tag = tags[i]
		cfg.IsProbeOnly = modes[cfg.Tag]
		if h := o.ohm.GetHandler(cfg.Tag); h != nil && !changedModes[cfg.Tag] && sameOutbound(h, rt.built[cfg.Tag], cfg) {
			unchanged = append(unchanged, cfg.Tag)
			continue
		}
		h, err := o.newHandler(cfg)
		if err != nil {
			closeAll()
			// By the tag the backend sent, as every other reason names
			// an outbound of the answer: the backend knows no prefix.
			return nil, nil, fmt.Errorf("build %s: %w", a.heads[i].Tag, err)
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
// open connections that no balancer chooses. An outbound that can sleep is
// asked as well: the observer forgets a connection idle for long, and
// Sleep refuses while one goes on through the tunnel; the drain then runs
// its time. Beyond maxOutbounds draining outbounds the oldest drains
// end early, so sets replaced one after another do not pile up handlers.
// The caller holds applyMu.
func (o *Observer) settleDrains(rt *providerRuntime) {
	open := o.oldestConnections()
	var idle, overdue []*drainingOutbound
	pinned := map[*drainingOutbound]bool{}
	o.mu.Lock()
	for _, d := range rt.draining {
		pinned[d] = d.registered && o.e.pinnedTo(d.tag)
		started, ok := open[d.tag]
		// Connections through a replaced tag that started later went to the
		// new handler.
		busy := ok && (d.registered || started <= d.since)
		switch {
		case d.overdue && !pinned[d]:
			overdue = append(overdue, d)
		case !busy && !(d.registered && o.e.active(d.tag)):
			idle = append(idle, d)
		}
	}
	o.mu.Unlock()
	for _, d := range overdue {
		o.endDrain(rt, d)
	}
	for _, d := range idle {
		if s := sleeperOf(d.handler); s != nil && !s.Sleep() {
			continue
		}
		o.endDrain(rt, d)
	}
	if over := len(rt.draining) - rt.cfg.maxOutbounds; over > 0 {
		oldest := slices.SortedFunc(maps.Values(rt.draining), func(a, b *drainingOutbound) int { return cmp.Compare(a.seq, b.seq) })
		for _, d := range oldest {
			if over == 0 {
				break
			}
			if pinned[d] {
				continue
			}
			o.endDrain(rt, d)
			over--
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
	for t := range o.closures {
		if t.state.Load() != trackerFinished {
			if at, ok := m[t.tag]; !ok || t.started < at {
				m[t.tag] = t.started
			}
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
	o.mu.Lock()
	pinned := d.registered && o.e.pinnedTo(d.tag)
	o.mu.Unlock()
	if pinned {
		// A pinned balancer stays on its outbound whatever happens to it.
		// Closed under the pin, the outbound would leave it a tag without
		// a handler: every connection refused, and no pinned_down to tell.
		// The drain ends once the pin does.
		d.overdue = true
		return
	}
	o.endDrain(rt, d)
	// The balancers that offered the outbound left it, which may fail them
	// over and queue probes; the loop runs those now, not at whatever wakes
	// it next.
	o.wake()
}

// endOverdueDrains ends the drains whose time ran out under a pin that is
// over now. The loop calls it once a pin ended or moved.
func (o *Observer) endOverdueDrains() {
	o.applyMu.Lock()
	defer o.applyMu.Unlock()
	if o.applyClosed {
		return
	}
	var ended []*drainingOutbound
	var of []*providerRuntime
	o.mu.Lock()
	for _, rt := range o.providers {
		for _, d := range rt.draining {
			if d.overdue && !o.e.pinnedTo(d.tag) {
				ended = append(ended, d)
				of = append(of, rt)
			}
		}
	}
	o.mu.Unlock()
	for i, d := range ended {
		o.endDrain(of[i], d)
	}
	if len(ended) > 0 {
		o.wake()
	}
}

// endDrain removes and closes a draining outbound. One that was registered
// and is no longer, which only HandlerService.RemoveOutbound does, was closed
// there and is not closed again. The caller holds applyMu.
func (o *Observer) endDrain(rt *providerRuntime, d *drainingOutbound) {
	if rt.draining[d.handler] != d {
		return
	}
	d.timer.Stop()
	delete(rt.draining, d.handler)
	current := o.ohm.GetHandler(d.tag)
	registered := d.registered && current == d.handler
	gone := d.registered && (registered || current == nil)
	if registered {
		_ = o.ohm.RemoveHandler(o.ctx, d.tag)
	}
	if d.registered {
		// HandlerService may have removed or replaced it before the drain
		// ended. Its cached config must not survive or describe another handler.
		delete(rt.built, d.tag)
	}
	if gone {
		// The handler registered its traffic counters by tag; they go with it.
		if o.stats != nil {
			for _, dir := range [...]string{"uplink", "downlink"} {
				_ = o.stats.UnregisterCounter("outbound>>>" + d.tag + ">>>traffic>>>" + dir)
			}
		}
	}
	o.mu.Lock()
	if gone {
		o.e.dropProviderOutbound(d.tag, o.now())
	}
	rt.p.draining = len(rt.draining)
	o.e.emit(o.now(), "provider_drained", map[string]any{"provider": rt.cfg.tag, "outbound": d.tag, "is_replaced": !d.registered})
	o.mu.Unlock()
	if registered || !d.registered {
		common.Close(d.handler)
	}
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

// RequestProviderRefresh asks the provider with the tag, or every provider when the
// tag is empty, to fetch its set as soon as its limits allow. It returns
// false when there is no such provider.
func (o *Observer) RequestProviderRefresh(provider string) bool {
	o.mu.Lock()
	ok := o.e.requestRefresh(provider)
	o.mu.Unlock()
	if ok {
		o.wake()
	}
	return ok
}
