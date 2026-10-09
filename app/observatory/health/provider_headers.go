package health

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"golang.org/x/net/http/httpguts"
)

// Header values go nowhere but into the requests of the provider: not into
// events, Status, the report, logs or errors. Only their names are told.

// Headers that belong to the transport of the request. Taken from an answer
// or the host they could break the framing of the request or send it
// elsewhere, so neither may set them.
var transportHeaders = map[string]bool{"Host": true, "Content-Length": true, "Content-Type": true,
	"Transfer-Encoding": true, "Connection": true, "Keep-Alive": true, "Te": true, "Trailer": true,
	"Upgrade": true, "Accept": true}

// CheckHeaderName tells why an answer, the initial set or the host may not
// set a provider header of this name, or nil when it may.
func CheckHeaderName(name string) error {
	if !httpguts.ValidHeaderFieldName(name) {
		return fmt.Errorf("header name %q is not a token", cut(name))
	}
	if canon := http.CanonicalHeaderKey(name); transportHeaders[canon] || strings.HasPrefix(canon, "Proxy-") {
		return fmt.Errorf("header %s cannot be set", canon)
	}
	return nil
}

// checkHeaderValue words what is wrong with a value by the name alone.
func checkHeaderValue(name, value string) error {
	switch {
	case value == "":
		return fmt.Errorf("header %s has no value", name)
	case len(value) > maxHeaderValue:
		return fmt.Errorf("header %s is longer than %d bytes", name, maxHeaderValue)
	case !httpguts.ValidHeaderFieldValue(value):
		return fmt.Errorf("header %s has a value that is not valid in HTTP", name)
	}
	return nil
}

// parseHeaders reads the headers of an answer by canonical name. Values are
// never quoted: the reason of a refusal goes into events and the next
// request.
func parseHeaders(raw []byte) (map[string]string, error) {
	var sent map[string]string
	if json.Unmarshal(raw, &sent) != nil || sent == nil {
		return nil, answerErrorf("headers must be an object of strings")
	}
	if len(sent) > maxRotatedHeaders {
		return nil, answerErrorf("headers: more than %d", maxRotatedHeaders)
	}
	out := make(map[string]string, len(sent))
	for _, name := range slices.Sorted(maps.Keys(sent)) {
		if err := CheckHeaderName(name); err != nil {
			return nil, answerErrorf("headers: %v", err)
		}
		canon := http.CanonicalHeaderKey(name)
		if _, twice := out[canon]; twice {
			return nil, answerErrorf("headers: %s given twice", canon)
		}
		if err := checkHeaderValue(canon, sent[name]); err != nil {
			return nil, answerErrorf("headers: %v", err)
		}
		out[canon] = sent[name]
	}
	return out, nil
}

// checkRotation refuses headers of an answer that apply.headers does not
// allow; the answer is refused whole, so the backend never takes a token
// for applied that the core threw away.
func checkRotation(a *providerAnswer, cfg *providerSettings) error {
	if a.headers == nil {
		return nil
	}
	if len(cfg.rotate) == 0 {
		return answerErrorf("headers are not accepted: apply.headers is empty")
	}
	for _, name := range slices.Sorted(maps.Keys(a.headers)) {
		if !slices.Contains(cfg.rotate, name) {
			return answerErrorf("header %s is not in apply.headers", name)
		}
	}
	return nil
}

// requestHeaders are the headers a refresh sends: those of the config, and
// over them the ones an answer or the host set, by canonical name, so that
// a rotated authorization replaces the Authorization of the config.
func (p *provider) requestHeaders() map[string]string {
	h := make(map[string]string, len(p.cfg.headers)+len(p.rotated))
	for name, v := range p.cfg.headers {
		h[http.CanonicalHeaderKey(name)] = v
	}
	maps.Copy(h, p.rotated)
	return h
}

// rotate puts headers over the rotated ones; an empty value removes one, so
// that the config applies again. A changed value starts a new generation of
// the headers and ends a pause after the backend refused the old ones. It
// reports whether anything changed.
func (e *engine) rotate(p *provider, headers map[string]string, by string, now time.Time) bool {
	var changed []string
	for name, v := range headers {
		old, had := p.rotated[name]
		switch {
		case v == "" && had:
			delete(p.rotated, name)
		case v != "" && (!had || old != v):
			if p.rotated == nil {
				p.rotated = map[string]string{}
			}
			p.rotated[name] = v
		default:
			continue
		}
		changed = append(changed, name)
	}
	if len(changed) == 0 {
		return false
	}
	slices.Sort(changed)
	p.headersGen++
	p.authFailed, p.authRetryAt = false, time.Time{}
	e.emit(now, "provider_headers", map[string]any{"provider": p.cfg.tag, "by": by, "names": changed})
	return true
}

// takeAnswerHeaders puts the headers of an applied answer over the rotated
// ones, unless the host changed them while the request ran: the host wins.
// It returns the names the answer carried and whether they lost.
func (e *engine) takeAnswerHeaders(p *provider, headers map[string]string, gen uint64, now time.Time) ([]string, bool) {
	if len(headers) == 0 {
		return nil, false
	}
	names := slices.Sorted(maps.Keys(headers))
	if gen != p.headersGen {
		return names, true
	}
	e.rotate(p, headers, "answer", now)
	return names, false
}

// headerFields tells in provider_apply which headers an answer carried and
// whether the host had replaced the headers meanwhile.
func headerFields(f map[string]any, names []string, stale bool) {
	if len(names) == 0 {
		return
	}
	f["headers"] = names
	if stale {
		f["headers_stale"] = true
	}
}

func (e *engine) setProviderHeaders(tag string, headers map[string]string, now time.Time) error {
	p := e.provider(tag)
	if p == nil {
		return fmt.Errorf("no provider %s", tag)
	}
	n := len(p.rotated)
	for name, v := range headers {
		_, had := p.rotated[name]
		switch {
		case v == "" && had:
			n--
		case v != "" && !had:
			n++
		}
	}
	if n > maxRotatedHeaders {
		return fmt.Errorf("more than %d headers", maxRotatedHeaders)
	}
	e.rotate(p, headers, "host", now)
	// The host vouches for the headers, changed or not.
	p.authFailed, p.authRetryAt = false, time.Time{}
	return nil
}

// SetProviderHeaders puts headers over those of the config for the
// requests of the provider, as an answer with headers does: the host got a
// new token, for example after the backend refused the old one. An empty
// value removes what the host or an answer set, and the config applies
// again. The call ends a pause after refused headers, but neither the pause
// a busy backend asked for nor the backoff, and it starts no refresh:
// RequestRefresh does. An answer to a request sent with the headers before
// the call does not override them. Errors name a header, never its value.
func (o *Observer) SetProviderHeaders(provider string, headers map[string]string) error {
	if len(headers) > maxRotatedHeaders {
		return fmt.Errorf("more than %d headers", maxRotatedHeaders)
	}
	set := make(map[string]string, len(headers))
	for name, v := range headers {
		if err := CheckHeaderName(name); err != nil {
			return err
		}
		canon := http.CanonicalHeaderKey(name)
		if _, twice := set[canon]; twice {
			return fmt.Errorf("header %s given twice", canon)
		}
		if v != "" {
			if err := checkHeaderValue(canon, v); err != nil {
				return err
			}
		}
		set[canon] = v
	}
	o.mu.Lock()
	err := o.e.setProviderHeaders(provider, set, o.now())
	o.mu.Unlock()
	if err != nil {
		return err
	}
	o.wake()
	return nil
}

// authRefused tells whether the backend refused the headers of a request:
// a 401 or 403 with an error body of the protocol that says so. A bare 401
// or 403 may come from a portal or a firewall on the way, and is a failure
// like any other.
func (r refreshOutcome) authRefused() bool {
	return (r.status == http.StatusUnauthorized || r.status == http.StatusForbidden) &&
		(r.code == "unauthorized" || r.code == "forbidden")
}

// authHeld tells whether the pause after refused headers lasts: until
// authRetryAt, or until the headers change without one.
func (p *provider) authHeld(now time.Time) bool {
	return p.authFailed && (p.authRetryAt.IsZero() || now.Before(p.authRetryAt))
}

// holdAuth starts the pause after refused headers. Asking again with the
// same headers only loads the backend: every trigger but the host waits,
// and the failure grows no backoff, since the pause is longer anyway.
func (e *engine) holdAuth(p *provider, now time.Time) {
	p.authFailed = true
	p.authRetryAt = time.Time{}
	if p.cfg.authRetry > 0 {
		p.authRetryAt = now.Add(e.jitter(p.cfg.authRetry, p.cfg.backoff.jitter))
	}
}
