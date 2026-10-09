package health

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"time"
)

// lastAnswer is the last answer a provider applied, as the host may keep it
// to start the next run with: the answer as it came, without whitespace,
// the time its leases count from and when it arrived by the wall clock. A
// new answer puts a new one in place; the bytes are never changed, so
// ProviderSet builds its document from them outside the observer lock.
type lastAnswer struct {
	raw        []byte
	ref        time.Time
	receivedAt time.Time
}

// compactAnswer copies an answer without the whitespace between its tokens,
// outside the observer lock: the copy is what a provider keeps.
func compactAnswer(body []byte) []byte {
	var b bytes.Buffer
	b.Grow(len(body))
	writeCompact(&b, body)
	return b.Bytes()
}

// keepAnswer keeps an applied answer, raw as compactAnswer gives it, for
// ProviderSet. changed tells that what the host would keep differs from the
// previous one: the set, the end of a lease, the headers or the base config
// the answer names. Only then does the sequence number grow, so a host that
// asks with the number it has gets nothing for an answer that only brought
// a fresher clock.
func (e *engine) keepAnswer(p *provider, a *providerAnswer, raw []byte, changed bool, now time.Time) {
	last := lastAnswer{raw: raw}
	if !a.receivedAt.IsZero() {
		// A kept set applied at the start stays what it was.
		last.ref, last.receivedAt = a.serverTime, a.receivedAt
		if last.ref.IsZero() {
			last.ref = a.receivedAt
		}
	} else {
		at := a.received
		if at.IsZero() {
			at = now
		}
		last.receivedAt = at.Round(0)
		last.ref, _ = leaseRef(a.serverTime, a.date, last.receivedAt)
	}
	p.last = last
	if changed {
		p.seq++
	}
}

// ProviderSet returns the last answer the provider applied as JSON valid as
// its initial: the answer with the headers an answer or the host set in
// place of its own (those of the config stay out), with serverTime the
// time its leases count from and with receivedAt. A host keeps it, safely,
// and passes it as initial when it starts the core without a fresh answer,
// so the core starts with the set, the leases as they ran on and the token
// last rotated. It returns nil when nothing was applied or nothing changed
// since the sequence number after; the number grows when the set, the end
// of a lease, the headers or the base the answer names change. Asked with
// the number it returned, it allocates nothing, so a host may ask often.
func (o *Observer) ProviderSet(provider string, after uint64) ([]byte, uint64, error) {
	o.mu.Lock()
	p := o.e.provider(provider)
	if p == nil {
		o.mu.Unlock()
		return nil, 0, fmt.Errorf("no provider %s", provider)
	}
	seq := p.seq
	if p.last.raw == nil || seq <= after {
		o.mu.Unlock()
		return nil, seq, nil
	}
	last, rotated := p.last, maps.Clone(p.rotated)
	o.mu.Unlock()
	doc, err := last.document(rotated)
	if err != nil {
		return nil, seq, err
	}
	return doc, seq, nil
}

func (l lastAnswer) document(rotated map[string]string) ([]byte, error) {
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(l.raw, &sections); err != nil {
		return nil, err
	}
	delete(sections, "headers")
	if len(rotated) > 0 {
		sections["headers"], _ = json.Marshal(rotated)
	}
	sections["serverTime"], _ = json.Marshal(l.ref.UTC().Format(time.RFC3339Nano))
	sections["receivedAt"], _ = json.Marshal(l.receivedAt.UTC().Format(time.RFC3339Nano))
	return json.Marshal(sections)
}
