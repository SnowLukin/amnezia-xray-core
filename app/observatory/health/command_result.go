package health

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

// CommandError is a bounded machine readable refusal. Messages never contain
// headers, destinations, credentials, or arbitrary errors from a provider.
type CommandError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *CommandError) Error() string { return e.Message }
func NewCommandError(code, message string) *CommandError {
	return &CommandError{Code: code, Message: message}
}

// Result is the shared JSON contract. Transports preserve it without encoding
// data a second time; the library alone wraps these bytes in base64.
type Result[T any] struct {
	Success bool          `json:"success"`
	Run     string        `json:"run,omitempty"`
	Data    T             `json:"data,omitempty"`
	Error   *CommandError `json:"error,omitempty"`
}

func CommandErrorOf(err error) *CommandError {
	if err == nil {
		return nil
	}
	var ce *CommandError
	if errors.As(err, &ce) {
		return ce
	}
	// Legacy internal errors remain local. Only a bounded classification crosses
	// the native JSON interface, never err.Error() from an arbitrary source.
	code := "invalid_argument"
	text := err.Error()
	switch {
	case strings.HasPrefix(text, "no provider "):
		code = "unknown_provider"
	case strings.HasPrefix(text, "no such balancer:"):
		code = "unknown_balancer"
	case strings.HasPrefix(text, "not a candidate:"):
		code = "unknown_outbound"
	case strings.Contains(text, "disabled"):
		code = "feature_disabled"
	case strings.Contains(text, "no candidates yet"):
		code = "busy"
	}
	return NewCommandError(code, strings.ReplaceAll(code, "_", " "))
}

func NewResult[T any](o *Observer, data T, err error) Result[T] {
	if o == nil {
		var zero T
		return Result[T]{Data: zero, Error: NewCommandError("not_running", "health observatory is not running")}
	}
	if err != nil {
		return Result[T]{Run: o.run, Error: CommandErrorOf(err)}
	}
	return Result[T]{Success: true, Run: o.run, Data: data}
}

func MarshalResult[T any](o *Observer, data T, err error) []byte {
	out, _ := json.Marshal(NewResult(o, data, err))
	return out
}

// FeatureEnabled is a read only check for signal ABI bridges and JSON commands.
func (o *Observer) FeatureEnabled(name string) bool {
	if o == nil {
		return false
	}
	f := o.s.features
	switch name {
	case "events":
		return f.Events
	case "report":
		return f.Report
	case "probe_requests":
		return f.ProbeRequests
	case "udp_probe":
		return f.UdpProbes
	case "pinning":
		return f.Pinning
	case "user_present":
		return f.UserPresent
	case "memory_pressure":
		return f.MemoryPressure
	case "host_labels":
		return f.HostLabels
	}
	return false
}
func (o *Observer) FeatureError(name string) error {
	if o == nil {
		return NewCommandError("not_running", "health observatory is not running")
	}
	if !o.FeatureEnabled(name) {
		return NewCommandError("feature_disabled", name+" is disabled")
	}
	return nil
}

type EventBatch struct {
	Events  json.RawMessage `json:"events"`
	Dropped uint64          `json:"dropped"`
}
type ProviderSetData struct {
	Seq uint64          `json:"seq"`
	Set json.RawMessage `json:"set"`
}

// ProviderRefreshInfo queues the same work as RequestProviderRefresh and tells
// the host why it will wait. Repeated host calls do not bypass any runtime gate.
type RefreshData struct {
	IsQueued  bool   `json:"is_queued"`
	Reason    string `json:"reason,omitempty"`
	RetryInMs int64  `json:"retry_in_ms"`
}

func (o *Observer) ProviderRefreshInfo(tag string) (RefreshData, error) {
	data, err := o.queueProviderRefresh(tag)
	if err != nil {
		return RefreshData{}, err
	}
	o.wake()
	return data, nil
}

func (o *Observer) queueProviderRefresh(tag string) (RefreshData, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.e.providers) == 0 || o.s.disabledProviders[tag] {
		return RefreshData{}, NewCommandError("feature_disabled", "providers are disabled")
	}
	if tag != "" && o.e.provider(tag) == nil {
		return RefreshData{}, NewCommandError("unknown_provider", "provider is unknown")
	}
	o.e.requestRefresh(tag)
	now := o.now()
	data := RefreshData{IsQueued: true}
	for _, p := range o.e.providers {
		if tag != "" && p.cfg.tag != tag {
			continue
		}
		if p.fetching {
			data.Reason = "busy"
			continue
		}
		expiry := p.expiryWant(now)
		if why := o.e.heldBack(p, triggerHost, expiry, o.e.idle(now), now); why != "" {
			data.Reason = why
			if why == "auth" && !p.authRetryAt.IsZero() {
				data.RetryInMs = max(data.RetryInMs, p.authRetryAt.Sub(now).Milliseconds())
			}
		}
		if at, why := p.allowedAt(true, now); now.Before(at) {
			data.Reason = why
			data.RetryInMs = max(data.RetryInMs, at.Sub(now).Milliseconds())
		}
	}
	return data, nil
}

// DecodeProbeRequest reads the whole typed request, including isUdp, on every bridge.
func DecodeProbeRequest(raw []byte) (ProbeRequest, error) {
	var request ProbeRequest
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return request, NewCommandError("invalid_argument", "probe request must be a JSON object")
	}
	for key, value := range fields {
		if (key != "balancer" && key != "outbounds" && key != "isUdp") || string(value) == "null" {
			return request, NewCommandError("invalid_argument", "probe request has an unknown or null field")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if !utf8.Valid(raw) || decoder.Decode(&request) != nil || request.Balancer == "" {
		return request, NewCommandError("invalid_argument", "probe request must contain balancer, outbounds and isUdp")
	}
	var trailingJSON any
	if decoder.Decode(&trailingJSON) != io.EOF {
		return ProbeRequest{}, NewCommandError("invalid_argument", "probe request must be one JSON object")
	}
	return request, nil
}
func (o *Observer) Run() string {
	if o == nil {
		return ""
	}
	return o.run
}

func (o *Observer) providerCommandError(tag string) error { return o.e.providerCommandError(tag) }
func (e *engine) providerCommandError(tag string) error {
	if len(e.providers) == 0 || e.s.disabledProviders[tag] {
		return NewCommandError("feature_disabled", "provider is disabled")
	}
	if e.provider(tag) == nil {
		return NewCommandError("unknown_provider", "provider is unknown")
	}
	return nil
}

// Omit data on errors even when T is a value struct (encoding/json's generic
// omitempty cannot omit a zero struct). This keeps failed Result decodable by
// every typed bridge and never exposes a partial ticket.
func (r Result[T]) MarshalJSON() ([]byte, error) {
	var data json.RawMessage
	if r.Success {
		raw, err := json.Marshal(r.Data)
		if err != nil {
			return nil, err
		}
		if string(raw) != "null" {
			data = raw
		}
	}
	return json.Marshal(struct {
		Success bool            `json:"success"`
		Run     string          `json:"run,omitempty"`
		Data    json.RawMessage `json:"data,omitempty"`
		Error   *CommandError   `json:"error,omitempty"`
	}{r.Success, r.Run, data, r.Error})
}
