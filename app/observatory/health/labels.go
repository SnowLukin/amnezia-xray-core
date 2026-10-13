package health

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
)

// Label names and values are opaque ASCII tokens, never addresses or text.
func validLabelName(s string) bool {
	if len(s) < 1 || len(s) > 24 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}

func validLabelValue(s string) bool {
	if len(s) < 1 || len(s) > 32 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == ':' || c == '-') {
			return false
		}
	}
	return true
}

// ValidateLabels is shared by the JSON loader and protobuf/provider validation.
func ValidateLabels(labels map[string]string, limit int) error {
	if len(labels) > limit {
		return fmt.Errorf("at most %d labels are allowed", limit)
	}
	for key, value := range labels {
		if !validLabelName(key) || !validLabelValue(value) {
			return fmt.Errorf("invalid label %q", key)
		}
	}
	return nil
}

func filterLabels(labels map[string]string, limit int) map[string]string {
	var out map[string]string
	for _, key := range slices.Sorted(maps.Keys(labels)) {
		if !validLabelName(key) || !validLabelValue(labels[key]) {
			continue
		}
		if len(out) == limit {
			break
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[key] = labels[key]
	}
	return out
}

func mergeLabels(group, member map[string]string) map[string]string {
	out := maps.Clone(group)
	if out == nil && len(member) > 0 {
		out = make(map[string]string)
	}
	maps.Copy(out, member)
	return filterLabels(out, 6)
}

type networkLabelsView struct {
	Host map[string]string `json:"host,omitempty"`
	IP   map[string]string `json:"ip,omitempty"`
}

func (e *engine) networkLabels() *networkLabelsView {
	var ip map[string]string
	if e.labelNetwork != nil {
		ip = e.labelNetwork.labels
	}
	if len(e.hostLabels) == 0 && len(ip) == 0 {
		return nil
	}
	return &networkLabelsView{Host: maps.Clone(e.hostLabels), IP: maps.Clone(ip)}
}

func cloneNetworkLabels(v *networkLabelsView) *networkLabelsView {
	if v == nil {
		return nil
	}
	return &networkLabelsView{Host: maps.Clone(v.Host), IP: maps.Clone(v.IP)}
}

func (e *engine) updateQualityLabels() {
	if e.quality == nil {
		return
	}
	v := e.networkLabels()
	e.quality.current.network.labels = v
	if e.quality.reporting {
		e.quality.open.network.labels = v
	}
}

func (e *engine) labelQualityPath(tag string, epoch uint64) {
	if e.quality == nil || epoch != e.quality.epoch {
		return
	}
	if !e.s.features.PathLabels {
		return
	}
	if _, exists := e.s.paths.Load(tag); !exists {
		return
	}
	labels := e.s.pathLabels(tag)
	for _, collection := range []*qualityCollection{&e.quality.current, &e.quality.open} {
		if path := collection.paths[tag]; path != nil {
			path.labels = maps.Clone(labels)
		}
	}
}

func (s *settings) pathLabels(tag string) map[string]string {
	if !s.features.PathLabels {
		return nil
	}
	if p, ok := s.paths.Load(tag); ok {
		return maps.Clone(p.(pathSettings).labels)
	}
	return nil
}

// NetworkChangedWithLabels has the same routing semantics as NetworkChanged.
// labels is a JSON object of strings. Malformed JSON is an error; malformed
// entries are discarded, with at most eight valid entries retained by key.
func (o *Observer) NetworkChangedWithLabels(id, labels string) error {
	if !o.s.features.HostLabels {
		o.networkChanged(id, nil)
		return nil
	}
	var values map[string]string
	if labels != "" {
		if len(labels) > 8192 {
			return fmt.Errorf("network labels JSON is too large")
		}
		if err := json.Unmarshal([]byte(labels), &values); err != nil || values == nil {
			return fmt.Errorf("network labels must be a JSON object of strings")
		}
	}
	o.networkChanged(id, values)
	return nil
}

func (o *Observer) networkChanged(id string, labels map[string]string) {
	o.mu.Lock()
	o.e.onNetworkChangeWithLabels(id, labels, o.now())
	o.generation.Store(o.e.generation)
	o.qualityEpoch.Store(o.e.qualityEpoch)
	o.mu.Unlock()
	o.wake()
}
