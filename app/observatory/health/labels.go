package health

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/netip"
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

// Labels go to analytics, so a value that is an address is refused.
func validLabelValue(s string) bool {
	if len(s) < 1 || len(s) > 32 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == ':' || c == '-') {
			return false
		}
	}
	_, err := netip.ParseAddr(s)
	_, errPort := netip.ParseAddrPort(s)
	return err != nil && errPort != nil
}

// ValidateLabels checks labels of the config, of provider answers and of
// protobuf configs alike.
func ValidateLabels(labels map[string]string, limit int) error {
	if len(labels) > limit {
		return fmt.Errorf("at most %d labels are allowed", limit)
	}
	for _, key := range slices.Sorted(maps.Keys(labels)) {
		if !validLabelName(key) {
			return fmt.Errorf("%s: invalid label name", key)
		}
		if !validLabelValue(labels[key]) {
			return fmt.Errorf("%s: invalid label value", key)
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
		e.quality.trimSegments()
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

// pathLabels are always in Status; pathLabels of telemetry only decides
// whether they go to the report and to quality.
func (s *settings) pathLabels(tag string) map[string]string {
	if p, ok := s.paths.Load(tag); ok {
		return maps.Clone(p.(pathSettings).labels)
	}
	return nil
}

// NetworkChangedWithLabels has the same routing semantics as NetworkChanged.
// labels is a JSON object of strings. Malformed JSON drops the labels, not
// the change, and is reported as an error; malformed entries are discarded,
// with at most eight valid entries retained by key.
func (o *Observer) NetworkChangedWithLabels(id, labels string) error {
	if !o.s.features.HostLabels {
		o.networkChanged(id, nil)
		return nil
	}
	var values map[string]string
	var err error
	if len(labels) > 8192 {
		err = fmt.Errorf("network labels JSON is too large")
	} else if labels != "" && (json.Unmarshal([]byte(labels), &values) != nil || values == nil) {
		values, err = nil, fmt.Errorf("network labels must be a JSON object of strings")
	}
	o.networkChanged(id, values)
	return err
}

func (o *Observer) networkChanged(id string, labels map[string]string) {

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.e.onNetworkChangeWithLabels(id, labels, o.now())
		o.generation.Store(o.e.generation)
		o.qualityEpoch.Store(o.e.qualityEpoch)

	}()
	o.wake()
}
