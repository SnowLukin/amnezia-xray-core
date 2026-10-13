package health

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"time"
)

func answerObject(raw json.RawMessage, where string, allowed map[string]bool) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if len(raw) == 0 || string(raw) == "null" || json.Unmarshal(raw, &obj) != nil || obj == nil {
		return nil, fmt.Errorf("%s must be an object", where)
	}
	for key := range obj {
		if !allowed[key] {
			return nil, fmt.Errorf("%s: unknown key %s", where, key)
		}
		if string(obj[key]) == "null" {
			return nil, fmt.Errorf("%s.%s must not be null", where, key)
		}
	}
	return obj, nil
}

func answerString(obj map[string]json.RawMessage, key, where string) (string, error) {
	if len(obj[key]) == 0 {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(obj[key], &value); err != nil {
		return "", fmt.Errorf("%s.%s must be a string", where, key)
	}
	return value, nil
}

func answerCost(obj map[string]json.RawMessage, where string) (float32, error) {
	if len(obj["cost"]) == 0 {
		return 0, fmt.Errorf("%s needs cost above zero", where)
	}
	var cost float32
	if err := json.Unmarshal(obj["cost"], &cost); err != nil || math.IsNaN(float64(cost)) || math.IsInf(float64(cost), 0) || cost <= 0 {
		return 0, fmt.Errorf("%s needs cost above zero", where)
	}
	return cost, nil
}

// A returnToCheaper block enables the override; omission inherits the balancer.
func answerReturn(obj map[string]json.RawMessage, where string) (int64, *bool, error) {
	raw := obj["returnToCheaper"]
	if len(raw) == 0 {
		return 0, nil, nil
	}
	ret, err := answerObject(raw, where+".returnToCheaper", map[string]bool{"isEnabled": true, "interval": true})
	if err != nil {
		return 0, nil, err
	}
	enabled := true
	if v := ret["isEnabled"]; len(v) > 0 {
		if json.Unmarshal(v, &enabled) != nil || enabled {
			return 0, nil, fmt.Errorf("%s.returnToCheaper.isEnabled only accepts false to disable inherited returnToCheaper", where)
		}
	}
	if len(ret["interval"]) == 0 {
		return 0, &enabled, nil
	}
	value, err := answerString(ret, "interval", where+".returnToCheaper")
	if err != nil {
		return 0, nil, err
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, nil, fmt.Errorf("%s.returnToCheaper.interval must be a positive duration", where)
	}
	return int64(d), &enabled, nil
}

func answerProbePolicy(obj map[string]json.RawMessage, where string) (bool, error) {
	if len(obj["probePolicy"]) == 0 {
		return false, nil
	}
	policy, err := answerString(obj, "probePolicy", where)
	if err != nil {
		return false, err
	}
	switch policy {
	case "regular":
		return false, nil
	case "onDemand":
		return true, nil
	}
	return false, fmt.Errorf("%s.probePolicy must be regular or onDemand", where)
}

func answerLabels(obj map[string]json.RawMessage, where string) (map[string]string, error) {
	if len(obj["labels"]) == 0 {
		return nil, nil
	}
	var labels map[string]string
	if json.Unmarshal(obj["labels"], &labels) != nil || labels == nil || len(labels) > 6 {
		return nil, fmt.Errorf("%s.labels must have at most 6 string labels", where)
	}
	for k, v := range labels {
		if len(k) < 1 || len(k) > 24 || len(v) < 1 || len(v) > 32 {
			return nil, fmt.Errorf("%s.labels has invalid bounded label", where)
		}
		for _, c := range k {
			if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
				return nil, fmt.Errorf("%s.labels has invalid key", where)
			}
		}
		for _, c := range v {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == ':' || c == '-') {
				return nil, fmt.Errorf("%s.labels has invalid value", where)
			}
		}
	}
	return labels, nil
}

func validateAnswerPaths(cfg *providerSettings, a *providerAnswer) error {
	groups := map[string]*PathGroup{}
	a.groups = nil
	if len(a.groupsRaw) > 0 {
		var raws []json.RawMessage
		if string(a.groupsRaw) == "null" || json.Unmarshal(a.groupsRaw, &raws) != nil || raws == nil {
			return fmt.Errorf("groups must be an array")
		}
		for i, raw := range raws {
			where := fmt.Sprintf("groups[%d]", i)
			obj, err := answerObject(raw, where, map[string]bool{"name": true, "cost": true, "probePolicy": true, "returnToCheaper": true, "probe": true, "failedInRowToLeave": true, "labels": true})
			if err != nil {
				return err
			}
			name, err := answerString(obj, "name", where)
			if err != nil || name == "" || len(cfg.prefix+name) > 128 {
				return fmt.Errorf("%s needs name of at most 128 bytes including its prefix", where)
			}
			if groups[name] != nil {
				return fmt.Errorf("duplicate group %s", name)
			}
			cost, err := answerCost(obj, where)
			if err != nil {
				return err
			}
			lazy, err := answerProbePolicy(obj, where)
			if err != nil {
				return err
			}
			interval, returnEnabled, err := answerReturn(obj, where)
			if err != nil {
				return err
			}
			probe, err := answerString(obj, "probe", where)
			if err != nil || (len(obj["probe"]) > 0 && probe == "") {
				return fmt.Errorf("%s.probe needs a name", where)
			}
			limit := int32(3)
			if len(obj["failedInRowToLeave"]) > 0 {
				if json.Unmarshal(obj["failedInRowToLeave"], &limit) != nil || limit < 1 || limit > 16 {
					return fmt.Errorf("%s.failedInRowToLeave must be from 1 to 16", where)
				}
			}
			labels, err := answerLabels(obj, where)
			if err != nil {
				return err
			}
			g := &PathGroup{Labels: labels, ReturnIsEnabled: returnEnabled, Name: cfg.prefix + name, Cost: cost, IsLazy: lazy, ReturnInterval: interval, Probe: probe, FailedInRowToLeave: limit}
			groups[name] = g
			a.groups = append(a.groups, g)
		}
	}
	if len(groups) > cfg.maxOutbounds {
		return fmt.Errorf("groups exceed maxOutbounds")
	}
	usedGroups := map[string]bool{}
	a.paths = nil
	for _, head := range a.heads {
		where := fmt.Sprintf("outbound %s selection", head.Tag)
		obj, err := answerObject(head.Selection, where, map[string]bool{"balancer": true, "group": true, "cost": true, "probePolicy": true, "returnToCheaper": true, "probe": true, "isProbeOnly": true, "labels": true})
		if err != nil {
			return err
		}
		balancer, err := answerString(obj, "balancer", where)
		if err != nil || balancer != cfg.balancer {
			return fmt.Errorf("%s.balancer must be %s", where, cfg.balancer)
		}
		group, err := answerString(obj, "group", where)
		if err != nil {
			return err
		}
		if _, present := obj["group"]; present && group == "" {
			return fmt.Errorf("%s.group needs name of at most 128 bytes including its prefix", where)
		}
		p := &PathDefinition{Tag: cfg.prefix + head.Tag, Balancer: balancer}
		if raw := obj["isProbeOnly"]; len(raw) > 0 {
			if json.Unmarshal(raw, &p.IsProbeOnly) != nil {
				return fmt.Errorf("%s.isProbeOnly must be a boolean", where)
			}
		}
		if group != "" {
			g := groups[group]
			if g == nil {
				return fmt.Errorf("%s: unknown group %s", where, group)
			}
			for _, key := range []string{"cost", "probePolicy", "returnToCheaper", "probe"} {
				if _, exists := obj[key]; exists {
					return fmt.Errorf("%s.%s is set by group %s", where, key, group)
				}
			}
			p.Group = g.Name
			usedGroups[group] = true
		} else {
			p.Cost, err = answerCost(obj, where)
			if err != nil {
				return err
			}
			p.IsLazy, err = answerProbePolicy(obj, where)
			if err != nil {
				return err
			}
			p.ReturnInterval, p.ReturnIsEnabled, err = answerReturn(obj, where)
			if err != nil {
				return err
			}
			p.Probe, err = answerString(obj, "probe", where)
			if err != nil || (len(obj["probe"]) > 0 && p.Probe == "") {
				return fmt.Errorf("%s.probe needs a name", where)
			}
		}
		p.Labels, err = answerLabels(obj, where)
		if err != nil {
			return err
		}
		if group != "" {
			merged := maps.Clone(groups[group].Labels)
			if merged == nil {
				merged = map[string]string{}
			}
			maps.Copy(merged, p.Labels)
			if len(merged) > 6 {
				return fmt.Errorf("%s.labels merged with its group exceeds 6", where)
			}
			p.Labels = merged
		}
		a.paths = append(a.paths, p)
	}
	for name := range groups {
		if !usedGroups[name] {
			return fmt.Errorf("unused group %s", name)
		}
	}
	return nil
}

func validateAnswerProbes(s *settings, a *providerAnswer) error {
	known := map[string]bool{}
	for _, p := range s.probes {
		known[p.name] = true
	}
	for _, p := range a.paths {
		if p.IsProbeOnly && !s.features.Candidates {
			return fmt.Errorf("selection isProbeOnly requires healthObservatory.probes.candidates")
		}
		if p.Group == "" && p.Probe == "" {
			return fmt.Errorf("outbound %s selection requires an explicit probe profile", p.Tag)
		}
		if p.Probe != "" && !known[p.Probe] {
			return fmt.Errorf("outbound %s: unknown probe %s", p.Tag, p.Probe)
		}
	}
	for _, g := range a.groups {
		if g.Probe == "" {
			return fmt.Errorf("group %s requires an explicit probe profile", g.Name)
		}
		if g.Probe != "" && !known[g.Probe] {
			return fmt.Errorf("group %s: unknown probe %s", g.Name, g.Probe)
		}
	}
	return nil
}
