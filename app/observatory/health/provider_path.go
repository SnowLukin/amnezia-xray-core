package health

import (
	"encoding/json"
	"fmt"
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

func answerInterval(obj map[string]json.RawMessage, where string) (int64, error) {
	if len(obj["returnInterval"]) == 0 {
		return 0, nil
	}
	value, err := answerString(obj, "returnInterval", where)
	if err != nil {
		return 0, err
	}
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s.returnInterval must be a nonnegative duration", where)
	}
	if d == 0 {
		return -1, nil
	}
	return int64(d), nil
}

func answerLazy(obj map[string]json.RawMessage, where string) (bool, error) {
	if len(obj["isLazy"]) == 0 {
		return false, nil
	}
	var lazy bool
	if err := json.Unmarshal(obj["isLazy"], &lazy); err != nil {
		return false, fmt.Errorf("%s.isLazy must be a boolean", where)
	}
	return lazy, nil
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
			obj, err := answerObject(raw, where, map[string]bool{"name": true, "cost": true, "isLazy": true, "returnInterval": true, "probe": true, "failedInRowToLeave": true})
			if err != nil {
				return err
			}
			name, err := answerString(obj, "name", where)
			if err != nil || name == "" {
				return fmt.Errorf("%s needs nonempty name", where)
			}
			if groups[name] != nil {
				return fmt.Errorf("duplicate group %s", name)
			}
			cost, err := answerCost(obj, where)
			if err != nil {
				return err
			}
			lazy, err := answerLazy(obj, where)
			if err != nil {
				return err
			}
			interval, err := answerInterval(obj, where)
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
			g := &PathGroup{Name: cfg.prefix + name, Cost: cost, IsLazy: lazy, ReturnInterval: interval, Probe: probe, FailedInRowToLeave: limit}
			groups[name] = g
			a.groups = append(a.groups, g)
		}
	}
	a.paths = nil
	for _, head := range a.heads {
		where := fmt.Sprintf("outbound %s path", head.Tag)
		obj, err := answerObject(head.Path, where, map[string]bool{"balancer": true, "group": true, "cost": true, "isLazy": true, "returnInterval": true, "probe": true, "isProbeOnly": true})
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
			return fmt.Errorf("%s.group needs nonempty name", where)
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
			for _, key := range []string{"cost", "isLazy", "returnInterval", "probe"} {
				if _, exists := obj[key]; exists {
					return fmt.Errorf("%s.%s is set by group %s", where, key, group)
				}
			}
			p.Group = g.Name
		} else {
			p.Cost, err = answerCost(obj, where)
			if err != nil {
				return err
			}
			p.IsLazy, err = answerLazy(obj, where)
			if err != nil {
				return err
			}
			p.ReturnInterval, err = answerInterval(obj, where)
			if err != nil {
				return err
			}
			p.Probe, err = answerString(obj, "probe", where)
			if err != nil || (len(obj["probe"]) > 0 && p.Probe == "") {
				return fmt.Errorf("%s.probe needs a name", where)
			}
		}
		a.paths = append(a.paths, p)
	}
	return nil
}

func validateAnswerProbes(s *settings, a *providerAnswer) error {
	known := map[string]bool{}
	for _, p := range s.probes {
		known[p.name] = true
	}
	for _, p := range a.paths {
		if p.Probe != "" && !known[p.Probe] {
			return fmt.Errorf("outbound %s: unknown probe %s", p.Tag, p.Probe)
		}
	}
	for _, g := range a.groups {
		if g.Probe != "" && !known[g.Probe] {
			return fmt.Errorf("group %s: unknown probe %s", g.Name, g.Probe)
		}
	}
	return nil
}
