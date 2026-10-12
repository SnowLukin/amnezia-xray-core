package health

import (
	"fmt"
	stdnet "net"
	"net/url"
	"strconv"
	"strings"
)

func (c *Config) ValidateProbeSteps() error {
	if err := c.ValidateUDP(); err != nil {
		return err
	}
	seenProfiles := make(map[string]bool, len(c.GetProbes()))
	for profileIndex, profile := range c.GetProbes() {
		where := fmt.Sprintf("healthObservatory: probes[%d]", profileIndex)
		if profile == nil {
			return fmt.Errorf("%s.name must be nonempty and unique (null profile)", where)
		}
		if profile.Name == "" || seenProfiles[profile.Name] {
			return fmt.Errorf("%s.name must be nonempty and unique", where)
		}
		seenProfiles[profile.Name] = true
		if profile.Steps == nil {
			continue
		}
		if len(profile.Steps) < 1 || len(profile.Steps) > 4 {
			return fmt.Errorf("%s.steps must contain 1 to 4 steps", where)
		}
		if profile.Destination != "" {
			return fmt.Errorf("%s.destination cannot be used with steps", where)
		}
		for _, field := range []struct {
			name    string
			present bool
		}{
			{"method", profile.Method != ""},
			{"expectedStatuses", len(profile.ExpectedStatuses) > 0},
			{"minBytes", profile.MinBytes != 0},
			{"timeout", profile.Timeout != 0},
			{"progressTimeout", profile.ProgressTimeout != 0},
		} {
			if field.present {
				return fmt.Errorf("%s.%s belongs to steps", where, field.name)
			}
		}
		seenNames := make(map[string]bool, len(profile.Steps))
		optionalStarted := false
		for stepIndex, step := range profile.Steps {
			stepWhere := fmt.Sprintf("%s.steps[%d]", where, stepIndex)
			if step == nil {
				return fmt.Errorf("%s is null", stepWhere)
			}
			if step.Name == "" || seenNames[step.Name] {
				return fmt.Errorf("%s.name must be nonempty and unique", stepWhere)
			}
			seenNames[step.Name] = true
			if (stepIndex == 0 && step.IsOptional) || (!step.IsOptional && optionalStarted) {
				return fmt.Errorf("%s: mandatory steps must precede optional steps", stepWhere)
			}
			optionalStarted = step.IsOptional
			if len(step.Destinations) < 1 || len(step.Destinations) > 4 {
				return fmt.Errorf("%s.destinations must contain 1 to 4 URLs", stepWhere)
			}
			for _, destination := range step.Destinations {
				u, err := url.Parse(destination)
				if destination == "" || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
					return fmt.Errorf("%s.destinations must be nonempty http or https URLs", stepWhere)
				}
			}
			switch strings.ToUpper(step.Method) {
			case "", "GET":
			case "HEAD":
				if step.MinBytes > 0 {
					return fmt.Errorf("%s.minBytes cannot be used with HEAD", stepWhere)
				}
			default:
				return fmt.Errorf("%s.method must be GET or HEAD", stepWhere)
			}
			if step.Timeout < 0 {
				return fmt.Errorf("%s.timeout must be positive", stepWhere)
			}
			if step.ProgressTimeout < 0 {
				return fmt.Errorf("%s.progressTimeout must be positive", stepWhere)
			}
		}
	}
	return nil
}

func (c *Config) ValidateUDP() error {
	if c.GetPassive().GetUdpReplyTimeout() < -1 {
		return fmt.Errorf("healthObservatory: passive.udpReplyTimeout must be nonnegative")
	}
	u := c.GetUdpProbe()
	if u == nil {
		return nil
	}
	if len(u.Destinations) < 1 || len(u.Destinations) > 4 {
		return fmt.Errorf("healthObservatory: udpProbe.destinations must contain 1 to 4 host:port addresses")
	}
	for _, address := range u.Destinations {
		host, port, err := stdnet.SplitHostPort(address)
		n, portErr := strconv.ParseUint(port, 10, 16)
		if err != nil || host == "" || strings.ContainsAny(host, " /?#@\\\t\r\n") || portErr != nil || n == 0 {
			return fmt.Errorf("healthObservatory: udpProbe.destinations must contain valid host:port addresses")
		}
	}
	for _, field := range []struct {
		name  string
		value int64
	}{{"requests", int64(u.Requests)}, {"interval", u.Interval}, {"timeout", u.Timeout}, {"perHour", int64(u.PerHour)}} {
		if field.value < 0 {
			return fmt.Errorf("healthObservatory: udpProbe.%s must be positive", field.name)
		}
	}
	return nil
}
