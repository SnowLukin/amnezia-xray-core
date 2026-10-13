package health

import (
	"testing"
	"time"
)

func TestProbeOnlyScheduleSettings(t *testing.T) {
	for _, tc := range []struct {
		name     string
		config   *Config
		verified int
		interval time.Duration
	}{
		{"defaults", &Config{}, 1, 15 * time.Minute},
		{"empty", &Config{Schedule: &ScheduleConfig{ProbeOnly: &ProbeOnlyConfig{}}}, 1, 15 * time.Minute},
		{"configured", &Config{Schedule: &ScheduleConfig{ProbeOnly: &ProbeOnlyConfig{KeepVerified: 2, RecheckInterval: int64(3 * time.Minute)}}}, 2, 3 * time.Minute},
		{"protobuf bounds", &Config{Schedule: &ScheduleConfig{ProbeOnly: &ProbeOnlyConfig{KeepVerified: 100, RecheckInterval: -1}}}, 4, 15 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := newSettings(tc.config).schedule.probeOnly
			if got.keepVerified != tc.verified || got.recheckInterval != tc.interval {
				t.Fatalf("probe-only settings %+v", got)
			}
		})
	}
}

func TestProbeOnlyModeIsPerGroupMember(t *testing.T) {
	s := newSelectorSettings(&SelectorConfig{
		Features: &SelectorFeatureConfig{Groups: v2Bool(true), ReturnToCheaper: v2Bool(true)},
		Paths: []*PathDefinition{
			{Tag: "real", Group: "relay"},
			{Tag: "candidate", Group: "relay", IsProbeOnly: true},
		},
		Groups: []*PathGroup{{Name: "relay", Cost: 3, IsLazy: true, Probe: "internal", ReturnInterval: int64(time.Minute)}},
	})
	if s.isProbeOnly("real") || !s.isProbeOnly("candidate") || s.isProbeOnly("missing") {
		t.Fatal("probe-only mode did not stay on its group member")
	}
	for _, tag := range []string{"real", "candidate"} {
		p := s.paths[tag]
		if p.groupName != "relay" || p.cost != 3 || !p.lazy || p.probe != "internal" || s.returnInterval(tag) != time.Minute {
			t.Fatalf("group member %s: %+v", tag, p)
		}
	}
}
