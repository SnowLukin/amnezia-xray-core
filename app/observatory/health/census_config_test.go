package health

import (
	"math"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestCensusConfigDefaultsAndGate(t *testing.T) {
	s := newSettings(&Config{})
	if s.census != nil || s.features.Census || s.features.status()["census"] {
		t.Fatal("absent census enabled")
	}
	s = newSettings(&Config{Census: &CensusConfig{SampleFraction: 0.03, OnDemand: []string{"reserve", "relay"}}})
	if s.census == nil || !s.features.Census || !s.features.status()["census"] || s.census.startAfter != 3*time.Minute || s.census.sampleFraction != 0.03 || !s.census.onDemand["reserve"] || !s.census.onDemand["relay"] {
		t.Fatalf("presence or default: %+v", s.census)
	}
	s = newSettings(&Config{Census: &CensusConfig{SampleFraction: 1, StartAfter: int64(time.Second)}})
	if s.census.startAfter != time.Second {
		t.Fatal("positive short duration not preserved")
	}
}

func TestCensusConfigDirectProtoValidation(t *testing.T) {
	for name, cc := range map[string]*CensusConfig{
		"zero":              {SampleFraction: 0},
		"negative":          {SampleFraction: -0.1},
		"over one":          {SampleFraction: 1.1},
		"nan":               {SampleFraction: math.NaN()},
		"infinity":          {SampleFraction: math.Inf(1)},
		"negative duration": {SampleFraction: 1, StartAfter: -1},
	} {
		t.Run(name, func(t *testing.T) {
			yes := true
			if err := (&Config{Census: cc, Features: &FeatureConfig{Report: &yes}}).ValidateFeatures(); err == nil || !strings.Contains(err.Error(), "telemetry.census") {
				t.Fatalf("accepted direct protobuf: %v", err)
			}
		})
	}
	if err := (&Config{Census: &CensusConfig{SampleFraction: 1}}).ValidateFeatures(); err == nil || !strings.Contains(err.Error(), "requires telemetry.report") {
		t.Fatalf("missing report accepted: %v", err)
	}
	yes := true
	valid := &Config{Census: &CensusConfig{SampleFraction: 1, StartAfter: int64(time.Second), OnDemand: []string{"a"}}, Features: &FeatureConfig{Report: &yes}}
	if err := valid.ValidateFeatures(); err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip Config
	if err := proto.Unmarshal(wire, &roundTrip); err != nil || !proto.Equal(valid, &roundTrip) {
		t.Fatalf("census wire round trip: %v", err)
	}
}
