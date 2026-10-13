package health

import (
	"context"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/core"
)

func TestQuietShareRejectsInvalidDirectConfig(t *testing.T) {
	for _, share := range []float64{-0.1, 1.1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		ctx := context.WithValue(context.Background(), core.XrayKey(1), new(core.Instance))
		_, err := New(ctx, &Config{Features: &FeatureConfig{Report: v2Bool(true)}, Report: &ReportConfig{QuietShare: share}})
		if err == nil || !strings.Contains(err.Error(), "report.quietShare") {
			t.Fatalf("quietShare %v: error %v", share, err)
		}
	}
}

func TestQuietShareDefaultsPreserveReportsAndRandomSequence(t *testing.T) {
	const seed = 42
	for _, cfg := range []*Config{{}, {Report: &ReportConfig{QuietShare: 0}}} {
		e := newEngine(newSettings(cfg), seed)
		e.report.begin(t0)
		if e.report.quietSample || e.snapshotReport(at(time.Second)) != nil || e.takeReport(true, at(time.Second)) != nil {
			t.Fatal("default quietShare emitted a report without decisions")
		}
		want := rand.New(rand.NewSource(seed)).Int63()
		if got := e.rnd.Int63(); got != want {
			t.Fatalf("default quietShare changed scheduling randomness: got %d, want %d", got, want)
		}
	}
}

func TestQuietShareSampleIsChosenOncePerRun(t *testing.T) {
	seen := map[bool]bool{}
	for seed := int64(1); seed <= 32; seed++ {
		cfg := &Config{Features: &FeatureConfig{Report: v2Bool(true)}, Report: &ReportConfig{QuietShare: 0.5, Interval: int64(time.Second)}}
		e := newEngine(newSettings(cfg), seed)
		e.report.begin(t0)
		control := rand.New(rand.NewSource(seed))
		want := control.Float64() < 0.5
		seen[want] = true
		if e.report.quietSample != want {
			t.Fatalf("seed %d selection %v, want %v", seed, e.report.quietSample, want)
		}
		for i := 0; i < 3; i++ {
			snapshot := e.snapshotReport(at(time.Duration(i+1) * time.Second))
			if (snapshot != nil) != want || snapshot != nil && !snapshot.IsQuietSample {
				t.Fatalf("seed %d snapshot %+v", seed, snapshot)
			}
			if e.takeReport(false, at(time.Duration(i+1)*time.Second)) != nil {
				t.Fatal("quiet selection made an interval report due without decisions")
			}
		}
		e.report.decide()
		interval := e.takeReport(false, at(4*time.Second))
		if interval == nil || interval.IsQuietSample != want {
			t.Fatalf("seed %d interval %+v", seed, interval)
		}
		final := e.takeReport(true, at(5*time.Second))
		if final == nil || final.IsQuietSample != want || e.takeReport(true, at(6*time.Second)) != nil {
			t.Fatalf("seed %d final %+v", seed, final)
		}
		if got, next := e.rnd.Int63(), control.Int63(); got != next {
			t.Fatalf("reports resampled seed %d: got %d, want %d", seed, got, next)
		}
	}
	if len(seen) != 2 {
		t.Fatal("test seeds failed to exercise both sampled and unsampled runs")
	}
}

func TestQuietShareOneReportsQuietRunAtStop(t *testing.T) {
	o := newObserver(context.Background(), &Config{Features: &FeatureConfig{Report: v2Bool(true)}, Report: &ReportConfig{QuietShare: 1}})
	defer o.Close()
	if o.PeekReport() == nil || o.TakeReport(false) != nil {
		t.Fatal("quiet snapshot or interval eligibility differs from the contract")
	}
	v := udpReport(t, o.TakeReport(true))
	if !v.IsQuietSample || v.Reason != reportStop || o.TakeReport(true) != nil || o.PeekReport() != nil {
		t.Fatalf("quiet stop report %+v", v)
	}
}
