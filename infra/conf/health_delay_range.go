package conf

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
)

// HealthDelayRange is a fixed delay or inclusive duration range. It is local to
// provider triggers; the upstream duration parser is deliberately unchanged.
type HealthDelayRange string

func (d HealthDelayRange) bounds() (time.Duration, time.Duration, error) {
	parts := strings.Split(string(d), "..")
	if len(parts) < 1 || len(parts) > 2 {
		return 0, 0, fmt.Errorf("expected duration or min..max")
	}
	low, err := time.ParseDuration(parts[0])
	if err != nil {
		return 0, 0, err
	}
	high := low
	if len(parts) == 2 {
		high, err = time.ParseDuration(parts[1])
		if err != nil {
			return 0, 0, err
		}
	}
	if low < 0 || high < low {
		return 0, 0, fmt.Errorf("delay bounds must be nonnegative and ordered")
	}
	return low, high, nil
}
func (d *HealthDelayRange) UnmarshalJSON(raw []byte) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("delay must be a duration string: %w", err)
	}
	parsed := HealthDelayRange(value)
	if _, _, err := parsed.bounds(); err != nil {
		return err
	}
	*d = parsed
	return nil
}
func (d HealthDelayRange) valid() bool { _, _, err := d.bounds(); return err == nil }
func rangeMin(d *HealthDelayRange) int64 {
	if d == nil {
		return 0
	}
	low, _, _ := d.bounds()
	return int64(low)
}
func rangeMax(d *HealthDelayRange) *int64 {
	if d == nil {
		return nil
	}
	_, high, _ := d.bounds()
	v := int64(high)
	return &v
}
func rangeMaxMin(d *HealthDelayRange) *int64 {
	if d == nil {
		return nil
	}
	v := rangeMin(d)
	return &v
}
func rangeProto(d *HealthDelayRange) *health.DurationRange {
	if d == nil {
		return nil
	}
	return &health.DurationRange{Min: rangeMin(d), Max: *rangeMax(d)}
}
