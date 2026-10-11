package health

import (
	"fmt"
	"math"
)

func validateQuietShare(config *ReportConfig) error {
	share := config.GetQuietShare()
	if math.IsNaN(share) || share < 0 || share > 1 {
		return fmt.Errorf("healthObservatory: report.quietShare must be in [0, 1]")
	}
	return nil
}
