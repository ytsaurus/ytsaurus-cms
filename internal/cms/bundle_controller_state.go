package cms

import (
	"time"

	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/ytsys"
)

type BundleControllerState string

const (
	BCStateReady    BundleControllerState = "ready"
	BCStateWaiting  BundleControllerState = "waiting"
	BCStateTimedOut BundleControllerState = "timed_out"
)

func (p *TaskProcessor) isBundleControllerManaged(ann *ytsys.BundleControllerAnnotations) bool {
	return p.conf.BundleControllerReadinessTimeout != 0 &&
		ann != nil && ann.DeallocationStrategy == ytsys.DeallocationStrategyHulkRequest
}

func (p *TaskProcessor) bundleControllerState(
	ann *ytsys.BundleControllerAnnotations,
	maintenanceStartTime yson.Time,
) BundleControllerState {
	if ann.Allocated != nil && !bool(*ann.Allocated) && ann.DeallocatedAt != nil {
		return BCStateReady
	}
	startedAt := time.Time(maintenanceStartTime)
	// TODO: drop the IsZero check once maintenance start time is always set. See YTADMIN-13032
	if startedAt.IsZero() || time.Since(startedAt) >= p.conf.BundleControllerReadinessTimeout {
		return BCStateTimedOut
	}
	return BCStateWaiting
}
