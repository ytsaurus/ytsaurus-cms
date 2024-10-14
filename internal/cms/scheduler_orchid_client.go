package cms

import (
	"context"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type ResourceUsage struct {
	GPU float64 `json:"gpu" yson:"gpu"`
}

type SchedulerOrchidNode struct {
	Usage *ResourceUsage `json:"resource_usage" yson:"resource_usage"`
}

//go:generate mockgen -destination=scheduler_orchid_client_mock.go -package cms . SchedulerOrchidClient

type SchedulerOrchidClient interface {
	GetNode(ctx context.Context, path ypath.YPath, result any, options *yt.GetNodeOptions) error
}

type schedulerOrchidClient struct {
	yc yt.Client
}

func (c *schedulerOrchidClient) GetNode(
	ctx context.Context,
	path ypath.YPath,
	result any,
	options *yt.GetNodeOptions,
) error {
	return c.yc.GetNode(ctx, path, result, nil)
}
