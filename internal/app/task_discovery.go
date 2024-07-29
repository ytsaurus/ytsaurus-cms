package app

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/cms"
)

type TaskDiscoveryConfig struct {
}

type TaskDiscoverer struct {
}

func NewTaskDiscoverer(registry *MetricsRegistry, clusters []*SystemConfig, token string, l log.Structured) (*TaskDiscoverer, error) {
	return &TaskDiscoverer{}, nil
}

func (d *TaskDiscoverer) initTaskDiscovery(ctx context.Context) error {
	return nil
}

func (d *TaskDiscoverer) runTaskDiscovery(ctx context.Context, conf *SystemConfig, storage cms.Storage, l log.Structured) error {
	return nil
}
