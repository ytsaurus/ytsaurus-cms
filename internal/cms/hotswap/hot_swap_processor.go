package hotswap

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/yt/admin/cms/internal/cms"
)

type HotSwapProcessorConfig struct {
}

func (c *HotSwapProcessorConfig) Init(cms.TaskProcessorConfig) {
}

type HotSwapProcessor struct {
}

func NewHotSwapProcessor(*HotSwapProcessorConfig, log.Structured, cms.DiscoveryClient, Storage) *HotSwapProcessor {
	return &HotSwapProcessor{}
}

func (p *HotSwapProcessor) Run(ctx context.Context) error {
	return nil
}

func (p *HotSwapProcessor) RegisterMetrics(r metrics.Registry) {
}

func (p *HotSwapProcessor) ResetLeaderMetrics() {
}
