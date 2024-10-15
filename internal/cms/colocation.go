package cms

import (
	"context"
	"errors"
	"math"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"golang.org/x/time/rate"
)

type WalleClient interface {
	// FindProject searches for the Wall-e project that the host belongs to.
	FindProject(ctx context.Context, host string) (walle.Project, error)
	// GetHostInfo returns host information from Wall-e for specified instances.
	GetHostInfo(ctx context.Context, hosts ...string) (map[string]*walle.HostInfo, error)
}

// DefaultMaxHostsPerHour is the value used for ColocationConfig's MaxHostsPerHour setting
// when the latter one is not set.
const DefaultMaxHostsPerHour = math.MaxFloat64

type ColocationConfig struct {
	WalleProject walle.Project `yaml:"walle_project"`
	// MaxHostsPerHour limits the number of hosts (that don't have any yt component)
	// CMS can give to Wall-e.
	MaxHostsPerHour float64 `yaml:"max_hosts_per_hour"`
}

func (c *ColocationConfig) UnmarshalYAML(unmarshal func(any) error) error {
	type plain ColocationConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.MaxHostsPerHour == 0.0 {
		c.MaxHostsPerHour = DefaultMaxHostsPerHour
	}

	return nil
}

type ColocationConfigs map[walle.Project]*ColocationConfig

type ColocationRateLimits struct {
	l log.Structured

	Configs ColocationConfigs

	wc WalleClient

	Limiters map[walle.Project]*rate.Limiter

	// Metrics.
	failedWalleProjectResolves metrics.Counter
}

func NewColocationRateLimits(l log.Structured, configs ColocationConfigs) *ColocationRateLimits {
	limits := &ColocationRateLimits{
		l:        l,
		Configs:  configs,
		wc:       walle.NewClient(),
		Limiters: make(map[walle.Project]*rate.Limiter),
	}

	for _, c := range configs {
		r := rate.Every(time.Duration(float64(time.Hour) / c.MaxHostsPerHour))
		limits.Limiters[c.WalleProject] = rate.NewLimiter(r, 1)
	}

	return limits
}

func (l *ColocationRateLimits) RegisterMetrics(r metrics.Registry) {
	l.failedWalleProjectResolves = r.Counter("failed_walle_project_resolves")
}

func (l *ColocationRateLimits) Allow(ctx context.Context, h *models.Host) bool {
	p, err := l.wc.FindProject(ctx, h.Host)
	if err != nil && !errors.Is(err, walle.ErrHostNotFound) {
		l.l.Error("unable to resolve walle project", log.String("host", h.Host), log.Error(err))
		l.failedWalleProjectResolves.Inc()
		return false
	}

	if limiter, ok := l.Limiters[p]; ok {
		return limiter.Allow()
	}

	return true
}
