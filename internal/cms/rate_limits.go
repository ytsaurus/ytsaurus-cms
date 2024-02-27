package cms

import (
	"time"

	"golang.org/x/time/rate"
)

const (
	defaultMaxParallelHosts = 150
	defaultMaxHostsPerHour  = 60.0
)

type RateLimitConfig struct {
	MaxParallelHosts int     `yaml:"max_parallel_hosts"`
	MaxHostsPerHour  float64 `yaml:"max_hosts_per_hour"`
}

type Limiter struct {
	*rate.Limiter
	Config *RateLimitConfig
}

func (c *RateLimitConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain RateLimitConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.MaxParallelHosts == 0 {
		c.MaxParallelHosts = defaultMaxParallelHosts
	}

	if c.MaxHostsPerHour == 0 {
		c.MaxHostsPerHour = defaultMaxHostsPerHour
	}

	return nil
}

func NewRateLimiter(c *RateLimitConfig, activeHostCount int) *Limiter {
	r := rate.Every(time.Duration(float64(time.Hour) / c.MaxHostsPerHour))
	limiter := &Limiter{rate.NewLimiter(r, c.MaxParallelHosts), c}

	if activeHostCount < c.MaxParallelHosts {
		limiter.AllowN(time.Now(), activeHostCount)
	} else {
		limiter.AllowN(time.Now(), c.MaxParallelHosts)
	}

	return limiter
}
