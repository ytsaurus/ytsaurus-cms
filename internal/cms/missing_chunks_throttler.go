package cms

import (
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ytsys"
)

const (
	missingChunksThrottlerBlockSize = time.Minute
	missingChunksMaxFailurePeriod   = time.Minute * 45
)

type MissingChunksThrottlerConfig struct {
	BlockSize time.Duration `yaml:"block_size"`
	// maxFailurePeriod is a maximum time since the last successful period of time (of BlockSize duration)
	// without check errors.
	MaxFailurePeriod time.Duration `yaml:"max_failure_period"`
}

// NewMissingChunksThrottlerConfig creates throttler with default settings.
func NewMissingChunksThrottlerConfig() *MissingChunksThrottlerConfig {
	c := &MissingChunksThrottlerConfig{}
	c.init()
	return c
}

func (c *MissingChunksThrottlerConfig) init() {
	if c.BlockSize <= 0 {
		c.BlockSize = missingChunksThrottlerBlockSize
	}

	if c.MaxFailurePeriod <= 0 {
		c.MaxFailurePeriod = missingChunksMaxFailurePeriod
	}
}

func (c *MissingChunksThrottlerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain MissingChunksThrottlerConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	c.init()
	return nil
}

// MissingChunksThrottler checks whether task processing must be stopped due to
// long period of (PMC > 0 || DMC > 0).
//
// Namely it returns no error iff the last blockSize of consecutive 'missing part chunks' checks was
// at most T seconds ago.
//
// Throttler is parametrized with a blockSize size and T.
type MissingChunksThrottler struct {
	conf *MissingChunksThrottlerConfig
	// lastFailure stores local time of the last failed missing part chunks check.
	lastFailure time.Time
	// lastSuccess stores local time of the last failed missing part chunks check.
	lastSuccess time.Time
	// lastSuccessfulBlock stores local time of the last blockSize without failures.
	lastSuccessfulBlock time.Time
}

// NewMissingChunksThrottler creates new throttler.
func NewMissingChunksThrottler(conf *MissingChunksThrottlerConfig) *MissingChunksThrottler {
	conf.init()
	return &MissingChunksThrottler{
		conf:                conf,
		lastSuccessfulBlock: time.Now().Add(-conf.MaxFailurePeriod / 2),
	}
}

// OnChunkIntegrityUpdate updates throttler state.
func (t *MissingChunksThrottler) OnChunkIntegrityUpdate(i *ytsys.ChunkIntegrity) {
	missingChunks := i == nil || i.PMC > 0 || i.DMC > 0
	if missingChunks {
		t.lastFailure = time.Now()
		return
	} else {
		if t.lastSuccess.After(t.lastFailure) && time.Since(t.lastFailure) > t.conf.BlockSize {
			t.lastSuccessfulBlock = time.Now()
		}
		t.lastSuccess = time.Now()
	}
}

// Allow checks whether last group of 'missing part chunks' checks without failure was recently.
func (t *MissingChunksThrottler) Allow() error {
	if passed := time.Since(t.lastSuccessfulBlock); passed > t.conf.MaxFailurePeriod {
		return xerrors.Errorf("last %s period without errors was %s ago", t.conf.BlockSize, passed)
	}
	return nil
}
