package cms

import (
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ytsys"
)

const (
	missingPartChunksThrottlerBlockSize = time.Minute
	missingPartChunksMaxFailurePeriod   = time.Minute * 45
)

type MissingPartChunksThrottlerConfig struct {
	BlockSize time.Duration `yaml:"block_size"`
	// maxFailurePeriod is a maximum time since the last successful period of time (of BlockSize duration)
	// without check errors.
	MaxFailurePeriod time.Duration `yaml:"max_failure_period"`
}

// NewMissingPartChunksThrottlerConfig creates throttler with default settings.
func NewMissingPartChunksThrottlerConfig() *MissingPartChunksThrottlerConfig {
	c := &MissingPartChunksThrottlerConfig{}
	c.init()
	return c
}

func (c *MissingPartChunksThrottlerConfig) init() {
	if c.BlockSize <= 0 {
		c.BlockSize = missingPartChunksThrottlerBlockSize
	}

	if c.MaxFailurePeriod <= 0 {
		c.MaxFailurePeriod = missingPartChunksMaxFailurePeriod
	}
}

func (c *MissingPartChunksThrottlerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain MissingPartChunksThrottlerConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	c.init()
	return nil
}

// MissingPartChunksThrottler checks whether task processing must be stopped due to
// long period of PMC > 0 || DMC > 0.
//
// Namely it returns no error iff the last blockSize of consecutive 'missing part chunks' checks was
// at most T seconds ago.
//
// Throttler is parametrized with a blockSize size and T.
type MissingPartChunksThrottler struct {
	conf *MissingPartChunksThrottlerConfig
	// lastFailure stores local time of the last failed missing part chunks check.
	lastFailure time.Time
	// lastSuccess stores local time of the last failed missing part chunks check.
	lastSuccess time.Time
	// lastSuccessfulBlock stores local time of the last blockSize without failures.
	lastSuccessfulBlock time.Time
}

// NewOdinChecker creates new throttler.
func NewMissingPartChunksThrottler(conf *MissingPartChunksThrottlerConfig) *MissingPartChunksThrottler {
	conf.init()
	return &MissingPartChunksThrottler{
		conf:                conf,
		lastSuccessfulBlock: time.Now().Add(-conf.MaxFailurePeriod / 2),
	}
}

// OnChunkIntegrityUpdate updates throttler state.
func (t *MissingPartChunksThrottler) OnChunkIntegrityUpdate(i *ytsys.ChunkIntegrity) {
	missingPartChunks := i == nil || i.PMC > 0 || i.DMC > 0
	if missingPartChunks {
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
func (t *MissingPartChunksThrottler) Allow() error {
	if passed := time.Since(t.lastSuccessfulBlock); passed > t.conf.MaxFailurePeriod {
		return xerrors.Errorf("last %s period without errors was %s ago", t.conf.BlockSize, passed)
	}
	return nil
}
