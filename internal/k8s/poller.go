package k8s

import (
	"context"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/library/go/core/xerrors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var ErrNotReady = xerrors.Errorf("snapshot is missing")

type PollerConfig struct {
	PollInterval time.Duration `yaml:"poll_interval"`
}

// Poller polls and caches all k8s nodes.
type Poller struct {
	conf *PollerConfig
	l    log.Structured

	cs *kubernetes.Clientset

	// mu guards snapshot and lastTimestamp.
	mu sync.RWMutex
	// snapshot stores last full state of all k8s nodes.
	snapshot      *corev1.NodeList
	lastTimestamp time.Time

	// Metrics.
	updateSnapshotErrors metrics.Counter
}

func NewPoller(conf *PollerConfig, l log.Structured, cs *kubernetes.Clientset) *Poller {
	return &Poller{
		conf: conf,
		l:    l,
		cs:   cs,
	}
}

func (p *Poller) RegisterMetrics(r metrics.Registry) {
	p.updateSnapshotErrors = r.Counter("update_snapshot_errors")
}

func (p *Poller) Run(ctx context.Context) error {
	t := time.NewTicker(p.conf.PollInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if err := p.updateSnapshot(ctx); err != nil {
				p.l.Error("k8s poll error", log.Error(err), log.Duration("next_poll_after", p.conf.PollInterval))
				break
			}
			p.l.Debug("k8s poll succeeded", log.Duration("next_poll_after", p.conf.PollInterval))

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// GetNodes returns cached cluster nodes.
func (p *Poller) GetNodes() (*corev1.NodeList, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.snapshot == nil {
		return nil, ErrNotReady
	}

	return p.snapshot, nil
}

func (p *Poller) updateSnapshot(ctx context.Context) error {
	snapshot, err := p.cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		p.updateSnapshotErrors.Inc()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.lastTimestamp = time.Time{}
	if snapshot != nil {
		p.lastTimestamp = time.Now()
	}

	p.snapshot = snapshot
	return err
}
