package app

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/cms"
	"go.ytsaurus.tech/yt/admin/cms/internal/k8s"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type TaskDiscoveryConfig struct {
}

type TaskDiscoverer struct {
	cs     *kubernetes.Clientset
	poller *k8s.Poller
}

func NewTaskDiscoverer(registry *MetricsRegistry, clusters []*SystemConfig, token string, l log.Structured) (*TaskDiscoverer, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, xerrors.Errorf("failed to create in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, xerrors.Errorf("failed to create k8s client: %w", err)
	}

	p := k8s.NewPoller(&k8s.PollerConfig{PollInterval: defaultPollPeriod}, l, cs)
	p.RegisterMetrics(registry.registry)

	return &TaskDiscoverer{cs: cs, poller: p}, nil
}

func (d *TaskDiscoverer) initTaskDiscovery(ctx context.Context) error {
	return d.poller.Run(ctx)
}

func (d *TaskDiscoverer) runTaskDiscovery(ctx context.Context, conf *SystemConfig, storage cms.Storage, l log.Structured) error {
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		l.Debug("starting k8s task discovery")
		err := k8s.NewTaskDiscovery(&k8s.TaskDiscoveryConfig{}, l, storage, d.poller).Run(gctx)
		l.Debug("stopped k8s task discovery")
		return err
	})

	g.Go(func() error {
		l.Debug("starting node annotator")
		err := k8s.NewNodeAnnotator(&k8s.NodeAnnotatorConfig{}, l, storage, d.cs, d.poller).Run(gctx)
		l.Debug("stopped node annotator")
		return err
	})

	return g.Wait()
}
