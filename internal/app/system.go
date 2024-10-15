package app

import (
	"context"
	"errors"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-chi/chi/v5"
	"go.uber.org/atomic"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/yt/admin/cms/internal/cms"
	"go.ytsaurus.tech/yt/admin/cms/internal/cms/hotswap"
	"go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/go/ytlock"
	"go.ytsaurus.tech/yt/go/ytsys"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

const (
	// cmsTasksNode is a name of cypress node containing tasks table.
	cmsTasksNode = "tasks"
	// hotswapTasksNode is a name of cypress node containing hot swap tasks table.
	hotswapTasksNode = "hotswap_tasks"

	// distributedLockNode is a name of a cypress node containing distributed lock.
	distributedLockNode = "lock"

	sysTransactions = ypath.Path("//sys/transactions")
	leaderURLAttr   = "leader_url"

	BundleSys = "sys"
)

type SystemConfig struct {
	StartrekConfig *cms.StartrekConfig `yaml:"-"`

	selfURL string `yaml:"-"`

	// CypressRoot is a parent of all cypress nodes used in service.
	CypressRoot ypath.Path `yaml:"-"`

	// Proxy identifies cluster.
	Proxy               string              `yaml:"proxy"`
	TaskDiscoveryConfig TaskDiscoveryConfig `yaml:"task_discovery_config"`

	// Bundle for service tables, sys by default.
	Bundle string `yaml:"bundle"`

	EnableHotSwapProcessing bool `yaml:"enable_hot_swap_processing"`

	ClusterPollPeriod      time.Duration                  `yaml:"cluster_poll_period"`
	TaskProcessorConfig    cms.TaskProcessorConfig        `yaml:",inline"`
	HotSwapProcessorConfig hotswap.HotSwapProcessorConfig `yaml:"hot_swap"`
	ColocationConfigs      cms.ColocationConfigs          `yaml:"-"`
	ClusterDiscoveryConfig discovery.ClusterConfig        `yaml:"cluster_discovery_config"`

	AuthConfig AuthConfig `yaml:"-"`
}

func (c *SystemConfig) UnmarshalYAML(unmarshal func(any) error) error {
	type plain SystemConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.ClusterPollPeriod <= 0 {
		return xerrors.Errorf("cluster poll period must be positive, got %s", c.ClusterPollPeriod)
	}

	if c.Bundle == "" {
		c.Bundle = BundleSys
	}

	c.HotSwapProcessorConfig.Init(c.TaskProcessorConfig)

	return nil
}

// System is management system for single cluster.
type System struct {
	conf *SystemConfig
	l    log.Structured

	yc        yt.Client
	api       *API
	systemAPI *SystemAPI

	cluster        models.Cluster
	storage        cms.Storage
	taskProcessor  *cms.TaskProcessor
	taskDiscoverer *TaskDiscoverer

	hotSwapProcessor *hotswap.HotSwapProcessor

	// leader stores FQDN of CMS app instance that acquired exclusive lock.
	// When the current instance is leading this field is set to an empty string.
	leader atomic.String

	// Metrics.
	restarts            metrics.Counter
	migrateTableErrors  metrics.Counter
	clusterUpdateErrors metrics.Counter
	clusterPollLoop     metrics.Timer
	leading             metrics.Gauge
}

// NewSystem creates new system object corresponding to specific cluster.
func NewSystem(
	conf *SystemConfig,
	yc yt.Client,
	d *TaskDiscoverer,
	l log.Structured,
) *System {
	c := models.NewCluster(&conf.ClusterDiscoveryConfig, l)
	dc := ytsys.NewClient(yc, l)

	s := &System{
		conf:           conf,
		l:              l,
		yc:             yc,
		taskDiscoverer: d,
		cluster:        c,
	}

	s.storage = cms.NewStorage(yc, s.tasksPath())

	conf.TaskProcessorConfig.Proxy = conf.Proxy
	conf.TaskProcessorConfig.StartrekConfig = conf.StartrekConfig
	conf.TaskProcessorConfig.ColocationConfigs = conf.ColocationConfigs
	s.taskProcessor = cms.NewTaskProcessor(&conf.TaskProcessorConfig, l, yc, dc, c, s.storage)

	s.hotSwapProcessor = hotswap.NewHotSwapProcessor(
		&conf.HotSwapProcessorConfig, log.With(l.Logger(), log.String("service", "hotswap")).Structured(), dc,
		hotswap.NewStorage(yc, s.hotswapTasksPath()),
	)

	s.api = NewAPI(s, s.conf.AuthConfig.cliAuth, yc, s.storage, c, l)

	systemAPIConf := &SystemAPIConfig{Proxy: conf.Proxy}
	s.systemAPI = NewSystemAPI(systemAPIConf, l, s.storage)

	return s
}

func (s *System) RegisterMetrics(r metrics.Registry) {
	s.restarts = r.Counter("restarts")
	s.migrateTableErrors = r.Counter("migrate_tables_errors")
	s.clusterUpdateErrors = r.Counter("cluster_update_errors")
	s.clusterPollLoop = r.Timer("cluster_poll_loop")
	s.leading = r.Gauge("leading")

	s.taskProcessor.RegisterMetrics(r)
	s.systemAPI.RegisterMetrics(r)
	s.api.RegisterMetrics(r)

	s.hotSwapProcessor.RegisterMetrics(r.WithPrefix("hot_swap"))
}

func (s *System) resetLeaderMetrics() {
	s.taskProcessor.ResetLeaderMetrics()
	s.hotSwapProcessor.ResetLeaderMetrics()
}

func (s *System) RegisterAPI(r chi.Router) {
	cmsAPIRouter := s.systemAPI.Routes()
	r.With(s.conf.AuthConfig.auth).Mount("/"+s.conf.Proxy, cmsAPIRouter)

	r.Mount("/api/"+s.conf.Proxy, s.api.Routes())
}

// Run starts background CMS task processing.
func (s *System) Run(ctx context.Context) error {
	s.l.Info("migrating tables")
	if err := s.ensureTables(ctx); err != nil {
		return err
	}
	s.l.Info("migrated tables")

	s.api.SetReady()
	s.systemAPI.SetReady()

	lockPath := s.conf.CypressRoot.Child(distributedLockNode)
	lock := ytlock.NewLockOptions(s.yc, lockPath, ytlock.Options{
		CreateIfMissing: true,
		LockMode:        yt.LockExclusive,
		TxAttributes:    map[string]any{leaderURLAttr: s.conf.selfURL},
	})

	return ytlock.RunLocked(ctx, lock, s.run, func(err error, retryAfter time.Duration) {
		s.leading.Set(0)
		s.resetLeaderMetrics()

		if yterrors.ContainsErrorCode(err, yterrors.CodeConcurrentTransactionLockConflict) {
			s.l.Debug("failed to acquire lock", log.Error(err))
		} else if errors.Is(err, ytlock.ErrLockLost) {
			s.l.Debug("lock lost")
		}
		s.l.Debug("task processing job stopped",
			log.Duration("retry_after", retryAfter), log.Error(err))

		if tx := ytlock.FindConflictWinner(err); tx != nil {
			s.updateLeader(ctx, tx)
		}
	})
}

// OtherLeaderIsActive checks whether this instance is not the one that acquired exclusive lock.
//
// The first return arg is an url of a leading instance.
// It is set iff the second return argument is true.
func (s *System) OtherLeaderIsActive() (*url.URL, bool) {
	if h := s.leader.Load(); h != "" {
		u := &url.URL{Scheme: "http", Host: h}
		return u, true
	}
	return nil, false
}

func (s *System) tasksPath() ypath.Path {
	return s.conf.CypressRoot.Child(cmsTasksNode)
}

func (s *System) hotswapTasksPath() ypath.Path {
	return s.conf.CypressRoot.Child(hotswapTasksNode)
}

// run polls cluster state and processes CMS tasks including YP maintenance requests.
func (s *System) run(ctx context.Context) error {
	s.l.Info("acquired lock")
	s.leader.Store("")

	s.restarts.Inc()
	s.leading.Set(1)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		s.l.Info("starting cluster polling")
		err := s.runClusterPolling(gctx)
		s.l.Info("stopped cluster polling", log.Error(err))
		return err
	})

	g.Go(func() error {
		s.l.Info("starting task discovery")
		err := s.taskDiscoverer.runTaskDiscovery(ctx, s.conf, s.storage, s.l)
		s.l.Info("stopped task discovery")
		return err
	})

	g.Go(func() error {
		return s.taskProcessor.Run(ctx)
	})

	if s.conf.EnableHotSwapProcessing {
		g.Go(func() error {
			return s.hotSwapProcessor.Run(ctx)
		})
	}

	return g.Wait()
}

func (s *System) updateLeader(ctx context.Context, tx *ytlock.WinnerTx) {
	req := sysTransactions.Child(tx.ID.String()).Attr(leaderURLAttr)
	var l string
	err := s.yc.GetNode(ctx, req, &l, nil)
	if err != nil {
		s.l.Warn("failed to get winner URL", log.Error(err))
	} else {
		s.l.Info("other leader is active", log.String("leader_url", l))
		s.leader.Store(l)
	}
}

// ensureTables creates and mounts all dynamic tables used by the system.
//
// The operation is retried infinitely until success.
//
// Can be stopped via ctx.
func (s *System) ensureTables(ctx context.Context) error {
	attrs := map[string]any{
		"tablet_cell_bundle": s.conf.Bundle,
	}

	tables := map[ypath.Path]migrate.Table{
		s.tasksPath(): {Schema: schema.MustInfer(&models.Task{}), Attributes: attrs},
	}

	if s.conf.EnableHotSwapProcessing {
		tables[s.hotswapTasksPath()] = migrate.Table{Schema: schema.MustInfer(&models.HotSwapTask{})}
	}

	s.l.Info("path", log.String("path", s.tasksPath().String()))
	return backoff.RetryNotify(
		func() error {
			return migrate.EnsureTables(ctx, s.yc, tables, migrate.OnConflictFail)
		},
		backoff.WithContext(backoff.NewConstantBackOff(time.Second), ctx),
		func(err error, retryAfter time.Duration) {
			s.l.Error("failed to migrate dynamic tables", log.Error(err))
			s.migrateTableErrors.Inc()
		})
}

// runClusterPolling updates cluster state in loop.
func (s *System) runClusterPolling(ctx context.Context) error {
	dc := ytsys.NewClient(s.yc, s.l)
	return pollWithContext(ctx, s.conf.ClusterPollPeriod, func(ctx context.Context) {
		start := time.Now()
		defer s.clusterPollLoop.RecordDuration(time.Since(start))

		if err := s.cluster.Reload(ctx, dc); err != nil {
			s.l.Error("failed to update cluster state", log.Error(err))
			s.clusterUpdateErrors.Inc()
			return
		}
		s.l.Info("reloaded cluster state")
	})
}
