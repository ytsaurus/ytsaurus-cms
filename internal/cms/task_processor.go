package cms

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/startrek"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/ytsys"
	"golang.org/x/sync/errgroup"
)

const (
	defaultMaxParallelGroupTasks            = 1
	defaultMinGroupTaskSize                 = 2
	defaultOfflineNodeRetentionPeriod       = time.Minute * 20
	defaultBannedNodeHoldOffPeriod          = time.Minute * 3
	defaultDisabledSchedulerJobsWaitTimeout = time.Minute * 20
	defaultDisabledWriteSessionsWaitTimeout = time.Minute * 20
	defaultOfflineHTTPProxyRetentionPeriod  = time.Minute * 10
	defaultMaxHTTPProxiesPerRole            = 0.1
	defaultMaxRPCProxiesPerRole             = 0.1
	defaultCPUReserve                       = 27
	defaultBundleSlotReserve                = 1
	defaultTabletCommonNodeReserve          = 1
	defaultUnconfirmedChunkCheckTimeout     = time.Hour
	defaultUnconfirmedChunkThreshold        = 100
	defaultHostAnnotationPeriod             = time.Second * 15
)

type StartrekConfig struct {
	OAuthToken string `yaml:"-"`
	URL        string `yaml:"api_url"`
	Queue      string `yaml:"queue"`
}

func (c *StartrekConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain StartrekConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.URL == "" {
		return xerrors.New("url can not be empty")
	}

	if c.Queue == "" {
		return xerrors.New("queue can not be empty")
	}

	return nil
}

type TaskProcessorConfig struct {
	Proxy string `yaml:"-"`

	StartrekConfig *StartrekConfig `yaml:"-"`

	TaskProcessingPeriod      time.Duration `yaml:"task_processing_period"`
	ChunkIntegrityCheckPeriod time.Duration `yaml:"chunk_integrity_check_period"`

	EnableGroupTasks      bool `yaml:"enable_group_tasks"`
	MaxParallelGroupTasks int  `yaml:"max_parallel_group_tasks"`
	// MinGroupTaskSize stores minimum number of tasks with the same maintenance_info/node_set_id
	// that considered a group.
	// Typically 2.
	MinGroupTaskSize int `yaml:"min_group_task_size"`

	MaintenanceAttrRemoveTimeout time.Duration `yaml:"maintenance_attr_remove_timeout"`

	UseMaintenanceAPI bool `yaml:"use_maintenance_api"`

	EnableGPUTesting bool `yaml:"enable_gpu_testing"`

	EnableFollowerProcessing bool `yaml:"enable_follower_processing"`

	MaxURC float64 `yaml:"max_urc"`
	// IgnorePMCThreshold is a max number of chunks node can have and still be able to be banned
	// even if DMC > 0 or PMC > 0 or URC > %.
	IgnorePMCThreshold int64 `yaml:"ignore_pmc_threshold"`

	// yaml/v2 does not support nested structs for structs with custom unmarshaller:
	// https://github.com/go-yaml/yaml/issues/125
	// structs can only be inlined and no pointers are allowed.
	// todo migrate to v3
	MissingChunksThrottlerConfig MissingChunksThrottlerConfig `yaml:",inline"`

	OfflineNodeRetentionPeriod time.Duration `yaml:"offline_node_retention_period"`
	BannedNodeHoldOffPeriod    time.Duration `yaml:"banned_node_hold_off_period"`
	// DisabledSchedulerJobsWaitTimeout is a max time to wait for node's scheduler jobs to finish
	// in fast decommission scenarios.
	DisabledSchedulerJobsWaitTimeout time.Duration `yaml:"disabled_scheduler_jobs_wait_timeout"`
	// DisabledWriteSessionsWaitTimeout is a max time to wait for node's write sessions to finish
	// in fast decommission scenarios.
	DisabledWriteSessionsWaitTimeout time.Duration `yaml:"disabled_write_sessions_wait_timeout"`

	// NOCTaskDisabledSchedulerJobsWaitTimeout is a max time to wait for node's scheduler jobs to finish
	// in fast noc decommission scenarios.
	NOCTaskDisabledSchedulerJobsWaitTimeout time.Duration `yaml:"noc_task_disabled_scheduler_jobs_wait_timeout"`
	// NOCTaskDisabledWriteSessionsWaitTimeout is a max time to wait for node's write sessions to finish
	// in fast noc decommission scenarios.
	NOCTaskDisabledWriteSessionsWaitTimeout time.Duration `yaml:"noc_task_disabled_write_sessions_wait_timeout"`

	OfflineHTTPProxyRetentionPeriod time.Duration `yaml:"offline_http_proxy_retention_period"`
	MaxHTTPProxiesPerRole           float64       `yaml:"max_http_proxies_per_role"`
	MaxRPCProxiesPerRole            float64       `yaml:"max_rpc_proxies_per_role"`

	RateLimits    RateLimitConfig `yaml:",inline"`
	GPURateLimits RateLimitConfig `yaml:"gpu_rate_limits"`

	UseMaxOfflineNodesConstraint bool `yaml:"use_max_offline_nodes_constraint"`
	MaxOfflineNodes              int  `yaml:"max_offline_nodes"`

	CPUReserve        float64 `yaml:"cpu_reserve"`
	CPUReservePercent float64 `yaml:"cpu_reserve_percent"`
	GPUReserve        float64 `yaml:"gpu_reserve"`

	UseReservePool  bool       `yaml:"use_reserve_pool"`
	ReservePoolPath ypath.Path `yaml:"reserve_pool_path"`

	// BundleSlotReserve is a min number of free tablet slots (available for a bundle)
	// required for node (belonging to bundle) decommission.
	//
	// Only used for bundles which have balancer disabled.
	BundleSlotReserve       int `yaml:"bundle_slot_reserve"`
	TabletCommonNodeReserve int `yaml:"tablet_common_node_reserve"`

	// UnconfirmedChunkCheckTimeout is a timeout that handles slow decommissions.
	// If node is still not decommissioned after this timeout and it has small number of chunks
	// all of which are unconfirmed node is considered decommissioned.
	UnconfirmedChunkCheckTimeout time.Duration `yaml:"unconfirmed_chunk_check_timeout"`
	// UnconfirmedChunkThreshold is a max number of unconfirmed chunks a node can have
	// and still be considered decommissioned.
	UnconfirmedChunkThreshold int64 `yaml:"unconfirmed_chunk_threshold"`

	ColocationConfigs ColocationConfigs `yaml:"-"`
	// HostAnnotationPeriod is a time between two consecutive requests to Wall-e API
	// in order to get additional host info.
	HostAnnotationPeriod time.Duration `yaml:"host_annotation_period"`
}

func (c *TaskProcessorConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain TaskProcessorConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.TaskProcessingPeriod <= 0 {
		return xerrors.Errorf("task poll period must be positive, got %s",
			c.TaskProcessingPeriod)
	}

	if c.ChunkIntegrityCheckPeriod <= 0 {
		return xerrors.Errorf("chunk integrity check period must be positive, got %s",
			c.ChunkIntegrityCheckPeriod)
	}

	c.MissingChunksThrottlerConfig.init()

	if c.MaxParallelGroupTasks <= 0 {
		c.MaxParallelGroupTasks = defaultMaxParallelGroupTasks
	}

	if c.MinGroupTaskSize <= 0 {
		c.MinGroupTaskSize = defaultMinGroupTaskSize
	}

	if c.OfflineNodeRetentionPeriod <= 0 {
		c.OfflineNodeRetentionPeriod = defaultOfflineNodeRetentionPeriod
	}

	if c.BannedNodeHoldOffPeriod <= 0 {
		c.BannedNodeHoldOffPeriod = defaultBannedNodeHoldOffPeriod
	}

	if c.DisabledSchedulerJobsWaitTimeout <= 0 {
		c.DisabledSchedulerJobsWaitTimeout = defaultDisabledSchedulerJobsWaitTimeout
	}

	if c.DisabledWriteSessionsWaitTimeout <= 0 {
		c.DisabledWriteSessionsWaitTimeout = defaultDisabledWriteSessionsWaitTimeout
	}

	if c.NOCTaskDisabledSchedulerJobsWaitTimeout <= 0 {
		c.NOCTaskDisabledSchedulerJobsWaitTimeout = defaultDisabledSchedulerJobsWaitTimeout
	}

	if c.NOCTaskDisabledWriteSessionsWaitTimeout <= 0 {
		c.NOCTaskDisabledWriteSessionsWaitTimeout = defaultDisabledWriteSessionsWaitTimeout
	}

	if c.OfflineHTTPProxyRetentionPeriod <= 0 {
		c.OfflineHTTPProxyRetentionPeriod = defaultOfflineHTTPProxyRetentionPeriod
	}

	if c.MaxHTTPProxiesPerRole <= 0 {
		c.MaxHTTPProxiesPerRole = defaultMaxHTTPProxiesPerRole
	}

	if c.MaxRPCProxiesPerRole <= 0 {
		c.MaxRPCProxiesPerRole = defaultMaxRPCProxiesPerRole
	}

	if c.UseMaxOfflineNodesConstraint {
		if c.MaxOfflineNodes < 0 {
			return xerrors.New("max offline nodes must be non-negative")
		}
	}

	if c.CPUReserve <= 0 {
		c.CPUReserve = defaultCPUReserve
	}

	if c.CPUReservePercent < 0 || c.CPUReservePercent > 100.0 {
		return xerrors.New("cpu reserve percent must be in [0, 100]")
	}

	if c.GPUReserve < 0 {
		return xerrors.New("gpu reserve must be non-negative")
	}

	if c.UseReservePool {
		if !strings.HasPrefix(c.ReservePoolPath.String(), ytsys.PoolTreesPath.String()) {
			return xerrors.New("reserve pool path should start with " + ytsys.PoolTreesPath.String())
		}
	}

	if c.BundleSlotReserve < 0 {
		c.BundleSlotReserve = defaultBundleSlotReserve
	}

	if c.TabletCommonNodeReserve < 0 {
		c.TabletCommonNodeReserve = defaultTabletCommonNodeReserve
	}

	if c.UnconfirmedChunkCheckTimeout <= 0 {
		c.UnconfirmedChunkCheckTimeout = defaultUnconfirmedChunkCheckTimeout
	}

	if c.UnconfirmedChunkThreshold <= 0 {
		c.UnconfirmedChunkThreshold = defaultUnconfirmedChunkThreshold
	}

	if c.HostAnnotationPeriod <= 0 {
		c.HostAnnotationPeriod = defaultHostAnnotationPeriod
	}

	return nil
}

const (
	labelAction          = "action"
	labelProcessingState = "processing_state"
)

// TaskProcessor is an actual CMS implementation.
type TaskProcessor struct {
	// confLock allows modifying config fields concurrently in tests.
	confLock sync.Locker
	conf     *TaskProcessorConfig

	l log.Structured

	dc DiscoveryClient
	// cluster stores cached cluster state.
	cluster models.Cluster
	// storage is storage of wall-e tasks.
	storage Storage

	colocationLimits *ColocationRateLimits
	startrekClient   StartrekClient

	httpProxyRoleLimits *ProxyRoleLimits
	rpcProxyRoleLimits  *ProxyRoleLimits
	rateLimiter         *Limiter
	gpuRateLimiter      *Limiter

	reservePool *ReservePool

	// taskCache is an in-memory representation of all tasks stored in storage.
	// taskCache is rebuilt on each task processing iteration.
	taskCache *models.TaskCache

	// hostAnnotations store additional host information retrieved from Wall-e.
	hostAnnotations *HostAnnotations

	// lastNodeBanTime stores a time of the latest node ban made by CMS.
	// This value is used to limit the number of parallel node bans.
	lastNodeBanTime time.Time
	// lastBannedNodeGroup stores group name of a last banned node
	// belonging to a group task.
	lastBannedNodeGroup string

	chunkIntegrity         *ytsys.ChunkIntegrity
	missingChunksThrottler *MissingChunksThrottler

	// Metrics.
	taskProcessingLoop     metrics.Timer
	tasksInProcess         metrics.GaugeVec
	gpuTasksInProcess      metrics.GaugeVec
	taskProcessingDuration metrics.Timer

	chunkIntegrityCheckErrors       metrics.Counter
	manualConfirmationErrors        metrics.Counter
	storageErrors                   metrics.Counter
	taskCacheBuildErrors            metrics.Counter
	failedTaskUpdateLoops           metrics.Counter
	unknownStateTasks               metrics.Counter
	startrekErrors                  metrics.Counter
	failedMaintenanceRequestUpdates metrics.Counter
	failedBans                      metrics.Counter
	failedUnbans                    metrics.Counter
	hostAnnotationsReloadErrors     metrics.Counter

	reservePoolCPULimit  metrics.Gauge
	maxTasksInProcess    metrics.IntGauge
	maxGPUTasksInProcess metrics.IntGauge
}

// NewTaskProcessor creates task processor with given params.
func NewTaskProcessor(conf *TaskProcessorConfig, l log.Structured, dc DiscoveryClient,
	c models.Cluster, s Storage) *TaskProcessor {

	p := &TaskProcessor{
		confLock:         new(nopLocker),
		conf:             conf,
		l:                l,
		dc:               dc,
		cluster:          c,
		storage:          s,
		colocationLimits: NewColocationRateLimits(l, conf.ColocationConfigs),
		startrekClient: startrek.NewClient(&startrek.ClientConfig{
			OAuthToken: conf.StartrekConfig.OAuthToken,
			URL:        conf.StartrekConfig.URL,
		}),
		reservePool:     NewReservePool(dc, conf.ReservePoolPath, nil),
		hostAnnotations: NewHostAnnotations(s, walle.NewClient(), l),
	}

	p.reset(nil)

	return p
}

func (p *TaskProcessor) reset(tasks []*models.Task) {
	p.resetMissingChunksThrottler()
	p.httpProxyRoleLimits = NewProxyRoleLimits(p.Conf().MaxHTTPProxiesPerRole, &httpProxyCache{c: p.cluster})
	p.rpcProxyRoleLimits = NewProxyRoleLimits(p.Conf().MaxRPCProxiesPerRole, &rpcProxyCache{c: p.cluster})
	p.initRateLimiter(tasks)
	p.initGPURateLimiter(tasks)
	p.initLastNodeBanTime(tasks)
}

func (p *TaskProcessor) resetMissingChunksThrottler() {
	p.missingChunksThrottler = NewMissingChunksThrottler(&p.conf.MissingChunksThrottlerConfig)
}

func (p *TaskProcessor) resetReservePool(tasks []*models.Task) {
	p.reservePool = NewReservePool(p.dc, p.conf.ReservePoolPath, tasks)
}

func (p *TaskProcessor) ResetLeaderMetrics() {
	p.reservePoolCPULimit.Set(0)
}

func (p *TaskProcessor) initRateLimiter(tasks []*models.Task) {
	if p.rateLimiter != nil {
		return
	}

	activeHostCount := 0
	for _, t := range tasks {
		if t.ProcessingState != models.StateNew && !p.isGPUTask(t) {
			activeHostCount += len(t.Hosts)
		}
	}

	p.l.Info("setting non-GPU rate limits", log.Any("config", p.conf.RateLimits), log.Int("active_host_count", activeHostCount))
	p.rateLimiter = NewRateLimiter(&p.conf.RateLimits, activeHostCount)
}

func (p *TaskProcessor) initGPURateLimiter(tasks []*models.Task) {
	if p.gpuRateLimiter != nil {
		return
	}

	activeHostCount := 0
	for _, t := range tasks {
		if t.ProcessingState != models.StateNew && p.isGPUTask(t) {
			activeHostCount += len(t.Hosts)
		}
	}

	p.l.Info("setting GPU rate limits", log.Any("config", p.conf.GPURateLimits), log.Int("active_host_count", activeHostCount))
	p.gpuRateLimiter = NewRateLimiter(&p.conf.GPURateLimits, activeHostCount)
}

// initLastNodeBanTime initializes last node ban time.
func (p *TaskProcessor) initLastNodeBanTime(tasks []*models.Task) {
	if !p.lastNodeBanTime.IsZero() {
		return
	}

	for _, t := range tasks {
		for _, n := range t.GetNodes() {
			banTime := time.Time(n.BanTime)
			if n.Banned && banTime.After(p.lastNodeBanTime) {
				p.lastNodeBanTime = banTime
				if t.IsGroupTask {
					p.lastBannedNodeGroup = t.TaskGroup
				}
			}
		}
	}

	p.l.Info("setting last node ban time",
		log.Time("last_node_ban_time", p.lastNodeBanTime),
		log.String("last_banned_node_group", p.lastBannedNodeGroup))
}

// ensureState initializes task processor.
//
// The operation is retried infinitely until success.
//
// Can be stopped via ctx.
func (p *TaskProcessor) ensureState(ctx context.Context) error {
	for {
		tasks, err := p.storage.GetAll(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			p.l.Error("error retrieving tasks", log.Error(err))
			p.storageErrors.Inc()
			continue
		}
		p.reset(tasks)
		break
	}
	return nil
}

func (p *TaskProcessor) RegisterMetrics(r metrics.Registry) {
	p.taskProcessingLoop = r.Timer("task_processing_loop")
	p.tasksInProcess = r.GaugeVec("tasks_in_process", []string{labelAction, labelProcessingState})
	p.gpuTasksInProcess = r.GaugeVec("gpu_tasks_in_process", []string{labelAction, labelProcessingState})
	p.taskProcessingDuration = r.DurationHistogram("task_processing_duration", metrics.NewDurationBuckets(
		time.Minute,
		time.Minute*5,
		time.Minute*15,
		time.Minute*30,
		time.Hour,
		time.Hour*4,
		time.Hour*12,
	))

	p.chunkIntegrityCheckErrors = r.Counter("chunk_integrity_check_errors")
	p.manualConfirmationErrors = r.Counter("manual_confirmation_errors")
	p.storageErrors = r.Counter("task_storage_errors")
	p.taskCacheBuildErrors = r.Counter("task_cache_build_errors")
	p.failedTaskUpdateLoops = r.Counter("failed_task_update_loops")
	p.unknownStateTasks = r.Counter("unknown_state_tasks")
	p.startrekErrors = r.Counter("startrek_errors")
	p.failedMaintenanceRequestUpdates = r.Counter("failed_maintenance_requests_updates")
	p.failedBans = r.Counter("failed_bans")
	p.failedUnbans = r.Counter("failed_unbans")
	p.hostAnnotationsReloadErrors = r.Counter("host_annotations_reload_errors")

	p.reservePoolCPULimit = r.Gauge("reserve_pool_cpu_limit")
	p.maxTasksInProcess = r.IntGauge("max_tasks_in_process")
	p.maxTasksInProcess.Set(int64(p.conf.RateLimits.MaxParallelHosts))
	p.maxGPUTasksInProcess = r.IntGauge("max_gpu_tasks_in_process")
	p.maxGPUTasksInProcess.Set(int64(p.conf.GPURateLimits.MaxParallelHosts))

	p.colocationLimits.RegisterMetrics(r)
}

func (p *TaskProcessor) resetLoopMetrics() {
	for _, action := range walle.HostActions {
		for _, state := range models.TaskProcessingStates {
			p.tasksInProcess.With(map[string]string{
				labelAction:          string(action),
				labelProcessingState: string(state),
			}).Set(0.0)
			p.gpuTasksInProcess.With(map[string]string{
				labelAction:          string(action),
				labelProcessingState: string(state),
			}).Set(0.0)
		}
	}
}

func (p *TaskProcessor) Conf() *TaskProcessorConfig {
	p.confLock.Lock()
	defer p.confLock.Unlock()

	return p.conf
}

// UpdateConf replaces current config with a result of applying f to a copy of current config.
func (p *TaskProcessor) UpdateConf(f func(c *TaskProcessorConfig)) {
	newConf := *p.Conf()
	f(&newConf)

	p.confLock.Lock()
	p.conf = &newConf
	p.confLock.Unlock()
}

// Run starts all processor's goroutines.
func (p *TaskProcessor) Run(ctx context.Context) error {
	if err := p.ensureState(ctx); err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return p.run(gctx)
	})

	g.Go(func() error {
		return p.runHostAnnotator(gctx)
	})

	return g.Wait()
}

// run starts task processing loop.
// Active tasks are periodically queried from storage, processed and written back.
//
// All interaction with Wall-e is done via storage.
func (p *TaskProcessor) run(ctx context.Context) error {
	integrityCheckTicker := time.NewTicker(p.Conf().ChunkIntegrityCheckPeriod)
	defer integrityCheckTicker.Stop()

	taskPollTicker := time.NewTicker(p.Conf().TaskProcessingPeriod)
	defer taskPollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.resetLoopMetrics()
			return ctx.Err()
		case <-integrityCheckTicker.C:
			if err := p.checkChunkIntegrity(ctx); err != nil {
				p.l.Error("chunk integrity check failed", log.Error(err))
				p.chunkIntegrityCheckErrors.Inc()
			} else {
				p.l.Info("chunk integrity check passed")
			}
		case <-taskPollTicker.C:
			if err := p.cluster.Err(); err != nil {
				p.l.Info("last cluster poll failed; limiting tasks processing", log.Error(err))
			}
			if err := p.updateTasks(ctx); err != nil {
				p.l.Error("tasks update failed", log.Error(err))
				p.failedTaskUpdateLoops.Inc()
			} else {
				p.l.Info("tasks update succeeded")
			}
		}
	}
}

// runHostAnnotator starts periodic process that retrieves additional host information from Wall-e.
func (p *TaskProcessor) runHostAnnotator(ctx context.Context) error {
	t := time.NewTicker(p.Conf().TaskProcessingPeriod)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			p.l.Info("reloading host annotations")
			if err := p.hostAnnotations.Reload(ctx); err != nil {
				p.l.Error("host annotations reload failed", log.Error(err))
				p.hostAnnotationsReloadErrors.Inc()
			} else {
				p.l.Info("host annotations reloaded")
			}
		}
	}
}

func (p *TaskProcessor) checkChunkIntegrity(ctx context.Context) error {
	defer func() {
		p.missingChunksThrottler.OnChunkIntegrityUpdate(p.chunkIntegrity)
	}()

	i, err := p.dc.GetIntegrityIndicators(ctx)
	if err != nil {
		p.l.Error("chunk integrity update failed", log.Error(err))
		p.chunkIntegrity = nil
		return err
	}
	p.l.Info("successfully queried chunk integrity indicators", log.String("indicators", i.String()))
	p.chunkIntegrity = i

	if i.LVC > 0 || i.QMC > 0 {
		p.l.Info("LVC > 0 or QMC > 0 -> unbanning nodes", log.Int64("lvc", i.LVC), log.Int64("qmc", i.QMC))
		if err := p.unbanNodes(ctx); err != nil {
			p.l.Error("error unbanning nodes", log.Error(err))
		} else {
			p.lastNodeBanTime = time.Time{}
		}
	} else {
		p.l.Info("LVC is 0")
	}

	if !i.Check(p.Conf().MaxURC) {
		p.l.Info("chunk integrity is broken", log.String("indicators", i.String()))
		return xerrors.Errorf("chunk integrity broken: %s", i.String())
	}
	p.l.Info("chunk integrity is intact", log.String("indicators", i.String()))

	return nil
}

// updateTasks loads active tasks from storage, processes them and writes updated ones back.
func (p *TaskProcessor) updateTasks(ctx context.Context) error {
	p.l.Info("updating tasks")

	start := time.Now()
	defer p.taskProcessingLoop.RecordDuration(time.Since(start))

	tasks, err := p.storage.GetAll(ctx)
	if err != nil {
		p.l.Error("error retrieving tasks", log.Error(err))
		p.storageErrors.Inc()
		return err
	}
	p.l.Info("retrieved tasks from storage", log.Int("count", len(tasks)))
	p.logClusterState(tasks)

	p.resetLoopMetrics()
	for _, t := range tasks {
		p.tasksInProcess.With(map[string]string{
			labelAction:          string(t.Action),
			labelProcessingState: string(t.ProcessingState),
		}).Add(1.0)
		if p.isGPUTask(t) {
			p.gpuTasksInProcess.With(map[string]string{
				labelAction:          string(t.Action),
				labelProcessingState: string(t.ProcessingState),
			}).Add(1.0)
		}
	}

	p.taskCache, err = models.NewTaskCache(tasks)
	if err != nil {
		p.l.Error("error building task cache", log.Error(err))
		p.taskCacheBuildErrors.Inc()
		return err
	}

	for _, t := range tasks {
		for _, h := range t.Hosts {
			p.preprocessHost(ctx, t, t.HostStates[h])
		}
	}

	if p.conf.UseReservePool {
		p.resetReservePool(tasks)
		if err := p.reservePool.UpdateStrongGuarantees(ctx); err != nil {
			return err
		}
		if limit := p.reservePool.GetCPULimit(); limit != 0 {
			p.reservePoolCPULimit.Set(limit)
		}
	}

	tasks = p.makeProcessingPlan(ctx, tasks)
	p.processConfirmationRequests(ctx, tasks)

	for _, t := range tasks {
		s := t.ProcessingState
		switch s {
		case models.StateNew, models.StatePending, models.StateDecommissioned,
			models.StateProcessed, models.StateConfirmedManually:
			p.processTask(ctx, t)
		default:
			p.l.Error("unknown processing state", log.String("state", string(s)))
			p.unknownStateTasks.Inc()
		}
	}

	return nil
}

// isGPUTask checks that task relates to only gpu nodes.
//
// For now we only have 1 gpu node per host.
// Anything other than that is considered non-gpu.
func (p *TaskProcessor) isGPUTask(t *models.Task) bool {
	nodes := t.GetNodes()

	if len(nodes) != 1 {
		return false
	}

	nodeComponent, ok := p.resolveNodeComponent(t, nodes[0])
	if ok && nodeComponent.HasTag("gpu") {
		return true
	}

	return false
}

// makeProcessingPlan reorders and filters tasks based on their and cluster states.
func (p *TaskProcessor) makeProcessingPlan(ctx context.Context, tasks []*models.Task) []*models.Task {
	groups, nonGroupTasks := p.partitionTasks(tasks)

	var withoutChunks, accepted, deleted, decommissioned, processed,
		manuallyConfirmed, requestedManualConfirmation, other []*models.Task

	for _, t := range nonGroupTasks {
		if t.ConfirmationRequested {
			requestedManualConfirmation = append(requestedManualConfirmation, t)
			continue
		}
		if t.DeletionRequested {
			deleted = append(deleted, t)
			continue
		}
		if t.ProcessingState == models.StateNew {
			accepted = append(accepted, t)
			continue
		}
		if p.taskConsistsOfNodesWithoutChunks(t) {
			withoutChunks = append(withoutChunks, t)
			continue
		}
		switch t.ProcessingState {
		case models.StateNew:
		case models.StateDecommissioned:
			decommissioned = append(decommissioned, t)
		case models.StateProcessed:
			processed = append(processed, t)
		case models.StateConfirmedManually:
			manuallyConfirmed = append(manuallyConfirmed, t)
		default:
			other = append(other, t)
		}
	}

	for _, t := range requestedManualConfirmation {
		if t.ProcessingState == models.StateNew {
			_ = p.addNewTaskToProcessing(ctx, t)
		}
	}

	// hosts is a set of in-process hosts.
	hosts := make(map[string]struct{})
	gpuHosts := make(map[string]struct{})
	addHosts := func(tasks ...*models.Task) {
		for _, t := range tasks {
			hostsByTaskType := hosts
			if p.isGPUTask(t) {
				hostsByTaskType = gpuHosts
			}

			for _, h := range t.Hosts {
				hostsByTaskType[h] = struct{}{}
			}
		}
	}
	addHosts(other...)

	var ret []*models.Task
	addTasks := func(tasks ...*models.Task) {
		ret = append(ret, tasks...)
		addHosts(tasks...)
	}

	addTasks(requestedManualConfirmation...)

	if err := p.cluster.Err(); err != nil {
		p.l.Info("cluster is in a broken state; processing only manual confirmation requests", log.Error(err))
		return ret
	}

	if p.chunkIntegrity == nil {
		p.l.Info("chunk integrity state is unknown; processing only manual confirmation requests")
		return ret
	}

	addTasks(deleted...)
	addTasks(withoutChunks...)

	if p.chunkIntegrity.LVC > 0 || p.chunkIntegrity.QMC > 0 {
		p.l.Info("LVC > 0 and/or QMC > 0; processing only nodes without chunks, " +
			"returned nodes and unprocessed manual confirmation requests")
		return ret
	}

	addTasks(decommissioned...)
	addTasks(processed...)
	addTasks(manuallyConfirmed...)

	for i, g := range groups {
		id := g.GetGroupID()
		if i+1 > p.conf.MaxParallelGroupTasks {
			p.l.Info("skipping task group due to rate limit", log.String("group_id", id),
				log.Int("max_parallel_group_tasks", p.conf.MaxParallelGroupTasks))
			continue
		}
		p.l.Info("adding task group to processing", log.String("group_id", id))

		for _, t := range g {
			if t.ProcessingState == models.StateNew {
				if err := p.addNewTaskToProcessing(ctx, t); err != nil {
					continue
				}
			}
			if !t.IsGroupTask {
				if err := p.addTaskToGroup(ctx, t); err != nil {
					continue
				}
			}
			p.l.Info("adding group task to processing", log.Any("task", t),
				log.String("group_id", id))
			addTasks(t)
		}
	}

	addNewTask := func(t *models.Task) {
		if t.ProcessingState != models.StateNew {
			return
		}

		isGPUTask := p.isGPUTask(t)
		hostsByTaskType := hosts
		if isGPUTask {
			hostsByTaskType = gpuHosts
		}

		newHostCount := 0
		for _, h := range t.Hosts {
			if _, ok := hostsByTaskType[h]; !ok {
				newHostCount++
			}
		}

		limiter := p.rateLimiter
		if isGPUTask {
			limiter = p.gpuRateLimiter
		}

		if !t.ConfirmationRequested && len(hostsByTaskType)+newHostCount > limiter.Config.MaxParallelHosts {
			p.l.Info("skipping new task due to rate limit", log.Any("task", t),
				log.Int("max_parallel_hosts", limiter.Config.MaxParallelHosts))
			return
		}

		start := time.Now()
		reserve := limiter.ReserveN(start, newHostCount)

		if !t.ConfirmationRequested && (!reserve.OK() || reserve.Delay() != 0) {
			p.l.Info("skipping new task due to rate limit", log.Any("task", t),
				log.Duration("wait_time", reserve.Delay()))
			reserve.CancelAt(start)
			return
		}

		if err := p.addNewTaskToProcessing(ctx, t); err != nil {
			reserve.CancelAt(start)
			return
		}

		addTasks(t)
	}

	// Order new tasks so that all master tasks come first.
	// Other tasks are ordered by id.
	sort.Slice(accepted, func(i, j int) bool {
		if accepted[i].HasMasterRole() {
			return !accepted[j].HasMasterRole() || accepted[i].ID < accepted[j].ID
		}
		return !accepted[j].HasMasterRole() && accepted[i].ID < accepted[j].ID
	})

	// Add new tasks.
	for _, t := range accepted {
		addNewTask(t)
	}

	if err := p.missingChunksThrottler.Allow(); err != nil {
		p.l.Info("throttling task processing due to missing part chunks", log.Error(err))
		return ret
	}

	p.l.Info("chunk integrity", log.String("indicators", p.chunkIntegrity.String()))
	addTasks(other...)

	return ret
}

// partitionTasks returns task groups and individual tasks not belonging to any group.
//
// Groups are ordered by group id. Non-group tasks are ordered by id.
//
// Groups will be empty unless enabled in config.
//
// Manually confirmed tasks do not belong to any group.
func (p *TaskProcessor) partitionTasks(tasks []*models.Task) ([]models.TaskGroup, []*models.Task) {
	groupByID := make(map[string]models.TaskGroup)
	for _, t := range tasks {
		nodeSetID := ""
		if t.MaintenanceInfo != nil && !t.ConfirmationRequested {
			nodeSetID = t.MaintenanceInfo.NodeSetID
		}
		if _, ok := groupByID[nodeSetID]; !ok {
			groupByID[nodeSetID] = make(models.TaskGroup, 0)
		}
		groupByID[nodeSetID] = append(groupByID[nodeSetID], t)
	}

	// isSingleHostGroup checks if group consists of the tasks for a single host
	// e.g. tasks for different host pods.
	isSingleHostGroup := func(g []*models.Task) bool {
		hosts := make(map[string]struct{})
		for _, t := range g {
			for _, h := range t.Hosts {
				hosts[h] = struct{}{}
			}
		}
		return len(hosts) == 1
	}

	var nonGroupTasks []*models.Task
	for id, g := range groupByID {
		if id == "" || len(g) < p.conf.MinGroupTaskSize || !p.conf.EnableGroupTasks || isSingleHostGroup(g) {
			for _, t := range g {
				nonGroupTasks = append(nonGroupTasks, t)
			}
			delete(groupByID, id)
		}
	}

	var groups []models.TaskGroup
	for _, g := range groupByID {
		groups = append(groups, g)
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].GetGroupID() < groups[j].GetGroupID()
	})

	sort.Slice(nonGroupTasks, func(i, j int) bool {
		return nonGroupTasks[i].ID < nonGroupTasks[j].ID
	})

	return groups, nonGroupTasks
}

// taskConsistsOfNodesWithoutChunks checks if task consists only of nodes
// and all those nodes have no chunks.
func (p *TaskProcessor) taskConsistsOfNodesWithoutChunks(t *models.Task) bool {
	return p.taskConsistsOfNodesWithFewChunks(t, 0)
}

// taskConsistsOfNodesWithFewChunks checks if task consists only of nodes
// and all those nodes have no more than given number of chunks.
func (p *TaskProcessor) taskConsistsOfNodesWithFewChunks(t *models.Task, maxChunkCount int64) bool {
	nodes := t.GetNodes()
	if len(t.HostStates) != len(nodes) || len(nodes) == 0 {
		return false
	}

	for _, n := range nodes {
		node, ok := p.resolveNodeComponent(t, n)
		if !ok {
			return false
		}
		if node.Statistics.TotalStoredChunkCount > maxChunkCount {
			return false
		}
	}

	return true
}

// addNewTaskToProcessing changes task processing state in storage from 'new' to 'pending'.
func (p *TaskProcessor) addNewTaskToProcessing(ctx context.Context, t *models.Task) error {
	t.ProcessingState = models.StatePending
	if err := p.storage.Update(ctx, t); err != nil {
		p.l.Info("error adding new task to processing", log.Any("task", t), log.Error(err))
		p.storageErrors.Inc()
		return err
	}
	p.l.Info("new task proceeded to processing", log.Any("task", t))
	return nil
}

// addTaskToGroup marks task as a group task.
func (p *TaskProcessor) addTaskToGroup(ctx context.Context, t *models.Task) error {
	t.IsGroupTask = true
	if err := p.storage.Update(ctx, t); err != nil {
		p.l.Info("error adding task to group", log.Any("task", t), log.Error(err))
		p.storageErrors.Inc()
		return err
	}
	p.l.Info("task was added to group", log.Any("task", t))
	return nil
}

func (p *TaskProcessor) processConfirmationRequests(ctx context.Context, tasks []*models.Task) {
	p.l.Info("processing manual confirmation requests")

	for _, t := range tasks {
		if !t.ConfirmationRequested {
			continue
		}
		if err := p.doManualConfirmation(ctx, t); err != nil {
			p.l.Error("manual confirmation failed", log.Error(err))
			p.manualConfirmationErrors.Inc()
		} else {
			p.l.Info("manual confirmation done", log.Any("task", t))
		}
	}
}

// doManualConfirmation performs actual confirmation that updates task status in storage.
func (p *TaskProcessor) doManualConfirmation(ctx context.Context, task *models.Task) error {
	if task.ProcessingState == models.StateConfirmedManually && task.WalleStatus == walle.StatusOK {
		p.l.Info("task is already confirmed", log.String("task_id", string(task.ID)))
		return nil
	}

	p.l.Info("performing manual task confirmation", log.Any("task", task))
	task.ConfirmManually(models.DecisionMaker(task.ConfirmationRequestedBy))
	if err := p.storage.Update(ctx, task); err != nil {
		return err
	}

	p.l.Info("editing status of related tickets", log.Any("task", task))
	for _, m := range task.GetMasters() {
		p.ensureMasterMaintenance(ctx, task, m)
		p.startProcessingTicket(ctx, task, m)
	}

	return nil
}

type taskKeyType int

// taskKey is a key used to access task in ctx.
var taskKey taskKeyType

func (p *TaskProcessor) processTask(ctx context.Context, t *models.Task) {
	p.l.Info("processing task", log.Any("task", t))

	// Check that latest cluster state update was after the task was created.
	clusterReloadTime := p.cluster.LastReloadTime()
	if time.Time(t.CreatedAt).After(clusterReloadTime) {
		p.l.Error("skipping task due to stale cluster snapshot",
			log.String("task_id", string(t.ID)),
			log.Time("task_creation_time", time.Time(t.CreatedAt)),
			log.Time("cluster_reload_time", clusterReloadTime),
			log.Error(p.cluster.Err()))
		return
	}
	p.l.Info("task was created before last successful cluster state update -> proceeding to precessing",
		log.String("task_id", string(t.ID)))

	taskCtx := context.WithValue(ctx, taskKey, t)
	for _, h := range t.Hosts {
		p.processHost(taskCtx, t.HostStates[h])
	}

	// Update task state in storage.
	if t.AllHostsFinished() {
		t.SetFinished()
		p.l.Info("all task hosts finished", log.Any("task", t))
		p.taskProcessingDuration.RecordDuration(time.Time(t.UpdatedAt).Sub(time.Time(t.CreatedAt)))
		p.tryUpdateTaskInStorage(ctx, t)
		return
	} else {
		p.l.Info("not all task hosts are finished", log.String("task_id", string(t.ID)))
	}

	// Update task state in storage.
	if t.AllHostsProcessed() {
		t.SetProcessed()
		p.l.Info("all task hosts processed", log.Any("task", t))
		p.tryUpdateTaskInStorage(ctx, t)
		return
	} else {
		p.l.Info("not all task hosts are processed", log.String("task_id", string(t.ID)))
	}

	if t.AllHostsDecommissioned() {
		t.SetDecommissioned()
		p.l.Info("all task hosts decommissioned", log.Any("task", t))
		p.tryUpdateTaskInStorage(ctx, t)
	} else {
		p.l.Info("not all task hosts are decommissioned", log.String("task_id", string(t.ID)))
	}
}

type hostKeyType int

// hostKey is a key used to access host in ctx.
var hostKey hostKeyType

func (p *TaskProcessor) processHost(ctx context.Context, h *models.Host) {
	task := ctx.Value(taskKey).(*models.Task)

	p.l.Info("processing host", log.Any("host", h))

	p.updateRoles(ctx, task, h)
	p.annotateHost(ctx, task, h)

	hostCtx := context.WithValue(ctx, hostKey, h)

	if len(h.Roles) == 0 {
		p.processNotYTHost(ctx, h)
		return
	}

	for _, r := range h.Roles {
		p.processRole(hostCtx, r)
	}

	// Update host state in storage.
	if h.AllRolesFinished() {
		h.SetFinished()

		p.l.Info("all host roles finished",
			log.String("task_id", string(task.ID)), log.Any("host", h))

		task := ctx.Value(taskKey).(*models.Task)
		p.tryUpdateTaskInStorage(ctx, task)
		return
	} else {
		p.l.Info("not all host roles are finished",
			log.String("task_id", string(task.ID)), log.String("host", h.Host))
	}

	// Update host state in storage.
	if h.AllRolesProcessed() {
		h.SetProcessed()

		p.l.Info("all host roles processed",
			log.String("task_id", string(task.ID)), log.Any("host", h))

		task := ctx.Value(taskKey).(*models.Task)
		p.tryUpdateTaskInStorage(ctx, task)
		return
	} else {
		p.l.Info("not all host roles are processed",
			log.String("task_id", string(task.ID)), log.String("host", h.Host))
	}

	// Update host state in storage.
	if h.AllRolesDecommissioned() {
		h.SetDecommissioned()

		p.l.Info("all host roles decommissioned",
			log.String("task_id", string(task.ID)), log.Any("host", h))

		task := ctx.Value(taskKey).(*models.Task)
		p.tryUpdateTaskInStorage(ctx, task)
	} else {
		p.l.Info("not all host roles are decommissioned",
			log.String("task_id", string(task.ID)), log.String("host", h.Host))
	}
}

func (p *TaskProcessor) preprocessHost(ctx context.Context, t *models.Task, h *models.Host) {
	p.updateRoles(ctx, t, h)

	components, ok := p.cluster.GetHostComponents(h.Host)
	if !ok {
		p.l.Info("no cluster components found", log.String("host", h.Host))
		return
	}

	for path, r := range h.Roles {
		c, ok := components[path]
		if !ok {
			continue
		}

		switch r.Type {
		case ytsys.RoleNode:
			node := c.(*ytsys.Node)
			role := r.Role.(*models.Node)
			_ = p.startNodeMaintenance(ctx, t, node, role)
			_ = p.fillNodeFlavors(ctx, t, node, role)

		case ytsys.RoleRPCProxy:
			_ = p.startRPCProxyMaintenance(ctx, t, c.(*ytsys.RPCProxy), r.Role.(*models.RPCProxy))
		case ytsys.RolePrimaryMaster, ytsys.RoleSecondaryMaster, ytsys.RoleTimestampProvider:
			if !p.conf.EnableFollowerProcessing || p.cluster.Err() != nil {
				if t.ProcessingState != models.StateProcessed {
					p.createTicket(ctx, t, r.Role.(*models.Master))
				}
			}
		}
	}
}

// updateRoles synchronizes host roles with cluster.
func (p *TaskProcessor) updateRoles(ctx context.Context, t *models.Task, h *models.Host) {
	components, ok := p.cluster.GetHostComponents(h.Host)
	if !ok {
		p.l.Info("no cluster components found", log.String("host", h.Host))
		return
	}

	if t.Origin == models.OriginYP {
		// Filter other roles but the ones corresponding to the task's pod.
		filtered := make(discovery.HostComponents)
		for path, c := range components {
			if strings.Contains(path.String(), t.YPInfo.PodID) {
				filtered[path] = c
			}
		}
		components = filtered
	}

	if changed := h.UpdateRoles(components); changed {
		p.l.Info("host component set changed", log.Any("host", h))
		p.tryUpdateTaskInStorage(ctx, t)
	} else {
		p.l.Info("host component set not changed", log.String("host", h.Host))
	}
}

func (p *TaskProcessor) annotateHost(ctx context.Context, t *models.Task, h *models.Host) {
	info, ok := p.hostAnnotations.GetHostInfo(h.Host)
	if !ok {
		return
	}
	if added := h.AddTicket(startrek.TicketKey(info.Ticket)); added {
		p.l.Info("host ticket set changed", log.Any("host", h))
		p.tryUpdateTaskInStorage(ctx, t)
	} else {
		p.l.Info("host ticket set not changed", log.String("host", h.Host))
	}
}

func (p *TaskProcessor) processNotYTHost(ctx context.Context, h *models.Host) {
	task := ctx.Value(taskKey).(*models.Task)

	p.l.Info("processing non-YT host", log.String("task_id", string(task.ID)), log.Any("host", h))

	if task.DeletionRequested {
		h.SetFinished()
		p.l.Info("finishing processing of non-YT host", log.String("host", h.Host))
		p.tryUpdateTaskInStorage(ctx, task)
		return
	}

	if h.State == models.HostStateProcessed {
		return
	}

	if !p.colocationLimits.Allow(ctx, h) {
		p.l.Info("can not allow non-YT host due to rate limit",
			log.String("task_id", string(task.ID)), log.Any("host", h))
		return
	}

	p.l.Info("allowing walle to take non-YT host", log.String("host", h.Host))

	h.AllowAsForeign()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) tryUpdateTaskInStorage(ctx context.Context, task *models.Task) {
	_ = p.updateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) updateTaskInStorage(ctx context.Context, task *models.Task) error {
	if err := p.storage.Update(ctx, task); err != nil {
		p.l.Error("error updating task",
			log.String("task_id", string(task.ID)), log.Error(err))
		p.storageErrors.Inc()
		return err
	}
	p.l.Info("successfully updated task in storage", log.Any("task", task))
	return nil
}

func (p *TaskProcessor) processRole(ctx context.Context, c *models.Component) {
	switch role := c.Role.(type) {
	case *models.Master:
		p.processMaster(ctx, role)
	case *models.Scheduler:
		p.processScheduler(ctx, role)
	case *models.ControllerAgent:
		p.processControllerAgent(ctx, role)
	case *models.HTTPProxy:
		p.processHTTPProxy(ctx, role)
	case *models.RPCProxy:
		p.processRPCProxy(ctx, role)
	case *models.Node:
		p.processNode(ctx, role)
	}
}

func (p *TaskProcessor) logClusterState(tasks []*models.Task) {
	for _, t := range tasks {
		p.l.Info("cms task state", log.Any("task", t.GetClusterInfo(p.cluster)))
	}
}

// hasTaskUpgrade uses in-memory task cache to check if there is an upgrade for given task.
//
// An upgrade is an active task for the same host with bigger id.
//
// Returns false for tasks with no or more than one hosts.
func (p *TaskProcessor) hasTaskUpgrade(t *models.Task) bool {
	if len(t.Hosts) == 0 || len(t.Hosts) > 1 {
		return false // should never happen
	}

	u, ok := p.taskCache.TaskUpgrades[t.Hosts[0]]
	if !ok {
		return false
	}

	return !u.New.DeletionRequested
}

func IsNOCTask(t *models.Task) bool {
	return t.IsGroupTask && t.Action == walle.ActionTemporaryUnreachable
}
