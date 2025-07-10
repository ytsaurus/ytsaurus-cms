package cms

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	ypath "go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytsys"
)

const (
	permanentBanSubstr   = "permanent_ban"
	banMessageTimeLayout = "2006-01-02T15:04:05 MST"

	safeDecommissionRemovalSlots = 8192
)

type DecommissionOptions struct {
	SkipConstraintCheck   bool
	OverrideRemovalSlots  bool
	WaitChunkDecommission bool

	LimitDisabledSchedulerJobsWaitTime bool
	LimitDisabledSessionsWaitTime      bool
}

type nodeKeyType int

// nodeKey is a key used to access cached cluster node state in ctx.
var nodeKey nodeKeyType

func (p *TaskProcessor) processNode(ctx context.Context, r *models.Node) {
	task := ctx.Value(taskKey).(*models.Task)
	p.l.Info("processing node", p.nodeLogFields(task, r)...)

	node, ok := p.resolveNodeComponent(task, r)
	if !ok {
		// Node of deleted task was already removed from cypress.
		if p.cluster.Err() == nil && task.DeletionRequested && p.CheckPathMissing(ctx, r.Path) {
			p.l.Info("finish processing deleted node", p.nodeLogFields(task, r)...)
			r.SetFinished()
			p.tryUpdateTaskInStorage(ctx, task)
		}
		return
	}
	nodeCtx := context.WithValue(ctx, nodeKey, node)

	if task.DeletionRequested {
		p.l.Info("deletion requested -> activating node", p.nodeLogFields(task, r)...)
		p.activateNode(nodeCtx, r)
		return
	}

	if !p.checkOfflineNodesConstraint(ctx, r) {
		p.l.Info("offline node constraint failed -> skipping node", p.nodeLogFields(task, r)...)
		return
	}

	switch r.State {
	case models.NodeStateAccepted, models.NodeStateDecommissioned:
		p.processPendingNode(nodeCtx, r)
	case models.NodeStateProcessed:
	default:
		p.l.Error("unexpected node state", log.String("state", string(r.State)))
	}
}

// checkOfflineNodesConstraint checks whether there are not that many offline nodes in cluster.
func (p *TaskProcessor) checkOfflineNodesConstraint(ctx context.Context, r *models.Node) bool {
	task := ctx.Value(taskKey).(*models.Task)

	if !p.conf.UseMaxOfflineNodesConstraint {
		return true
	}

	offlineNodes, err := p.getOfflineNodes()
	if err != nil {
		p.l.Debug("unable to get offline nodes", p.nodeLogFields(task, r, log.Error(err))...)
		return false
	}

	_, nodeOffline := offlineNodes[*r.Addr]
	p.l.Info("checking max_offline_nodes constraint",
		p.nodeLogFields(task, r,
			log.Int("max_offline_nodes", p.conf.MaxOfflineNodes),
			log.Int("offline_node_count", len(offlineNodes)),
			log.Bool("node_offline", nodeOffline))...)

	offlineNodeCount := len(offlineNodes)
	if nodeOffline {
		offlineNodeCount--
	}

	return offlineNodeCount <= p.conf.MaxOfflineNodes
}

func (p *TaskProcessor) processPendingNode(ctx context.Context, r *models.Node) {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if node.Banned {
		p.l.Info("node is banned -> proceeding to decommission", p.nodeLogFields(task, r)...)
		p.decommissionNode(ctx, r)
		return
	}

	if p.checkNodeOffline(node) {
		p.l.Info("node is offline for a long time -> proceeding to decommission",
			p.nodeLogFields(task, r, log.Time("last_seen_time", time.Time(node.LastSeenTime)))...)
		p.decommissionNode(ctx, r)
		return
	}

	switch task.Action {
	case walle.ActionPrepare:
		p.decommissionSlow(ctx, r)
	case walle.ActionRepairLink, walle.ActionTemporaryUnreachable, walle.ActionRedeploy,
		walle.ActionReboot, walle.ActionProfile:
		p.decommissionFast(ctx, r)
	case walle.ActionDeactivate, walle.ActionPowerOff, walle.ActionChangeDisk:
		switch task.Type {
		case walle.TaskTypeAutomated:
			p.decommissionSlow(ctx, r)
		case walle.TaskTypeManual:
			p.decommissionFast(ctx, r)
		}
	default:
		p.l.Warn("unexpected task action", p.nodeLogFields(task, r)...)
	}
}

// decommissionSlow safely decommissions node.
//
// Used when the machine does not threaten cluster integrity
// and the request does not require any urgency.
func (p *TaskProcessor) decommissionSlow(ctx context.Context, r *models.Node) {
	o := &DecommissionOptions{
		OverrideRemovalSlots:               true,
		WaitChunkDecommission:              true,
		LimitDisabledSchedulerJobsWaitTime: false,
		LimitDisabledSessionsWaitTime:      false,
	}
	if !p.checkDecommissionReady(ctx, r, o) {
		return
	}
	p.decommissionNode(ctx, r)
}

// decommissionFast decommissions node omitting some checks (e.g. waiting for chunk decommission).
//
// Used when the machine does not threaten cluster integrity
// but the task was created manually or
// the intended action is very fast and does not lead to redeploy.
func (p *TaskProcessor) decommissionFast(ctx context.Context, r *models.Node) {
	o := &DecommissionOptions{
		OverrideRemovalSlots:               true,
		LimitDisabledSchedulerJobsWaitTime: true,
		LimitDisabledSessionsWaitTime:      true,
	}
	if !p.checkDecommissionReady(ctx, r, o) {
		return
	}
	p.decommissionNode(ctx, r)
}

// decommissionUrgent decommissions node ASAP.
//
// Used when the machine threatens cluster integrity or
// when the task was created manually.
func (p *TaskProcessor) decommissionUrgent(ctx context.Context, r *models.Node) {
	o := &DecommissionOptions{
		SkipConstraintCheck:                true,
		LimitDisabledSchedulerJobsWaitTime: true,
		LimitDisabledSessionsWaitTime:      true,
	}
	if !p.checkDecommissionReady(ctx, r, o) {
		return
	}
	p.decommissionNode(ctx, r)
}

// checkDecommissionReady checks whether node is ready to be decommissioned or not.
func (p *TaskProcessor) checkDecommissionReady(ctx context.Context, r *models.Node, o *DecommissionOptions) bool {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if err := p.startNodeMaintenance(ctx, task, node, r); err != nil {
		return false
	}

	if !o.SkipConstraintCheck {
		if !p.checkNodeConstraints(ctx, r) {
			p.l.Info("node constraints are not met -> postponing node decommission", p.nodeLogFields(task, r)...)
			return false
		}
		p.l.Info("node constraints are met -> proceeding to decommission", p.nodeLogFields(task, r)...)
	} else {
		p.l.Info("skipping node constraints check -> proceeding to decommission", p.nodeLogFields(task, r)...)
	}

	if err := p.startChunkDecommission(ctx, r); err != nil {
		return false
	}

	if o.OverrideRemovalSlots {
		if err := p.overrideRemovalSlots(ctx, r); err != nil {
			return false
		}
	}

	if err := p.disableWriteSessions(ctx, r); err != nil {
		return false
	}

	if err := p.disableSchedulerJobs(ctx, r); err != nil {
		return false
	}

	if !p.checkTabletCellsDecommissioned(ctx, r) {
		return false
	}

	if o.WaitChunkDecommission {
		if !p.checkChunksDecommissioned(ctx, r) {
			return false
		}
	}

	if !p.checkWriteSessionsDisabled(ctx, r, o.LimitDisabledSessionsWaitTime) {
		return false
	}

	if !p.checkSchedulerJobsDisabled(ctx, r, o.LimitDisabledSchedulerJobsWaitTime) {
		return false
	}

	return true
}

func (p *TaskProcessor) checkNodeConstraints(ctx context.Context, r *models.Node) bool {
	task := ctx.Value(taskKey).(*models.Task)

	if !p.checkResourceLimits(ctx, r) {
		p.l.Info("resource limits are not met -> postponing node decommission", p.nodeLogFields(task, r)...)
		return false
	}
	p.l.Info("node resource limits are met", p.nodeLogFields(task, r)...)

	if !p.checkTabletCellGuarantees(ctx, r) {
		p.l.Info("tablet cell guarantees are not met -> postponing node decommission", p.nodeLogFields(task, r)...)
		return false
	}
	p.l.Info("tablet cell guarantees are met", p.nodeLogFields(task, r)...)

	return true
}

func (p *TaskProcessor) needBanNode(ctx context.Context, r *models.Node) bool {
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if p.conf.UseMaintenanceAPI {
		return !containCMSMaintenanceRequest(node.MaintenanceRequests, yt.MaintenanceTypeBan) &&
			!r.Banned
	}

	return !bool(node.Banned)
}

// decommissionNode bans node and allows walle to take the role after some period.
func (p *TaskProcessor) decommissionNode(ctx context.Context, r *models.Node) {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if !p.needBanNode(ctx, r) {
		if !r.Banned { // Node was banned by someone else (not this service) or storage was not updated.
			banTime := node.LastSeenTime
			if time.Time(r.UnbanTime).After(time.Time(banTime)) {
				// Node was unbanned (e.g. due to LVC) after last_seen_time.
				banTime = r.UnbanTime
			}
			r.Ban(banTime)
			p.tryUpdateTaskInStorage(ctx, task)
		}

		if p.checkNodeOffline(node) {
			p.l.Info("allowing walle to take inactive node (banned and offline)", p.nodeLogFields(task, r)...)
			p.allowNode(ctx, node, r)
		} else {
			p.l.Info("not allowing walle to take banned node yet",
				p.nodeLogFields(task, r,
					log.String("node_state", node.State),
					log.Time("last_seen_time", time.Time(node.LastSeenTime)),
					log.Duration("offline_for", time.Since(time.Time(node.LastSeenTime))),
					log.Duration("offline_node_retention_period", p.Conf().OfflineNodeRetentionPeriod),
				)...)
		}
		return
	}

	storedChunkCount := node.Statistics.TotalStoredChunkCount
	nodeWithoutChunks := storedChunkCount == 0 && node.State == ytsys.NodeStateOnline
	nodeOffline := p.checkNodeOffline(node)

	if !p.nodeBanLimiter.Allow() &&
		((!task.IsGroupTask && !nodeWithoutChunks) || (task.IsGroupTask && task.TaskGroup != p.lastBannedNodeGroup)) {
		p.l.Info("can not ban node as another one was banned recently", p.nodeLogFields(task, r)...)
		return
	}

	chunkIntegrityIntact := p.chunkIntegrity.Check(p.conf.MaxURC)
	unrecoverableDataSafe := p.chunkIntegrity.CheckUnrecoverable() &&
		(chunkIntegrityIntact || !chunkIntegrityIntact && storedChunkCount <= p.conf.IgnorePMCThreshold)

	if task.IsGroupTask && !task.IsUrgent() && !nodeOffline && !nodeWithoutChunks {
		p.l.Info("will not ban node as maintenance is far ahead",
			p.nodeLogFields(task, r, log.String("indicators", p.chunkIntegrity.String()),
				log.Int64("total_stored_chunks", storedChunkCount),
				log.Time("maintenance_start_time", task.ScenarioInfo.MaintenanceStart()),
			)...)
		return
	}

	if !task.IsGroupTask && !nodeWithoutChunks && !unrecoverableDataSafe {
		p.l.Info("can not ban node as unrecoverable data might be in danger",
			p.nodeLogFields(task, r, log.String("indicators", p.chunkIntegrity.String()),
				log.Int64("total_stored_chunks", storedChunkCount),
				log.Int64("ignore_pmc_threshold", p.conf.IgnorePMCThreshold))...)
		return
	}

	p.l.Info("banning node", p.nodeLogFields(task, r)...)
	if err := banNode(ctx, p.dc, node, p.conf.UseMaintenanceAPI, p.makeBanMessage(task)); err != nil {
		p.l.Error("error banning node", p.nodeLogFields(task, r, log.Error(err))...)
		p.failedBans.Inc()
		return
	}
	p.l.Info("node banned", p.nodeLogFields(task, r)...)

	p.nodeBanLimiter.LastBanTime = time.Now()
	if task.IsGroupTask {
		p.lastBannedNodeGroup = task.TaskGroup
	}

	r.BanNow()
	p.l.Info("banning node in storage", p.nodeLogFields(task, r)...)
	p.tryUpdateTaskInStorage(ctx, task)

	if nodeWithoutChunks {
		p.l.Info("allowing walle to take node with no data", p.nodeLogFields(task, r)...)
		p.allowNode(ctx, node, r)
		return
	}

	if r.State != models.NodeStateDecommissioned {
		r.SetDecommissioned()
		p.l.Info("node is decommissioned; checking that LVC is 0", p.nodeLogFields(task, r)...)
		p.tryUpdateTaskInStorage(ctx, task)
	}
}

func (p *TaskProcessor) resolveNodeComponent(t *models.Task, r *models.Node) (*ytsys.Node, bool) {
	c, ok := p.cluster.GetComponent(r.Path)
	if !ok {
		p.l.Error("unable to find node cluster component", p.nodeLogFields(t, r)...)
		return nil, false
	}
	return c.(*ytsys.Node), true
}

// checkNodeOffline checks the following condition:
//
// /node/@state == 'offline' and (now - //node/@last_seen_time > offline_node_retention_period).
func (p *TaskProcessor) checkNodeOffline(node *ytsys.Node) bool {
	offlinePeriod := time.Since(time.Time(node.LastSeenTime))
	return node.State == ytsys.NodeStateOffline && offlinePeriod > p.Conf().OfflineNodeRetentionPeriod
}

func (p *TaskProcessor) checkTabletCellsDecommissioned(ctx context.Context, r *models.Node) bool {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	for _, s := range node.TabletSlots {
		if s.State != ytsys.TabletSlotStateNone {
			p.l.Info("some tablet slots are not free -> continue waiting",
				p.nodeLogFields(task, r, log.String("cell_id", s.CellID.String()))...)
			return false
		}
	}

	p.l.Info("all node tablet slots are free", p.nodeLogFields(task, r)...)
	return true
}

func (p *TaskProcessor) checkChunksDecommissioned(ctx context.Context, r *models.Node) bool {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if r.TotalStoredChunks != node.Statistics.TotalStoredChunkCount {
		r.UpdateTotalStoredChunks(node.Statistics.TotalStoredChunkCount)
		p.l.Info("updating node's total stored chunk count",
			p.nodeLogFields(task, r, log.Int64("total_stored_chunk_count", r.TotalStoredChunks))...)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	if node.Statistics.TotalStoredChunkCount == 0 {
		p.l.Info("all chunks decommissioned", p.nodeLogFields(task, r)...)
		return true
	}

	decommissionPeriod := time.Since(time.Time(r.MarkedDecommissionedTime))
	if decommissionPeriod > p.conf.UnconfirmedChunkCheckTimeout &&
		node.Statistics.TotalStoredChunkCount <= p.conf.UnconfirmedChunkThreshold {
		chunks, err := p.dc.GetNodeChunks(ctx, node.Addr, &ytsys.GetNodeChunksOption{All: true})
		if err != nil {
			p.l.Error("error querying node chunks", p.nodeLogFields(task, r, log.Error(err))...)
			return false
		}
		allUnconfirmed := true
		for _, c := range chunks {
			if c.Confirmed {
				allUnconfirmed = false
				break
			}
		}
		if allUnconfirmed {
			p.l.Info("node has small number of unconfirmed chunks after a long period of time",
				p.nodeLogFields(task, r,
					log.Duration("decommission_period", decommissionPeriod),
					log.Int64("total_stored_chunk_count", node.Statistics.TotalStoredChunkCount))...)
			return true
		}
	}

	p.l.Info("waiting for chunk decommission", p.nodeLogFields(task, r,
		log.Int64("total_stored_chunk_count", node.Statistics.TotalStoredChunkCount))...)

	return false
}

func (p *TaskProcessor) checkWriteSessionsDisabled(ctx context.Context, r *models.Node, limitTime bool) bool {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	sessionCount := node.Statistics.TotalSessionCount
	disabledFor := time.Since(time.Time(r.WriteSessionsDisableTime))
	timeout := p.getDisabledWriteSessionsWaitTimeout(task)
	if sessionCount > 0 && (!limitTime || limitTime && disabledFor <= timeout) {
		p.l.Info("waiting for disabled write sessions",
			p.nodeLogFields(task, r,
				log.Int64("total_session_count", sessionCount),
				log.Duration("disabled_for", disabledFor))...)
		return false
	}

	return true
}

func (p *TaskProcessor) getDisabledWriteSessionsWaitTimeout(task *models.Task) time.Duration {
	if IsNOCTask(task) {
		return p.conf.NOCTaskDisabledWriteSessionsWaitTimeout
	}
	return p.conf.DisabledWriteSessionsWaitTimeout
}

func (p *TaskProcessor) checkSchedulerJobsDisabled(ctx context.Context, r *models.Node, limitTime bool) bool {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	slots := node.ResourceUsage.UserSlots
	disabledFor := time.Since(time.Time(r.SchedulerJobsDisableTime))
	timeout := p.getDisabledSchedulerJobsWaitTimeout(task)
	if slots > 0 && (!limitTime || limitTime && disabledFor <= timeout) {
		p.l.Info("waiting for disabled scheduler jobs",
			p.nodeLogFields(task, r,
				log.Int64("user_slots", slots),
				log.Duration("disabled_for", disabledFor))...)
		return false
	}

	return true
}

func (p *TaskProcessor) getDisabledSchedulerJobsWaitTimeout(task *models.Task) time.Duration {
	if IsNOCTask(task) {
		return p.conf.NOCTaskDisabledSchedulerJobsWaitTimeout
	}
	return p.conf.DisabledSchedulerJobsWaitTimeout
}

func (p *TaskProcessor) makeDecommissionMessage(task *models.Task) string {
	decommissionTime := time.Now().UTC().Format(banMessageTimeLayout)
	return fmt.Sprintf("Decommissioned by %s at %s; wall-e task_id: %s;",
		models.DecisionMakerCMS, decommissionTime, task.ID)
}

func (p *TaskProcessor) makeBanMessage(task *models.Task) string {
	banTime := time.Now().UTC().Format(banMessageTimeLayout)
	return fmt.Sprintf("Banned by %s at %s; wall-e task_id: %s;",
		models.DecisionMakerCMS, banTime, task.ID)
}

func (p *TaskProcessor) allowNode(ctx context.Context, n *ytsys.Node, r *models.Node) {
	task := ctx.Value(taskKey).(*models.Task)

	p.l.Info("executing pre allow hook", p.nodeLogFields(task, r)...)
	if err := p.preNodeAllowHook(ctx, task, n, r); err != nil {
		p.l.Info("pre allow hook execution failed", p.nodeLogFields(task, r, log.Error(err))...)
		return
	}
	p.l.Info("executed pre allow hook", p.nodeLogFields(task, r)...)

	p.l.Info("allowing walle to take node", p.nodeLogFields(task, r)...)
	r.AllowWalle()
	p.tryUpdateTaskInStorage(ctx, task)
}

// nodeLogFields creates a slice with task and node fields to log.
func (p *TaskProcessor) nodeLogFields(t *models.Task, n *models.Node, extra ...log.Field) []log.Field {
	fields := []log.Field{
		log.String("task_id", string(t.ID)),
		log.String("host", n.Host),
		log.String("addr", n.Addr.String()),
		log.Bool("group_task", t.IsGroupTask),
		log.String("group_id", t.MaintenanceInfo.NodeSetID),
	}
	fields = append(fields, extra...)
	return fields
}

func (p *TaskProcessor) unbanNodesBecauseOfLVC(ctx context.Context) error {
	tasks, err := p.storage.GetAll(ctx)
	if err != nil {
		return err
	}

	var firstError error
	var unbanned bool

	for _, t := range tasks {
		if t.WalleStatus == walle.StatusOK {
			continue
		}

		for _, h := range t.HostStates {
			for _, r := range h.Roles {
				if r.Type != ytsys.RoleNode {
					continue
				}

				n := r.Role.(*models.Node)
				if time.Since(time.Time(n.BanTime)) > p.conf.LVCUnbanWindow {
					continue
				}

				p.l.Info("unbanning node", p.nodeLogFields(t, n)...)
				err := p.unbanNode(ctx, t, n)
				if err != nil && firstError == nil {
					firstError = err
				} else if err == nil {
					unbanned = true
				}
			}
		}
	}

	if unbanned {
		p.nodeBanLimiter.Unbanned()
	}

	return firstError
}

func (p *TaskProcessor) unbanNode(ctx context.Context, t *models.Task, r *models.Node) error {
	node, ok := p.resolveNodeComponent(t, r)
	if !ok {
		return nil
	}

	if !p.conf.UseMaintenanceAPI && strings.Contains(node.BanMessage, permanentBanSubstr) {
		p.l.Info("can not unban node with %s in ban message",
			p.nodeLogFields(t, r, log.String("ban_message", node.BanMessage))...)
		return nil
	}

	bannedRecently := time.Time(r.BanTime).After(p.cluster.LastReloadTime())
	needUnban := p.conf.UseMaintenanceAPI && r.Banned ||
		bool(node.Banned) || !bool(node.Banned) && bannedRecently
	// Node must be unbanned in the following cases:
	// 1. Cluster state is fresh and node is banned.
	// 2. Cluster state is stale and thus ban status is stale.
	// 3. UseMaintenanceAPI is true and node is banned by robot-yt-cms.
	if needUnban {
		p.l.Info("unbanning node", p.nodeLogFields(t, r)...)
		if err := unbanNode(ctx, p.dc, node, p.conf.UseMaintenanceAPI); err != nil {
			p.l.Error("error unbanning node", p.nodeLogFields(t, r)...)
			p.failedUnbans.Inc()
			return err
		}
		p.l.Info("node unbanned", p.nodeLogFields(t, r)...)
	}

	if r.Banned {
		p.l.Info("unbanning node in storage", p.nodeLogFields(t, r)...)
		r.Unban()
		p.tryUpdateTaskInStorage(ctx, t)
	}

	return nil
}

func (p *TaskProcessor) activateNode(ctx context.Context, r *models.Node) {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if err := p.unbanNode(ctx, task, r); err != nil {
		return
	}

	if err := p.enableSchedulerJobs(ctx, r); err != nil {
		return
	}

	if err := p.enableWriteSessions(ctx, r); err != nil {
		return
	}

	if err := p.dropRemovalSlotsOverride(ctx, r); err != nil {
		return
	}

	if err := p.finishChunkDecommission(ctx, r); err != nil {
		return
	}

	if err := p.finishNodeMaintenance(ctx, task, node, r); err != nil {
		return
	}

	r.SetFinished()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) needDecommission(ctx context.Context, r *models.Node) bool {
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if p.conf.UseMaintenanceAPI {
		return !containCMSMaintenanceRequest(node.MaintenanceRequests, yt.MaintenanceTypeDecommission) &&
			!r.DecommissionInProgress
	}

	return !bool(node.Decommissioned)
}

// startChunkDecommission stats node decommission if it isn't already decommissioned.
func (p *TaskProcessor) startChunkDecommission(ctx context.Context, r *models.Node) error {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	updateInStorage := func() {
		r.MarkDecommissioned(node.Statistics.TotalStoredChunkCount)
		p.l.Info("marking node decommissioned", p.nodeLogFields(task, r)...)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	if !p.needDecommission(ctx, r) {
		if !r.DecommissionInProgress {
			updateInStorage()
		}
		return nil
	}

	p.l.Info("marking node decommissioned", p.nodeLogFields(task, r)...)
	if err := markNodeDecommissioned(
		ctx, p.dc, node,
		p.conf.UseMaintenanceAPI,
		p.makeDecommissionMessage(task),
	); err != nil {
		p.l.Error("error marking node decommissioned", p.nodeLogFields(task, r, log.Error(err))...)
		return err
	}

	updateInStorage()
	p.cluster.OnNodeDecommission(node)

	return nil
}

// finishChunkDecommission stops node decommission.
func (p *TaskProcessor) finishChunkDecommission(ctx context.Context, r *models.Node) error {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	decommissionedRecently := time.Time(r.MarkedDecommissionedTime).After(p.cluster.LastReloadTime())
	needEnable := p.conf.UseMaintenanceAPI && r.Decommissioned() ||
		bool(node.Decommissioned) || !bool(node.Decommissioned) && decommissionedRecently
	// @decommission attribute must be unset in the following cases:
	// 1. Cluster state is fresh and node is decommissioned.
	// 2. Cluster state is stale and thus decommission status is stale.
	// 3. UseMaintenanceAPI is true and node is decommissioned by robot-yt-cms.
	if needEnable {
		p.l.Info("unmarking node decommissioned", p.nodeLogFields(task, r)...)
		if err := unmarkNodeDecommissioned(ctx, p.dc, p.conf.UseMaintenanceAPI); err != nil {
			p.l.Error("error unmarking node decommissioned", p.nodeLogFields(task, r, log.Error(err))...)
			return err
		}
	}

	if r.DecommissionInProgress {
		p.l.Info("unmarking node decommissioned", p.nodeLogFields(task, r)...)
		r.UnmarkDecommissioned()
	}

	return nil
}

func (p *TaskProcessor) overrideRemovalSlots(ctx context.Context, r *models.Node) error {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	var currentSlots *int
	if o := node.ResourceLimitsOverrides; o != nil && o.RemovalSlots != 0 {
		slots := o.RemovalSlots
		currentSlots = &slots
	}

	newSlots := safeDecommissionRemovalSlots
	if currentSlots == nil || newSlots != *currentSlots {
		if err := p.dc.OverrideRemovalSlots(ctx, node.Addr, newSlots); err != nil {
			p.l.Error("error overriding removal slots",
				p.nodeLogFields(task, r, log.Int("value", newSlots), log.Error(err))...)
			return err
		}

		r.OverrideRemovalSlots(&newSlots, currentSlots)
		p.l.Info("overriding removal slots", p.nodeLogFields(task, r, log.Int("new_slots", newSlots))...)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	return nil
}

func (p *TaskProcessor) dropRemovalSlotsOverride(ctx context.Context, r *models.Node) error {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if r.RemovalSlotsOverridden {
		if slots := r.PrevRemovalSlotsOverride; slots != nil {
			if err := p.dc.OverrideRemovalSlots(ctx, node.Addr, *slots); err != nil {
				p.l.Error("error returning removal slots override",
					p.nodeLogFields(task, r, log.Int("value", *slots), log.Error(err))...)
				return err
			}
		} else {
			if err := p.dc.DropRemovalSlotsOverride(ctx, node.Addr); err != nil {
				p.l.Error("error dropping removal slots override",
					p.nodeLogFields(task, r, log.Error(err))...)
				return err
			}
		}

		r.DropRemovalSlotsOverride()
		p.l.Info("dropping removal slots override", p.nodeLogFields(task, r)...)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	return nil
}

func (p *TaskProcessor) needDisableSchedulerJobs(ctx context.Context, r *models.Node) bool {
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if p.conf.UseMaintenanceAPI {
		return !containCMSMaintenanceRequest(node.MaintenanceRequests, yt.MaintenanceTypeDisableSchedulerJobs) &&
			!r.SchedulerJobsDisabled
	}

	return !bool(node.DisableSchedulerJobs)
}

// disableSchedulerJobs disables scheduler jobs if they aren't already disabled.
func (p *TaskProcessor) disableSchedulerJobs(ctx context.Context, r *models.Node) error {
	task := ctx.Value(taskKey).(*models.Task)

	updateInStorage := func() {
		r.DisableSchedulerJobs()
		p.l.Info("disabling scheduler jobs in storage", p.nodeLogFields(task, r)...)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	if !p.needDisableSchedulerJobs(ctx, r) {
		if !r.SchedulerJobsDisabled {
			updateInStorage()
		}
		return nil
	}

	p.l.Info("disabling scheduler jobs", p.nodeLogFields(task, r)...)
	if err := disableSchedulerJobs(ctx, p.dc, r, p.conf.UseMaintenanceAPI); err != nil {
		p.l.Error("error disabling scheduler jobs", p.nodeLogFields(task, r, log.Error(err))...)
		return err
	}

	updateInStorage()

	return nil
}

// enableSchedulerJobs enables scheduler jobs if the aren't already enabled.
func (p *TaskProcessor) enableSchedulerJobs(ctx context.Context, r *models.Node) error {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	disabledRecently := time.Time(r.SchedulerJobsDisableTime).After(p.cluster.LastReloadTime())
	needEnable := p.conf.UseMaintenanceAPI && r.SchedulerJobsDisabled ||
		bool(node.DisableSchedulerJobs) || !bool(node.DisableSchedulerJobs) && disabledRecently
	// @disable_scheduler_jobs must be unset in the following cases:
	// 1. Cluster state is fresh and jobs are disabled.
	// 2. Cluster state is stale and thus attribute value is stale.
	// 3. UseMaintenanceAPI is true and jobs are disabled by robot-yt-cms.
	if needEnable {
		p.l.Info("enabling scheduler jobs", p.nodeLogFields(task, r)...)
		if err := enableSchedulerJobs(ctx, p.dc, r, p.conf.UseMaintenanceAPI); err != nil {
			p.l.Error("error enabling scheduler jobs", p.nodeLogFields(task, r, log.Error(err))...)
			return err
		}
	}

	if r.SchedulerJobsDisabled {
		p.l.Info("enabling scheduler jobs in storage", p.nodeLogFields(task, r)...)
		r.EnableSchedulerJobs()
	}

	return nil
}

func (p *TaskProcessor) needDisableWriteSessions(ctx context.Context, r *models.Node) bool {
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if p.conf.UseMaintenanceAPI {
		return !containCMSMaintenanceRequest(node.MaintenanceRequests, yt.MaintenanceTypeDisableWriteSessions) &&
			!r.WriteSessionsDisabled
	}

	return !bool(node.DisableWriteSessions)
}

// disableWriteSessions disables write sessions if the aren't already disabled.
func (p *TaskProcessor) disableWriteSessions(ctx context.Context, r *models.Node) error {
	task := ctx.Value(taskKey).(*models.Task)

	updateInStorage := func() {
		r.DisableWriteSessions()
		p.l.Info("disabling write sessions in storage", p.nodeLogFields(task, r)...)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	if !p.needDisableWriteSessions(ctx, r) {
		if !r.SchedulerJobsDisabled {
			updateInStorage()
		}
		return nil
	}

	p.l.Info("disabling write sessions", p.nodeLogFields(task, r)...)
	if err := disableWriteSessions(ctx, p.dc, r, p.conf.UseMaintenanceAPI); err != nil {
		p.l.Error("error disabling write sessions", p.nodeLogFields(task, r, log.Error(err))...)
		return err
	}

	updateInStorage()

	return nil
}

// enableWriteSessions enables write sessions if the aren't already enabled.
func (p *TaskProcessor) enableWriteSessions(ctx context.Context, r *models.Node) error {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	disabledRecently := time.Time(r.WriteSessionsDisableTime).After(p.cluster.LastReloadTime())
	needEnable := p.conf.UseMaintenanceAPI && r.SchedulerJobsDisabled ||
		bool(node.DisableWriteSessions) || !bool(node.DisableWriteSessions) && disabledRecently
	// @disable_write_sessions must be unset in the following cases:
	// 1. Cluster state is fresh and write sessions are disabled.
	// 2. Cluster state is stale and thus attribute value is stale.
	// 3. UseMaintenanceAPI is true and sessions are disabled by robot-yt-cms.
	if needEnable {
		p.l.Info("enabling write sessions", p.nodeLogFields(task, r)...)
		if err := enableWriteSessions(ctx, p.dc, r, p.conf.UseMaintenanceAPI); err != nil {
			p.l.Error("error enabling write sessions", p.nodeLogFields(task, r, log.Error(err))...)
			return err
		}
	}

	if r.WriteSessionsDisabled {
		p.l.Info("enabling write sessions in storage", p.nodeLogFields(task, r)...)
		r.EnableWriteSessions()
	}

	return nil
}

func (p *TaskProcessor) checkResourceLimits(ctx context.Context, r *models.Node) bool {
	task := ctx.Value(taskKey).(*models.Task)

	tree, err := p.cluster.GetNodePoolTree(*r.Addr)
	if err != nil {
		p.l.Error("unable to get pool tree", p.nodeLogFields(task, r, log.Error(err))...)
		return false
	}
	if tree == nil {
		p.l.Info("node does not belong to any pool tree", p.nodeLogFields(task, r)...)
		return true
	}

	if ytsys.IsGPUPoolTree(tree) {
		p.l.Info("node belongs to gpu pool tree -> checking gpu resource limits",
			p.nodeLogFields(task, r, log.String("pool_tree", tree.Name))...)
		return p.checkGPULimits(ctx, r, tree)
	}

	p.l.Info("checking cpu resource limits", p.nodeLogFields(task, r, log.String("pool_tree", tree.Name))...)
	return p.checkCPULimits(ctx, r, tree)
}

func (p *TaskProcessor) checkGPULimits(ctx context.Context, r *models.Node, tree *ytsys.PoolTree) bool {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if node.ResourceLimits == nil {
		p.l.Info("node is missing resource limits -> waiting", p.nodeLogFields(task, r)...)
		return false
	}

	if node.ResourceLimits.GPU == 0.0 || node.ResourceLimits.UserSlots == 0 {
		p.l.Info("node has no gpu resources", p.nodeLogFields(task, r)...)
		return true
	}

	available := tree.AvailableResources.GPU - p.cluster.GetDecommissionStats().TotalGPU[tree.Name]
	guaranteed := tree.SumNodeGPUResources()
	reserve := available - node.ResourceLimits.GPU - guaranteed

	// Use reserve from fair_share_info if available.
	if r := tree.ResourceReserves; r != nil {
		reserve = r.GPU - p.cluster.GetDecommissionStats().TotalGPU[tree.Name]
	}

	requiredReserve := p.conf.GPUReserve

	p.l.Info("calculated gpu resources",
		p.nodeLogFields(task, r, log.Float64("available_gpu", available),
			log.Float64("guaranteed_gpu", guaranteed),
			log.Float64("node_gpu", node.ResourceLimits.GPU),
			log.Float64("required_reserve", requiredReserve),
			log.Float64("actual_reserve", reserve))...)

	if reserve < requiredReserve {
		p.l.Info("node can not be given as gpu limits will be broken")
		return false
	}

	return true
}

func (p *TaskProcessor) checkCPULimits(ctx context.Context, r *models.Node, tree *ytsys.PoolTree) bool {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if node.ResourceLimits == nil {
		p.l.Info("node is missing resource limits -> waiting", p.nodeLogFields(task, r)...)
		return false
	}

	if node.ResourceLimits.CPU == 0.0 || node.ResourceLimits.UserSlots == 0 {
		p.l.Info("node has no cpu resources", p.nodeLogFields(task, r)...)
		return true
	}

	if p.conf.UseReservePool && r.HasFlavor(ytsys.FlavorExec) {
		return p.checkReservePoolResources(ctx, r)
	}

	available := tree.AvailableResources.CPU - p.cluster.GetDecommissionStats().CPU[tree.Name]
	guaranteed := tree.SumNodeCPUResources()
	reserve := available - node.ResourceLimits.CPU - guaranteed

	// Use reserve from fair_share_info if available.
	if r := tree.ResourceReserves; r != nil {
		reserve = r.CPU - p.cluster.GetDecommissionStats().CPU[tree.Name]
	}

	relativeReserve := available * p.conf.CPUReservePercent / 100.0
	absoluteReserve := p.conf.CPUReserve

	// requiredReserve is a maximum of absolute and relative reserves.
	requiredReserve := relativeReserve
	if absoluteReserve > requiredReserve {
		requiredReserve = absoluteReserve
	}

	p.l.Info("calculated cpu resources",
		p.nodeLogFields(task, r, log.Float64("available_cpu", available),
			log.Float64("guaranteed_cpu", guaranteed),
			log.Float64("node_cpu", node.ResourceLimits.CPU),
			log.Float64("required_reserve_absolute", absoluteReserve),
			log.Float64("required_reserve_relative", relativeReserve),
			log.Float64("required_reserve_percent", p.conf.CPUReservePercent),
			log.Float64("actual_reserve", reserve))...)

	if reserve < requiredReserve {
		p.l.Info("node can not be given as cpu limits will be broken", p.nodeLogFields(task, r)...)
		return false
	}

	return true
}

func (p *TaskProcessor) checkReservePoolResources(ctx context.Context, r *models.Node) bool {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	if !p.reservePool.Allow(ctx, node) {
		p.l.Info(
			"strong_guarantee_resources/cpu of reserve pool less than node limit",
			p.nodeLogFields(
				task,
				r,
				log.Any("strong_guarantee_resources/cpu", p.reservePool.GetCPUGuarantee()),
				log.Any("node cpu limit", node.ResourceLimits.CPU),
			)...,
		)
		return false
	}

	if node.ResourceLimits.CPU == 0.0 {
		return true
	}
	r.ReservedCPU = node.ResourceLimits.CPU
	if err := p.updateTaskInStorage(ctx, task); err != nil {
		return false
	}

	p.reservePool.AddNode(ctx, r)

	return true
}

func (p *TaskProcessor) checkTabletCellGuarantees(ctx context.Context, r *models.Node) bool {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	cellBundles, err := p.cluster.GetTabletCellBundles(node)
	if err != nil {
		p.l.Error("unable to resolve tablet cell bundles", p.nodeLogFields(task, r, log.Error(err))...)
		return false
	}
	tabletCommonNodes, err := p.cluster.GetTabletCommonNodeCount()
	if err != nil {
		p.l.Error("unable to get tablet common node count", p.nodeLogFields(task, r, log.Error(err))...)
		return false
	}

	if len(cellBundles.Bundles) == 0 {
		p.l.Info("node has no active cells", p.nodeLogFields(task, r)...)

		reserve := tabletCommonNodes
		if bool(!node.Decommissioned) || !r.DecommissionInProgress {
			reserve--
		}

		tabletCommonLimitApplicable := node.HasTag(ytsys.TabletCommonTag) && p.conf.TabletCommonNodeReserve > 0
		if tabletCommonLimitApplicable && reserve < p.conf.TabletCommonNodeReserve {
			p.l.Info("can not release node as there are not enough nodes tagged with 'tablet_common'",
				p.nodeLogFields(task, r,
					log.Int("tablet_common_node_count", reserve),
					log.Int("required_reserve", p.conf.TabletCommonNodeReserve))...)
			return false
		}

		return true
	}

	freeSlotsUsed := 0
	// Each bundle is iterated n (=number of bundle slots) times.
	for _, b := range cellBundles.Bundles {
		if b.Production != nil && !*b.Production {
			p.l.Info("skipping bundle checks for bundle with @production=%false",
				p.nodeLogFields(task, r, log.String("bundle", b.Name))...)
			continue
		}

		disabled := cellBundles.BalancerDisabled || !bool(*b.BalancerEnabled)
		if disabled {
			p.l.Info("bundle balancer is disabled", p.nodeLogFields(task, r, log.String("bundle", b.Name))...)
			p.logBundleStats(ctx, r, b)

			freeSlots := p.getAvailableSlots(node, r, b) - freeSlotsUsed
			if freeSlots <= p.conf.BundleSlotReserve {
				p.l.Info("can not release node as there are not enough bundle slots",
					p.nodeLogFields(task, r, log.String("bundle", b.Name))...)
				return false
			}

			freeSlotsUsed++
		} else {
			p.l.Info("bundle balancer is enabled",
				p.nodeLogFields(task, r, log.String("bundle", b.Name),
					log.Int("tablet_common_node_count", tabletCommonNodes),
					log.Int("required_reserve", p.conf.TabletCommonNodeReserve))...)

			tabletCommonLimitApplicable := node.HasTag(ytsys.TabletCommonTag) && p.conf.TabletCommonNodeReserve > 0
			if tabletCommonLimitApplicable && tabletCommonNodes-1 < p.conf.TabletCommonNodeReserve {
				p.l.Info("can not release node as there are not enough nodes tagged with 'tablet_common'",
					p.nodeLogFields(task, r, log.String("bundle", b.Name))...)
				return false
			}
		}
	}

	return true
}

func (p *TaskProcessor) fillNodeFlavors(ctx context.Context, t *models.Task, n *ytsys.Node, r *models.Node) error {
	if len(n.Flavors) == 0 {
		return nil
	}
	if slices.Equal(r.Flavors, n.Flavors) {
		return nil
	}
	r.Flavors = n.Flavors
	return p.updateTaskInStorage(ctx, t)
}

func (p *TaskProcessor) startNodeMaintenance(ctx context.Context, t *models.Task, n *ytsys.Node, r *models.Node) error {
	if n.HasMaintenanceAttr() && r.InMaintenance {
		p.l.Info("node maintenance request is already started", p.nodeLogFields(t, r)...)
		return nil
	}

	p.l.Info("starting node maintenance request", p.nodeLogFields(t, r)...)

	req := r.MaintenanceRequest
	if req == nil {
		req = p.makeMaintenanceRequest(t)
	}

	if err := p.dc.SetMaintenance(ctx, n, req); err != nil {
		p.l.Error("error starting node maintenance request",
			p.nodeLogFields(t, r, log.Error(err))...)
		p.failedMaintenanceRequestUpdates.Inc()
		return err
	}

	p.l.Info("node maintenance request started", p.nodeLogFields(t, r)...)
	if r.MaintenanceRequest == nil {
		r.StartMaintenance(req)
		p.tryUpdateTaskInStorage(ctx, t)
	}

	return nil
}

func (p *TaskProcessor) finishNodeMaintenance(ctx context.Context, t *models.Task, n *ytsys.Node, r *models.Node) error {
	hasMaintenanceAttr, err := p.dc.HasMaintenanceAttr(ctx, n)
	if err != nil {
		p.l.Error("node maintenance status unknown", p.nodeLogFields(t, r, log.Error(err))...)
		return err
	}

	if hasMaintenanceAttr {
		p.l.Info("finishing node maintenance", p.nodeLogFields(t, r)...)
		if err := p.dc.UnsetMaintenance(ctx, n, r.MaintenanceRequest.GetID()); err != nil {
			p.l.Error("error finishing node maintenance",
				p.nodeLogFields(t, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return err
		}
		p.l.Info("node maintenance finished", p.nodeLogFields(t, r)...)
	}

	if r.InMaintenance {
		p.l.Info("finishing node maintenance", p.nodeLogFields(t, r)...)
		r.FinishMaintenance()
		p.tryUpdateTaskInStorage(ctx, t)
	}

	return nil
}

func (p *TaskProcessor) preNodeAllowHook(ctx context.Context, t *models.Task, n *ytsys.Node, r *models.Node) error {
	if n.HasTag(ytsys.GPUTag) && p.conf.EnableGPUTesting {
		return p.sendGPUToTesting(ctx, t, n, r)
	}
	return nil
}

// sendGPUToTesting removes gpu_prod tag.
func (p *TaskProcessor) sendGPUToTesting(ctx context.Context, t *models.Task, n *ytsys.Node, r *models.Node) error {
	if !n.HasTag(ytsys.GPUTag) || !n.HasTag(ytsys.GPUProdTag) {
		return nil
	}

	if !n.HasTag("gpu_tesla_a100") &&
		!n.HasTag("gpu_tesla_a100_80g") &&
		!n.HasTag("gpu_a800_80g") &&
		!n.HasTag("gpu_h100_80g") &&
		!n.HasTag("gpu_h100_80g_noib") {
		// Only specific models are tested.
		return nil
	}

	p.l.Info("removing gpu prod tag", p.nodeLogFields(t, r)...)
	err := p.dc.RemoveTag(ctx, n.GetCypressPath(), ytsys.GPUProdTag)
	if err != nil {
		p.l.Error("error removing gpu prod tag",
			p.nodeLogFields(t, r, log.Error(err))...)
	} else {
		p.l.Info("removed gpu prod tag", p.nodeLogFields(t, r)...)
	}

	return err
}

func (p *TaskProcessor) getAvailableSlots(node *ytsys.Node, r *models.Node, b *ytsys.TabletCellBundle) int {
	nodeSlots := b.GetNodeSlots(r.Addr)
	if nodeSlots == 0 && len(node.TabletSlots) > 0 {
		// Use upper bound for node slots in case if
		// node is already removed from //sys/tablet_cell_bundles/<BUNDLE>/@nodes
		// but still has //sys/cluster_nodes/<NODE>/@tablet_slots.
		nodeSlots = len(node.TabletSlots)
	}
	return b.GetFreeSlots() - nodeSlots - p.cluster.GetDecommissionStats().Slots[b.Name]
}

func (p *TaskProcessor) logBundleStats(ctx context.Context, r *models.Node, b *ytsys.TabletCellBundle) {
	task := ctx.Value(taskKey).(*models.Task)
	node := ctx.Value(nodeKey).(*ytsys.Node)

	freeSlots := p.getAvailableSlots(node, r, b)
	p.l.Info("bundle stats", p.nodeLogFields(task, r,
		log.String("bundle", b.Name),
		log.Int("free_slots_total", b.GetFreeSlots()),
		log.Int("node_slots_bundle", b.GetNodeSlots(r.Addr)),
		log.Int("node_slots_total", len(node.TabletSlots)),
		log.Int("recently_decommissioned_slots", p.cluster.GetDecommissionStats().Slots[b.Name]),
		log.Int("available_after_decommission", freeSlots),
		log.Int("required_slot_reserve", p.conf.BundleSlotReserve))...)

	const lowFreeSlotsThreshold = 20
	if freeSlots > lowFreeSlotsThreshold {
		return
	}

	for addr := range b.Slots {
		stats := b.GetNodeSlotStats(&addr)
		if stats[ytsys.TabletSlotStateNone] == 0 {
			// Node has no free slots.
			continue
		}

		p.l.Info("node slot stats",
			log.String("bundle", b.Name),
			log.String("addr", addr.String()),
			log.Any("stats", stats))
	}
}

// getOfflineNodes return non-online nodes.
func (p *TaskProcessor) getOfflineNodes() (ytsys.NodeMap, error) {
	nodes, err := p.cluster.GetNodes()
	if err != nil {
		return nil, err
	}

	if err := p.cluster.Err(); err != nil {
		return nil, xerrors.Errorf("last cluster update failed: %w", err)
	}

	offlineNodes := make(ytsys.NodeMap)
	for a, n := range nodes {
		if n.State != ytsys.NodeStateOnline {
			offlineNodes[a] = n
		}
	}

	return offlineNodes, nil
}

func (p *TaskProcessor) CheckPathMissing(ctx context.Context, path ypath.Path) bool {
	ok, err := p.dc.PathExists(ctx, path)
	if err != nil {
		return false
	}
	return !ok
}
