package cms

import (
	"context"
	"strings"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/ytsys"
)

func (p *TaskProcessor) processQueueAgent(ctx context.Context, r *models.QueueAgent) {
	task := ctx.Value(taskKey).(*models.Task)
	p.l.Info("processing queue agent", p.queueAgentLogFields(task, r)...)

	if task.DeletionRequested {
		p.l.Info("deletion requested -> activating queue agent", p.queueAgentLogFields(task, r)...)
		p.activateQueueAgent(ctx, r)
		return
	}

	switch r.State {
	case models.QueueAgentStateAccepted:
		p.processPendingQueueAgent(ctx, r)
	case models.QueueAgentStateProcessed:
	default:
		p.l.Error("unexpected queue agent state", log.String("state", string(r.State)))
	}
}

func (p *TaskProcessor) processPendingQueueAgent(ctx context.Context, r *models.QueueAgent) {
	task := ctx.Value(taskKey).(*models.Task)

	_, ok := p.resolveQueueAgent(task, r)
	if !ok {
		return
	}

	agents, err := p.cluster.GetQueueAgents()
	if err != nil {
		p.l.Error("unable to get queue agents", p.queueAgentLogFields(task, r, log.Error(err))...)
		return
	}

	if len(agents) <= 1 {
		p.l.Info("cannot allow queue agent as there are not that many left",
			p.queueAgentLogFields(task, r, log.Int("left_count", len(agents)))...)
		return
	}

	bannedCount := 0
	for _, a := range agents {
		if bool(a.Banned) {
			bannedCount++
		}
	}

	if float64(bannedCount+1) > p.conf.MaxBannedQueueAgents*float64(len(agents)) {
		p.l.Info("cannot allow queue agent as max allowed banned percent will be exceeded",
			p.queueAgentLogFields(task, r,
				log.Int("banned_queue_agents", bannedCount),
				log.Int("total_queue_agents", len(agents)),
				log.Float64("max_banned_queue_agents", p.conf.MaxBannedQueueAgents))...)
		return
	}

	p.l.Info("all other queue agents are alive -> proceeding to decommission",
		p.queueAgentLogFields(task, r)...)
	p.decommissionQueueAgent(ctx, r)
}

func (p *TaskProcessor) decommissionQueueAgent(ctx context.Context, r *models.QueueAgent) {
	task := ctx.Value(taskKey).(*models.Task)

	agent, ok := p.resolveQueueAgent(task, r)
	if !ok {
		p.l.Error("unable to find queue agent", p.queueAgentLogFields(task, r)...)
		return
	}
	var req *ytsys.MaintenanceRequest
	if !agent.InMaintenance {
		p.l.Info("starting queue agent maintenance", p.queueAgentLogFields(task, r)...)

		req = r.MaintenanceRequest
		if req == nil {
			req = p.makeMaintenanceRequest(task)
		}

		if err := p.dc.SetMaintenance(ctx, agent, req); err != nil {
			p.l.Error("error starting queue agent maintenance",
				p.queueAgentLogFields(task, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return
		}
	}
	if r.MaintenanceRequest == nil {
		p.l.Info("starting queue agent role maintenance", p.queueAgentLogFields(task, r)...)
		r.StartMaintenance(req)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	if !agent.Banned {
		p.l.Info("banning queue agent", p.queueAgentLogFields(task, r)...)
		if err := p.dc.Ban(ctx, agent, p.makeBanMessage(task)); err != nil {
			p.l.Error("error banning queue agent", p.queueAgentLogFields(task, r, log.Error(err))...)
			p.failedBans.Inc()
			return
		}
	}
	if !r.Banned {
		p.l.Info("banning queue agent role", p.queueAgentLogFields(task, r)...)
		r.Ban()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	p.l.Info("allowing walle to take queue agent)", p.queueAgentLogFields(task, r)...)
	r.AllowWalle()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) activateQueueAgent(ctx context.Context, r *models.QueueAgent) {
	task := ctx.Value(taskKey).(*models.Task)

	agent, ok := p.resolveQueueAgent(task, r)
	if !ok {
		p.l.Error("unable to find queue agent", p.queueAgentLogFields(task, r)...)
		return
	}

	permanentBan := false
	if strings.Contains(agent.BanMessage, permanentBanSubstr) {
		p.l.Info("can not unban queue agent with %s in ban message",
			p.queueAgentLogFields(task, r, log.String("ban_message", agent.BanMessage))...)
		permanentBan = true
	}

	bannedRecently := time.Time(r.BanTime).After(p.cluster.LastReloadTime())
	// Queue agent must be unbanned if ban message does not contain
	// permanentBanSubstr and one of the following cases is true:
	// 1. Cluster state is fresh and queue agent is banned.
	// 2. Cluster state is stale and thus ban status is stale.
	if !permanentBan && (bool(agent.Banned) || !bool(agent.Banned) && bannedRecently) {
		p.l.Info("unbanning queue agent", p.queueAgentLogFields(task, r)...)
		if err := p.dc.Unban(ctx, agent); err != nil {
			p.l.Error("error unbanning queue agent", p.queueAgentLogFields(task, r, log.Error(err))...)
			p.failedUnbans.Inc()
			return
		}
	}
	if r.Banned {
		p.l.Info("unbanning queue agent role", p.queueAgentLogFields(task, r)...)
		r.Unban()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	if agent.InMaintenance {
		p.l.Info("finishing queue agent maintenance", p.queueAgentLogFields(task, r)...)
		if err := p.dc.UnsetMaintenance(ctx, agent, r.MaintenanceRequest.GetID()); err != nil {
			p.l.Error("error finishing queue agent maintenance",
				p.queueAgentLogFields(task, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return
		}
	}
	if r.InMaintenance {
		p.l.Info("finishing queue agent role maintenance", p.queueAgentLogFields(task, r)...)
		r.FinishMaintenance()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	p.l.Info("finish processing queue agent", p.queueAgentLogFields(task, r)...)
	r.SetFinished()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) resolveQueueAgent(t *models.Task, r *models.QueueAgent) (*ytsys.QueueAgent, bool) {
	c, ok := p.cluster.GetComponent(r.Path)
	if !ok {
		p.l.Error("unable to resolve queue agent component", p.queueAgentLogFields(t, r)...)
		return nil, false
	}
	return c.(*ytsys.QueueAgent), true
}

// queueAgentLogFields creates a slice with task and queue agent fields to log.
func (p *TaskProcessor) queueAgentLogFields(t *models.Task, a *models.QueueAgent, extra ...log.Field) []log.Field {
	fields := []log.Field{
		log.String("task_id", string(t.ID)),
		log.String("host", a.Host),
		log.String("addr", a.Addr.String()),
		log.Bool("group_task", t.IsGroupTask),
		log.String("group_id", t.MaintenanceInfo.NodeSetID),
	}
	fields = append(fields, extra...)
	return fields
}
