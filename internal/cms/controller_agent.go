package cms

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/ytsys"
)

func (p *TaskProcessor) processControllerAgent(ctx context.Context, r *models.ControllerAgent) {
	task := ctx.Value(taskKey).(*models.Task)
	p.l.Info("processing controller agent", p.controllerAgentLogFields(task, r)...)

	if task.DeletionRequested {
		p.l.Info("deletion requested -> activating controller agent", p.controllerAgentLogFields(task, r)...)
		p.activateControllerAgent(ctx, r)
		return
	}

	switch r.State {
	case models.ControllerAgentStateAccepted:
		p.processPendingControllerAgent(ctx, r)
	case models.ControllerAgentStateProcessed:
	default:
		p.l.Error("unexpected controller agent state", log.String("state", string(r.State)))
	}
}

func (p *TaskProcessor) processPendingControllerAgent(ctx context.Context, r *models.ControllerAgent) {
	task := ctx.Value(taskKey).(*models.Task)

	agent, ok := p.resolveControllerAgent(task, r)
	if !ok {
		return
	}

	if p.taskCache.ControllerAgents.CountProcessed() != 0 {
		p.l.Info("cannot allow controller agent as another one is in process",
			p.controllerAgentLogFields(task, r)...)
		return
	}

	agents, err := p.cluster.GetControllerAgents()
	if err != nil {
		p.l.Error("unable to get controller agents", p.controllerAgentLogFields(task, r, log.Error(err))...)
		return
	}

	if len(agents) <= 1 {
		p.l.Info("cannot allow controller agent as there are not that many left",
			p.controllerAgentLogFields(task, r, log.Int("left_count", len(agents)))...)
		return
	}

	for _, a := range agents {
		if a.Addr.String() == agent.Addr.String() {
			continue
		}
		if err := a.ConnectionRequestError; err != nil {
			p.l.Info("cannot allow controller agent as connection state of another one is unknown",
				p.controllerAgentLogFields(task, r, log.Error(err))...)
			return
		}
		if !a.Connected {
			p.l.Info("cannot allow controller agent as another one is disconnected",
				p.controllerAgentLogFields(task, r, log.String("disconnected", a.Addr.String()))...)
			return
		}
	}

	p.l.Info("all other controller agents are alive -> proceeding to decommission",
		p.controllerAgentLogFields(task, r)...)
	p.decommissionControllerAgent(ctx, r)
}

func (p *TaskProcessor) decommissionControllerAgent(ctx context.Context, r *models.ControllerAgent) {
	task := ctx.Value(taskKey).(*models.Task)

	agent, ok := p.resolveControllerAgent(task, r)
	if !ok {
		p.l.Error("unable to find controller agent", p.controllerAgentLogFields(task, r)...)
		return
	}

	if !agent.InMaintenance {
		p.l.Info("starting controller agent maintenance", p.controllerAgentLogFields(task, r)...)

		req := r.MaintenanceRequest
		if req == nil {
			req = p.makeMaintenanceRequest(task)
		}

		if err := p.dc.SetMaintenance(ctx, agent, req); err != nil {
			p.l.Error("error starting controller agent maintenance",
				p.controllerAgentLogFields(task, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return
		}

		p.l.Info("controller agent maintenance started", p.controllerAgentLogFields(task, r)...)
		r.StartMaintenance(req)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	p.l.Info("allowing walle to take controller agent)", p.controllerAgentLogFields(task, r)...)
	r.AllowWalle()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) activateControllerAgent(ctx context.Context, r *models.ControllerAgent) {
	task := ctx.Value(taskKey).(*models.Task)

	agent, ok := p.resolveControllerAgent(task, r)
	if !ok {
		p.l.Error("unable to find controller agent", p.controllerAgentLogFields(task, r)...)
		return
	}

	if agent.InMaintenance {
		p.l.Info("finishing controller agent maintenance", p.controllerAgentLogFields(task, r)...)
		if err := p.dc.UnsetMaintenance(ctx, agent, r.MaintenanceRequest.GetID()); err != nil {
			p.l.Error("error finishing controller agent maintenance",
				p.controllerAgentLogFields(task, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return
		}
		p.l.Info("controller agent maintenance finished", p.controllerAgentLogFields(task, r)...)
	}

	if r.InMaintenance {
		p.l.Info("finishing controller agent maintenance", p.controllerAgentLogFields(task, r)...)
		r.FinishMaintenance()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	p.l.Info("finish processing controller agent", p.controllerAgentLogFields(task, r)...)
	r.SetFinished()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) resolveControllerAgent(t *models.Task, r *models.ControllerAgent) (*ytsys.ControllerAgent, bool) {
	c, ok := p.cluster.GetComponent(r.Path)
	if !ok {
		p.l.Error("unable to resolve controller agent component", p.controllerAgentLogFields(t, r)...)
		return nil, false
	}
	return c.(*ytsys.ControllerAgent), true
}

// controllerAgentLogFields creates a slice with task and controller agent fields to log.
func (p *TaskProcessor) controllerAgentLogFields(t *models.Task, n *models.ControllerAgent, extra ...log.Field) []log.Field {
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
