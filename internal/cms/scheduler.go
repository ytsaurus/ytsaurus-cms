package cms

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/ytsys"
)

func (p *TaskProcessor) processScheduler(ctx context.Context, r *models.Scheduler) {
	task := ctx.Value(taskKey).(*models.Task)
	p.l.Info("processing scheduler", p.schedulerLogFields(task, r)...)

	if task.DeletionRequested {
		p.l.Info("deletion requested -> activating scheduler", p.schedulerLogFields(task, r)...)
		p.activateScheduler(ctx, r)
		return
	}

	switch r.State {
	case models.SchedulerStateAccepted:
		p.processPendingScheduler(ctx, r)
	case models.SchedulerStateProcessed:
	default:
		p.l.Error("unexpected scheduler state", log.String("state", string(r.State)))
	}
}

func (p *TaskProcessor) processPendingScheduler(ctx context.Context, r *models.Scheduler) {
	task := ctx.Value(taskKey).(*models.Task)

	scheduler, ok := p.resolveScheduler(task, r)
	if !ok {
		return
	}

	if p.taskCache.Schedulers.CountProcessed() != 0 {
		p.l.Info("cannot allow scheduler as another one is in process", p.schedulerLogFields(task, r)...)
		return
	}

	schedulers, err := p.cluster.GetSchedulers()
	if err != nil {
		p.l.Error("unable to get schedulers", p.schedulerLogFields(task, r, log.Error(err))...)
		return
	}

	var connected []string
	for _, s := range schedulers {
		if err := s.ConnectionRequestError; err != nil && s.Addr.String() != scheduler.Addr.String() {
			p.l.Info("cannot allow scheduler as connection state of another one is unknown",
				p.schedulerLogFields(task, r, log.Error(err))...)
			return
		}
		if s.Connected {
			connected = append(connected, s.Addr.String())
		}
	}

	if len(connected) > 1 {
		p.l.Info("cannot allow scheduler as there are many connected schedulers",
			p.schedulerLogFields(task, r, log.Strings("connected", connected))...)
		return
	}

	if len(connected) == 0 {
		p.l.Info("cannot allow scheduler as there are no connected schedulers",
			p.schedulerLogFields(task, r)...)
		return
	}

	if len(schedulers) < 2 {
		p.l.Info("cannot allow scheduler as there are not that many left",
			p.schedulerLogFields(task, r, log.Int("left_count", len(schedulers)))...)
		return
	}

	p.l.Info("schedulers are alive -> proceeding to decommission", p.schedulerLogFields(task, r)...)
	p.decommissionScheduler(ctx, r)
}

func (p *TaskProcessor) decommissionScheduler(ctx context.Context, r *models.Scheduler) {
	task := ctx.Value(taskKey).(*models.Task)

	scheduler, ok := p.resolveScheduler(task, r)
	if !ok {
		p.l.Error("unable to find scheduler", p.schedulerLogFields(task, r)...)
		return
	}

	if !scheduler.InMaintenance {
		p.l.Info("starting scheduler maintenance", p.schedulerLogFields(task, r)...)

		req := r.MaintenanceRequest
		if req == nil {
			req = p.makeMaintenanceRequest(task)
		}

		if err := p.dc.SetMaintenance(ctx, scheduler, req); err != nil {
			p.l.Error("error starting scheduler maintenance",
				p.schedulerLogFields(task, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return
		}

		p.l.Info("scheduler maintenance started", p.schedulerLogFields(task, r)...)
		r.StartMaintenance(req)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	p.l.Info("allowing walle to take scheduler)", p.schedulerLogFields(task, r)...)
	r.AllowWalle()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) activateScheduler(ctx context.Context, r *models.Scheduler) {
	task := ctx.Value(taskKey).(*models.Task)

	scheduler, ok := p.resolveScheduler(task, r)
	if !ok {
		p.l.Error("unable to find scheduler", p.schedulerLogFields(task, r)...)
		return
	}

	if scheduler.InMaintenance {
		p.l.Info("finishing scheduler maintenance", p.schedulerLogFields(task, r)...)
		if err := p.dc.UnsetMaintenance(ctx, scheduler, r.MaintenanceRequest.GetID()); err != nil {
			p.l.Error("error finishing scheduler maintenance",
				p.schedulerLogFields(task, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return
		}
		p.l.Info("scheduler maintenance finished", p.schedulerLogFields(task, r)...)
	}

	if r.InMaintenance {
		p.l.Info("finishing scheduler maintenance", p.schedulerLogFields(task, r)...)
		r.FinishMaintenance()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	p.l.Info("finish processing scheduler", p.schedulerLogFields(task, r)...)
	r.SetFinished()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) resolveScheduler(t *models.Task, r *models.Scheduler) (*ytsys.Scheduler, bool) {
	c, ok := p.cluster.GetComponent(r.Path)
	if !ok {
		p.l.Error("unable to resolve scheduler component", p.schedulerLogFields(t, r)...)
		return nil, false
	}
	return c.(*ytsys.Scheduler), true
}

// schedulerLogFields creates a slice with task and scheduler fields to log.
func (p *TaskProcessor) schedulerLogFields(t *models.Task, n *models.Scheduler, extra ...log.Field) []log.Field {
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
