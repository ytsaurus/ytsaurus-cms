package cms

import (
	"context"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/ytsys"
)

type rpcProxyKeyType int

// rpcProxyKey is a key used to access cached rpc proxy cluster state in ctx.
var rpcProxyKey rpcProxyKeyType

func (p *TaskProcessor) processRPCProxy(ctx context.Context, r *models.RPCProxy) {
	task := ctx.Value(taskKey).(*models.Task)
	p.l.Debug("processing rpc proxy", p.rpcProxyLogFields(task, r)...)

	proxy, ok := p.resolveRPCProxy(task, r)
	if !ok {
		// RPC proxy of deleted task was already removed from cypress.
		if p.cluster.Err() == nil && task.DeletionRequested && p.CheckPathMissing(ctx, r.Path) && r.State != models.RPCProxyStateFinished {
			p.l.Info("finish processing deleted rpc proxy", p.rpcProxyLogFields(task, r)...)
			r.SetFinished()
			p.tryUpdateTaskInStorage(ctx, task)
		}
		return
	}
	proxyCtx := context.WithValue(ctx, rpcProxyKey, proxy)

	if task.DeletionRequested {
		p.l.Debug("deletion requested -> activating rpc proxy", p.rpcProxyLogFields(task, r)...)
		p.activateRPCProxy(proxyCtx, task, proxy, r)
		return
	}

	switch r.State {
	case models.RPCProxyStateAccepted:
		p.processPendingRPCProxy(proxyCtx, task, proxy, r)
	case models.RPCProxyStateProcessed:
	default:
		p.l.Error("unexpected rpc proxy state", log.String("state", string(r.State)))
	}
}

func (p *TaskProcessor) processPendingRPCProxy(
	ctx context.Context,
	task *models.Task,
	proxy *ytsys.RPCProxy,
	r *models.RPCProxy,
) {
	if err := p.startRPCProxyMaintenance(ctx, task, proxy, r); err != nil {
		return
	}

	if proxy.Banned {
		p.l.Debug("rpc proxy is banned -> proceeding to decommission", p.rpcProxyLogFields(task, r)...)
		p.decommissionRPCProxy(ctx, task, proxy, r)
		return
	}

	if !p.checkRPCProxyAlive(proxy) {
		p.l.Debug("last rpc proxy update was a long time age -> proceeding to decommission",
			p.rpcProxyLogFields(task, r)...)
		p.decommissionRPCProxy(ctx, task, proxy, r)
		return
	}

	p.rpcProxyRoleLimits.Reload()
	if p.rpcProxyRoleLimits.Ban(proxy) {
		p.l.Debug("rate limits allow rpc proxy decommission", p.rpcProxyLogFields(task, r)...)
		p.decommissionRPCProxy(ctx, task, proxy, r)
	} else {
		p.l.Debug("rate limits forbid rpc proxy decommission", p.rpcProxyLogFields(task, r)...)
	}
}

func (p *TaskProcessor) startRPCProxyMaintenance(
	ctx context.Context,
	task *models.Task,
	proxy *ytsys.RPCProxy,
	r *models.RPCProxy,
) error {
	if proxy.HasMaintenanceAttr() && r.InMaintenance {
		p.l.Debug("rpc proxy maintenance request is already started", p.rpcProxyLogFields(task, r)...)
		return nil
	}

	p.l.Debug("starting rpc proxy maintenance request", p.rpcProxyLogFields(task, r)...)

	req := r.MaintenanceRequest
	if req == nil {
		req = p.makeMaintenanceRequest(task)
	}

	if err := p.dc.SetMaintenance(ctx, proxy, req); err != nil {
		p.l.Error("error starting rpc proxy maintenance request",
			p.rpcProxyLogFields(task, r, log.Error(err))...)
		p.failedMaintenanceRequestUpdates.Inc()
		return err
	}

	p.l.Info("rpc proxy maintenance request started", p.rpcProxyLogFields(task, r)...)
	if r.MaintenanceRequest == nil {
		r.StartMaintenance(req)
		p.tryUpdateTaskInStorage(ctx, task)
	}

	return nil
}

func (p *TaskProcessor) finishRPCProxyMaintenance(
	ctx context.Context,
	task *models.Task,
	proxy *ytsys.RPCProxy,
	r *models.RPCProxy,
) error {
	hasMaintenanceAttr, err := p.dc.HasMaintenanceAttr(ctx, proxy)
	if err != nil {
		p.l.Error("rpc proxy maintenance status unknown", p.rpcProxyLogFields(task, r, log.Error(err))...)
		return err
	}

	if hasMaintenanceAttr {
		p.l.Debug("finishing rpc proxy maintenance", p.rpcProxyLogFields(task, r)...)
		if err := p.dc.UnsetMaintenance(ctx, proxy, r.MaintenanceRequest.GetID()); err != nil {
			p.l.Error("error finishing rpc proxy maintenance",
				p.rpcProxyLogFields(task, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return err
		}
		p.l.Info("rpc proxy maintenance finished", p.rpcProxyLogFields(task, r)...)
	}

	if r.InMaintenance {
		p.l.Info("rpc proxy maintenance finished in storage", p.rpcProxyLogFields(task, r)...)
		r.FinishMaintenance()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	return nil
}

// decommissionRPCProxy removes rpc proxy from cluster.
func (p *TaskProcessor) decommissionRPCProxy(
	ctx context.Context,
	task *models.Task,
	proxy *ytsys.RPCProxy,
	r *models.RPCProxy,
) {
	if !proxy.Banned {
		p.l.Debug("banning rpc proxy", p.rpcProxyLogFields(task, r)...)
		if err := p.dc.Ban(ctx, proxy, p.makeBanMessage(task)); err != nil {
			p.l.Error("error banning rpc proxy", p.rpcProxyLogFields(task, r, log.Error(err))...)
			p.failedBans.Inc()
			return
		}

		p.l.Info("rpc proxy banned", p.rpcProxyLogFields(task, r)...)
		r.Ban()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	p.allowRPCProxy(ctx, task, r)
}

// activateRPCProxy adds rpc proxy to cluster.
func (p *TaskProcessor) activateRPCProxy(
	ctx context.Context,
	t *models.Task,
	proxy *ytsys.RPCProxy,
	r *models.RPCProxy,
) {
	if err := p.unbanRPCProxy(ctx, t, proxy, r); err != nil {
		return
	}

	if err := p.finishRPCProxyMaintenance(ctx, t, proxy, r); err != nil {
		return
	}

	r.SetFinished()
	p.tryUpdateTaskInStorage(ctx, t)
}

func (p *TaskProcessor) unbanRPCProxy(
	ctx context.Context,
	t *models.Task,
	proxy *ytsys.RPCProxy,
	r *models.RPCProxy,
) error {
	bannedRecently := time.Time(r.BanTime).After(p.cluster.LastReloadTime())
	// Proxy must be unbanned in both of the following cases:
	// 1. Cluster state is fresh and proxy is banned.
	// 2. Cluster state is stale and thus ban status is stale.
	if bool(proxy.Banned) || !bool(proxy.Banned) && bannedRecently {
		p.l.Debug("unbanning rpc proxy", p.rpcProxyLogFields(t, r)...)
		if err := p.dc.Unban(ctx, proxy); err != nil {
			p.l.Error("error unbanning rpc proxy", p.rpcProxyLogFields(t, r, log.Error(err))...)
			p.failedUnbans.Inc()
			return err
		}
		p.l.Info("rpc proxy unbanned", p.rpcProxyLogFields(t, r)...)
	}

	if r.Banned {
		p.l.Info("unbanning rpc proxy in storage", p.rpcProxyLogFields(t, r)...)
		r.Unban()
		p.tryUpdateTaskInStorage(ctx, t)
	}

	return nil
}

func (p *TaskProcessor) resolveRPCProxy(t *models.Task, r *models.RPCProxy) (*ytsys.RPCProxy, bool) {
	c, ok := p.cluster.GetComponent(r.Path)
	if !ok {
		p.l.Error("unable to resolve rpc proxy component", p.rpcProxyLogFields(t, r)...)
		return nil, false
	}
	return c.(*ytsys.RPCProxy), true
}

func (p *TaskProcessor) checkRPCProxyAlive(proxy *ytsys.RPCProxy) bool {
	return proxy.Alive != nil
}

func (p *TaskProcessor) allowRPCProxy(ctx context.Context, task *models.Task, r *models.RPCProxy) {
	p.l.Info("allowing walle to take rpc proxy", p.rpcProxyLogFields(task, r)...)
	r.AllowWalle()
	p.tryUpdateTaskInStorage(ctx, task)
}

// rpcProxyLogFields creates a slice with task and rpc proxy fields to log.
func (p *TaskProcessor) rpcProxyLogFields(t *models.Task, n *models.RPCProxy, extra ...log.Field) []log.Field {
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
