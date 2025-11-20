package cms

import (
	"context"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/ytsys"
)

type httpProxyKeyType int

// httpProxyKey is a key used to access cached http proxy cluster state in ctx.
var httpProxyKey httpProxyKeyType

func (p *TaskProcessor) processHTTPProxy(ctx context.Context, r *models.HTTPProxy) {
	task := ctx.Value(taskKey).(*models.Task)
	p.l.Debug("processing http proxy", p.httpProxyLogFields(task, r)...)

	proxy, ok := p.resolveHTTPProxy(task, r)
	if !ok {
		// HTTP proxy of deleted task was already removed from cypress.
		if p.cluster.Err() == nil && task.DeletionRequested && p.CheckPathMissing(ctx, r.Path) {
			p.l.Info("finish processing deleted http proxy", p.httpProxyLogFields(task, r)...)
			r.SetFinished()
			p.tryUpdateTaskInStorage(ctx, task)
		}
		return
	}
	proxyCtx := context.WithValue(ctx, httpProxyKey, proxy)

	if task.DeletionRequested {
		p.l.Info("deletion requested -> activating http proxy", p.httpProxyLogFields(task, r)...)
		p.activateHTTPProxy(proxyCtx, task, proxy, r)
		return
	}

	switch r.State {
	case models.HTTPProxyStateAccepted:
		p.processPendingHTTPProxy(proxyCtx, task, proxy, r)
	case models.HTTPProxyStateProcessed:
	default:
		p.l.Error("unexpected http proxy state", log.String("state", string(r.State)))
	}
}

func (p *TaskProcessor) processPendingHTTPProxy(
	ctx context.Context,
	task *models.Task,
	proxy *ytsys.HTTPProxy,
	r *models.HTTPProxy,
) {
	if proxy.Banned {
		p.l.Info("http proxy is banned -> proceeding to decommission", p.httpProxyLogFields(task, r)...)
		p.decommissionHTTPProxy(ctx, task, proxy, r)
		return
	}

	if !p.checkHTTPProxyAlive(proxy) {
		p.l.Info("last http proxy update was a long time age -> proceeding to decommission",
			p.httpProxyLogFields(task, r, log.Time("updated_at", time.Time(proxy.Liveness.UpdatedAt)))...)
		p.decommissionHTTPProxy(ctx, task, proxy, r)
		return
	}

	p.httpProxyRoleLimits.Reload()
	if p.httpProxyRoleLimits.Ban(proxy) {
		p.l.Debug("rate limits allow http proxy decommission", p.httpProxyLogFields(task, r)...)
		p.decommissionHTTPProxy(ctx, task, proxy, r)
	} else {
		p.l.Debug("rate limits forbid http proxy decommission", p.httpProxyLogFields(task, r)...)
	}
}

// decommissionHTTPProxy removes http proxy from cluster.
func (p *TaskProcessor) decommissionHTTPProxy(
	ctx context.Context,
	task *models.Task,
	proxy *ytsys.HTTPProxy,
	r *models.HTTPProxy,
) {
	if !proxy.Banned {
		p.l.Debug("banning http proxy", p.httpProxyLogFields(task, r)...)
		if err := p.dc.Ban(ctx, proxy, p.makeBanMessage(task)); err != nil {
			p.l.Error("error banning http proxy", p.httpProxyLogFields(task, r, log.Error(err))...)
			p.failedBans.Inc()
			return
		}

		p.l.Info("http proxy banned", p.httpProxyLogFields(task, r)...)
		r.Ban()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	p.allowHTTPProxy(ctx, task, r)
}

// activateHTTPProxy adds http proxy to cluster.
func (p *TaskProcessor) activateHTTPProxy(
	ctx context.Context,
	task *models.Task,
	proxy *ytsys.HTTPProxy,
	r *models.HTTPProxy,
) {
	if err := p.unbanHTTPProxy(ctx, task, proxy, r); err != nil {
		return
	}

	r.SetFinished()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) unbanHTTPProxy(
	ctx context.Context,
	task *models.Task,
	proxy *ytsys.HTTPProxy,
	r *models.HTTPProxy,
) error {
	bannedRecently := time.Time(r.BanTime).After(p.cluster.LastReloadTime())
	// Proxy must be unbanned in both of the following cases:
	// 1. Cluster state is fresh and proxy is banned.
	// 2. Cluster state is stale and thus ban status is stale.
	if bool(proxy.Banned) || !bool(proxy.Banned) && bannedRecently {
		p.l.Debug("unbanning http proxy", p.httpProxyLogFields(task, r)...)
		if err := p.dc.Unban(ctx, proxy); err != nil {
			p.l.Error("error unbanning http proxy", p.httpProxyLogFields(task, r, log.Error(err))...)
			p.failedUnbans.Inc()
			return err
		}
		p.l.Info("http proxy unbanned", p.httpProxyLogFields(task, r)...)
	}

	if r.Banned {
		p.l.Info("unbanning http proxy role", p.httpProxyLogFields(task, r)...)
		r.Unban()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	return nil
}

func (p *TaskProcessor) resolveHTTPProxy(t *models.Task, r *models.HTTPProxy) (*ytsys.HTTPProxy, bool) {
	c, ok := p.cluster.GetComponent(r.Path)
	if !ok {
		p.l.Error("unable to resolve http proxy component", p.httpProxyLogFields(t, r)...)
		return nil, false
	}
	return c.(*ytsys.HTTPProxy), true
}

func (p *TaskProcessor) checkHTTPProxyAlive(proxy *ytsys.HTTPProxy) bool {
	offlinePeriod := time.Since(time.Time(proxy.Liveness.UpdatedAt))
	return offlinePeriod <= p.Conf().OfflineHTTPProxyRetentionPeriod
}

func (p *TaskProcessor) allowHTTPProxy(ctx context.Context, task *models.Task, r *models.HTTPProxy) {
	p.l.Info("allowing walle to take http proxy", p.httpProxyLogFields(task, r)...)
	r.AllowWalle()
	p.tryUpdateTaskInStorage(ctx, task)
}

// httpProxyLogFields creates a slice with task and http proxy fields to log.
func (p *TaskProcessor) httpProxyLogFields(t *models.Task, n *models.HTTPProxy, extra ...log.Field) []log.Field {
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
