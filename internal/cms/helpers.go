package cms

import (
	"context"
	"strings"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytsys"
)

const (
	robotCMS = "robot-yt-cms"
)

// nopLocker is an implementations of sync.Locker that does nothing.
type nopLocker int

func (l nopLocker) Lock() {}

func (l nopLocker) Unlock() {}

func containCMSMaintenanceRequest(reqs ytsys.SystemMaintenanceRequestMap, t yt.MaintenanceType) bool {
	for _, req := range reqs {
		if req.User == robotCMS && req.Type == t {
			return true
		}
	}
	return false
}

func enableSchedulerJobs(ctx context.Context, dc DiscoveryClient, r *models.Node, useMaintenanceAPI bool) error {
	if useMaintenanceAPI {
		_, err := dc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, r.Addr, &yt.RemoveMaintenanceOptions{
			Mine: ptr.Bool(true),
			Type: ptr.T(yt.MaintenanceTypeDisableSchedulerJobs),
		})
		return err
	}
	return dc.EnableSchedulerJobs(ctx, r.Addr)
}

func disableSchedulerJobs(ctx context.Context, dc DiscoveryClient, r *models.Node, useMaintenanceAPI bool) error {
	if useMaintenanceAPI {
		_, err := dc.AddMaintenance(
			ctx,
			yt.MaintenanceComponentClusterNode,
			r.Addr,
			yt.MaintenanceTypeDisableSchedulerJobs,
			"",
			nil,
		)
		return err
	}
	return dc.DisableSchedulerJobs(ctx, r.Addr)
}

func enableWriteSessions(ctx context.Context, dc DiscoveryClient, r *models.Node, useMaintenanceAPI bool) error {
	if useMaintenanceAPI {
		_, err := dc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, r.Addr, &yt.RemoveMaintenanceOptions{
			Mine: ptr.Bool(true),
			Type: ptr.T(yt.MaintenanceTypeDisableWriteSessions),
		})
		return err
	}
	return dc.EnableWriteSessions(ctx, r.Addr)
}

func disableWriteSessions(ctx context.Context, dc DiscoveryClient, r *models.Node, useMaintenanceAPI bool) error {
	if useMaintenanceAPI {
		_, err := dc.AddMaintenance(
			ctx,
			yt.MaintenanceComponentClusterNode,
			r.Addr,
			yt.MaintenanceTypeDisableWriteSessions,
			"",
			nil,
		)
		return err
	}
	return dc.DisableWriteSessions(ctx, r.Addr)
}

func unbanNode(ctx context.Context, dc DiscoveryClient, node *ytsys.Node, useMaintenanceAPI bool) error {
	if useMaintenanceAPI {
		_, err := dc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, node.Addr, &yt.RemoveMaintenanceOptions{
			Mine: ptr.Bool(true),
			Type: ptr.T(yt.MaintenanceTypeBan),
		})
		return err
	}
	return dc.Unban(ctx, node)
}

func banNode(ctx context.Context, dc DiscoveryClient, node *ytsys.Node, useMaintenanceAPI bool, banMsg string) error {
	if useMaintenanceAPI {
		_, err := dc.AddMaintenance(
			ctx,
			yt.MaintenanceComponentClusterNode,
			node.Addr,
			yt.MaintenanceTypeBan,
			"",
			nil,
		)
		return err
	}

	return dc.Ban(ctx, node, banMsg)
}

func unmarkNodeDecommissioned(ctx context.Context, dc DiscoveryClient, node *ytsys.Node, useMaintenanceAPI bool) error {
	if useMaintenanceAPI {
		_, err := dc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, node.Addr, &yt.RemoveMaintenanceOptions{
			Mine: ptr.Bool(true),
			Type: ptr.T(yt.MaintenanceTypeDecommission),
		})
		return err
	}
	return dc.UnmarkNodeDecommissioned(ctx, node.Addr)
}

func markNodeDecommissioned(
	ctx context.Context, dc DiscoveryClient, node *ytsys.Node,
	useMaintenanceAPI bool,
	decommisionMsg string,
) error {
	if useMaintenanceAPI {
		_, err := dc.AddMaintenance(
			ctx,
			yt.MaintenanceComponentClusterNode,
			node.Addr,
			yt.MaintenanceTypeDecommission,
			"",
			nil,
		)
		return err
	}
	return dc.MarkNodeDecommissioned(ctx, node.Addr, decommisionMsg)
}

func getPodIDFromYPath(path ypath.Path) string {
	pathSplit := strings.Split(path.String(), "/")
	addr := pathSplit[len(pathSplit)-1]
	shortAddr := strings.Split(addr, ".")[0]

	return shortAddr
}
