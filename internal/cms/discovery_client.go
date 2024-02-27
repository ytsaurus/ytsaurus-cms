package cms

import (
	"context"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytsys"
)

//go:generate mockgen -destination=discovery_client_mock.go -package cms . DiscoveryClient

type DiscoveryClient interface {
	GetIntegrityIndicators(ctx context.Context) (*ytsys.ChunkIntegrity, error)

	GetHydraState(ctx context.Context, path ypath.Path) (*ytsys.HydraState, error)

	Ban(ctx context.Context, c ytsys.Component, banMsg string) error
	Unban(ctx context.Context, c ytsys.Component) error

	SetMaintenance(ctx context.Context, c ytsys.Component, req *ytsys.MaintenanceRequest) error
	UnsetMaintenance(ctx context.Context, c ytsys.Component, requestID string) error
	HasMaintenanceAttr(ctx context.Context, c ytsys.Component) (ytsys.YTBool, error)

	SetTag(ctx context.Context, path ypath.Path, tag string) error
	RemoveTag(ctx context.Context, path ypath.Path, tag string) error

	MarkNodeDecommissioned(ctx context.Context, addr *ytsys.Addr, msg string) error
	UnmarkNodeDecommissioned(ctx context.Context, addr *ytsys.Addr) error
	GetNodeChunks(ctx context.Context, addr *ytsys.Addr,
		opts *ytsys.GetNodeChunksOption) ([]*ytsys.Chunk, error)

	OverrideRemovalSlots(ctx context.Context, addr *ytsys.Addr, slots int) error
	DropRemovalSlotsOverride(ctx context.Context, addr *ytsys.Addr) error

	DisableSchedulerJobs(ctx context.Context, addr *ytsys.Addr) error
	EnableSchedulerJobs(ctx context.Context, addr *ytsys.Addr) error

	DisableWriteSessions(ctx context.Context, addr *ytsys.Addr) error
	EnableWriteSessions(ctx context.Context, addr *ytsys.Addr) error

	AddMaintenance(
		ctx context.Context,
		component yt.MaintenanceComponent,
		addr *ytsys.Addr,
		maintenanceType yt.MaintenanceType,
		comment string,
		opts *yt.AddMaintenanceOptions,
	) (*yt.MaintenanceID, error)
	RemoveMaintenance(
		ctx context.Context,
		component yt.MaintenanceComponent,
		addr *ytsys.Addr,
		opts *yt.RemoveMaintenanceOptions,
	) (*yt.RemoveMaintenanceResponse, error)

	PathExists(ctx context.Context, path ypath.Path) (bool, error)

	GetStrongGuaranteeResources(
		ctx context.Context,
		path ypath.Path,
	) (*ytsys.StrongGuaranteeResources, error)
	SetStrongGuaranteeCPU(ctx context.Context, path ypath.Path, guarantee float64) error
	GetReservePoolCMSLimits(ctx context.Context, path ypath.Path) (*ytsys.ReservePoolCMSLimits, error)

	ListNodes(ctx context.Context, attrs []string) ([]*ytsys.Node, error)

	DisableChunkLocations(ctx context.Context, addr *ytsys.Addr, uuids []guid.GUID) ([]guid.GUID, error)
	DestroyChunkLocations(ctx context.Context, addr *ytsys.Addr, recoverUnlinkedDisks bool, uuids []guid.GUID) ([]guid.GUID, error)
	ResurrectChunkLocations(ctx context.Context, addr *ytsys.Addr, uuids []guid.GUID) ([]guid.GUID, error)
	GetDiskIDsMismatched(ctx context.Context, node *ytsys.Node) (*bool, error)
	RequestRestart(ctx context.Context, addr *ytsys.Addr) error
	GetNodeStartTime(ctx context.Context, node *ytsys.Node) (*yson.Time, error)
}
