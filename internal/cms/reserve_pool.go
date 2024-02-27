package cms

import (
	"context"

	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/ytsys"
)

type ReservePool struct {
	dc   DiscoveryClient
	path ypath.Path

	limits *ytsys.ReservePoolCMSLimits

	reservedCPU     float64
	nodeReservedCPU map[ytsys.Addr]float64
}

func NewReservePool(dc DiscoveryClient, path ypath.Path, tasks []*models.Task) *ReservePool {
	r := &ReservePool{
		dc:              dc,
		path:            path,
		limits:          &ytsys.ReservePoolCMSLimits{CPU: 0.0, Memory: 0},
		reservedCPU:     0.0,
		nodeReservedCPU: make(map[ytsys.Addr]float64),
	}
	for _, t := range tasks {
		nodes := t.GetNodes()
		for _, node := range nodes {
			if node.ReservedCPU == 0 || !node.HasFlavor(ytsys.FlavorExec) {
				continue
			}
			// This difference allows us to ignore the existence of duplicated
			r.reservedCPU += node.ReservedCPU - r.nodeReservedCPU[*node.Addr]
			r.nodeReservedCPU[*node.Addr] = node.ReservedCPU
		}
	}
	return r
}

func (r *ReservePool) GetCPUGuarantee() float64 {
	return r.limits.CPU - r.reservedCPU
}

func (r *ReservePool) GetCPULimit() float64 {
	return r.limits.CPU
}

func (r *ReservePool) Allow(ctx context.Context, n *ytsys.Node) bool {
	limits, err := r.dc.GetReservePoolCMSLimits(ctx, r.path)
	if err == nil {
		r.limits = limits
	}

	if r.limits.CPU-r.reservedCPU-n.ResourceLimits.CPU < 0 {
		return false
	}
	return true
}

// AddNode adds node cpu limit to reserve pool, updating cpu guarantee in Cypress.
// The function also checks that node isn't a duplicate.
func (r *ReservePool) AddNode(ctx context.Context, role *models.Node) {
	delta := role.ReservedCPU - r.nodeReservedCPU[*role.Addr]
	if delta == 0.0 {
		return
	}
	r.reservedCPU += delta
	// update nodeReservedCPU to detect duplicates in future tasks
	r.nodeReservedCPU[*role.Addr] = role.ReservedCPU

	_ = r.dc.SetStrongGuaranteeCPU(ctx, r.path, r.limits.CPU-r.reservedCPU)
}

func (r *ReservePool) UpdateStrongGuarantees(ctx context.Context) error {
	limits, err := r.dc.GetReservePoolCMSLimits(ctx, r.path)
	if err != nil {
		return err
	}
	r.limits = limits

	_ = r.dc.SetStrongGuaranteeCPU(ctx, r.path, limits.CPU-r.reservedCPU)
	return nil
}
