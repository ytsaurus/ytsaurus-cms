package models

import (
	"go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	"go.ytsaurus.tech/yt/go/ytsys"
)

// DecommissionStats stores statistics of decommissioned nodes.
type DecommissionStats struct {
	// TabletCommonNodeCount stores number of decommissioned nodes with tablet_common tag.
	TabletCommonNodeCount int
	// Slots contains number of tablet slots of decommissioned nodes grouped by bundle name.
	Slots map[string]int
	// GPU stores total amount of CPU of decommissioned nodes grouped by pool tree.
	CPU map[string]float64
	// GPU stores total amount of GPU of decommissioned nodes grouped by pool tree.
	GPU map[string]float64
}

func NewDecommissionStats() *DecommissionStats {
	return &DecommissionStats{
		Slots: make(map[string]int),
		CPU:   make(map[string]float64),
		GPU:   make(map[string]float64),
	}
}

func (s *DecommissionStats) OnNodeDecommission(n *ytsys.Node, t *ytsys.PoolTree, bs *discovery.CellBundles) {
	if n.HasTag(ytsys.TabletCommonTag) {
		s.TabletCommonNodeCount++
	}

	for _, b := range bs.Bundles {
		s.Slots[b.Name] += b.GetNodeSlots(n.Addr)
	}

	if t != nil {
		s.CPU[t.Name] += n.ResourceLimits.CPU
		s.GPU[t.Name] += n.ResourceLimits.GPU
	}
}
