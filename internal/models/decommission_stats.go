package models

import (
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	"go.ytsaurus.tech/yt/go/ytsys"
)

// DecommissionStats stores statistics of decommissioned nodes.
type DecommissionStats struct {
	l log.Structured

	// TabletCommonNodeCount stores number of decommissioned nodes with tablet_common tag.
	TabletCommonNodeCount int
	// Slots contains number of tablet slots of decommissioned nodes grouped by bundle name.
	Slots map[string]int
	// GPU stores total amount of CPU of decommissioned nodes grouped by pool tree.
	CPU map[string]float64
	// GPU stores total amount of GPU of decommissioned nodes grouped by pool tree.
	GPU map[string]float64
}

func NewDecommissionStats(l log.Structured) *DecommissionStats {
	return &DecommissionStats{
		l:     l,
		Slots: make(map[string]int),
		CPU:   make(map[string]float64),
		GPU:   make(map[string]float64),
	}
}

func (s *DecommissionStats) OnNodeDecommission(n *ytsys.Node, t *ytsys.PoolTree, bs *discovery.CellBundles) {
	if n.HasTag(ytsys.TabletCommonTag) {
		s.TabletCommonNodeCount++
		s.l.Debug("updated decommissioned tablet node count",
			log.String("host", n.GetPhysicalHost()),
			log.Int("current", s.TabletCommonNodeCount))
	}

	for _, b := range bs.Bundles {
		toAdd := b.GetNodeSlots(n.Addr)
		s.Slots[b.Name] += toAdd
		s.l.Debug("updated decommissioned bundle slots",
			log.String("host", n.GetPhysicalHost()),
			log.String("bundle", b.Name),
			log.Int("added", toAdd),
			log.Int("current", s.Slots[b.Name]))
	}

	if t != nil {
		s.CPU[t.Name] += n.ResourceLimits.CPU
		s.GPU[t.Name] += n.ResourceLimits.GPU
		s.l.Debug("updated decommissioned tree resources",
			log.String("host", n.GetPhysicalHost()),
			log.String("tree", t.Name),
			log.Float64("added_cpu", n.ResourceLimits.CPU),
			log.Float64("current_cpu", s.CPU[t.Name]),
			log.Float64("added_gpu", n.ResourceLimits.GPU),
			log.Float64("current_gpu", s.GPU[t.Name]))
	}
}
