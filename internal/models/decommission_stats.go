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
	// CPU stores total amount of CPU of decommissioned nodes grouped by pool tree.
	CPU map[string]float64
	// GPU stores the number of gpu cards on the decommissioned nodes grouped by pool tree.
	GPU map[string]map[string]float64
	// TotalGPU stores total amount of GPU of decommissioned nodes grouped by pool tree.
	TotalGPU map[string]float64
}

func NewDecommissionStats(l log.Structured) *DecommissionStats {
	return &DecommissionStats{
		l:        l,
		Slots:    make(map[string]int),
		CPU:      make(map[string]float64),
		GPU:      make(map[string]map[string]float64),
		TotalGPU: make(map[string]float64),
	}
}

func (s *DecommissionStats) Reload() {
	s.TotalGPU = make(map[string]float64)
	for poolTree, nodeMap := range s.GPU {
		for _, gpu := range nodeMap {
			s.TotalGPU[poolTree] += gpu
		}
	}
	s.Slots = make(map[string]int)
	s.CPU = make(map[string]float64)
}

func (s *DecommissionStats) Remove(poolTree, addr string) {
	s.l.Debug("removing decommissioned node",
		log.String("addr", addr),
		log.String("pool_tree", poolTree))
	if _, ok := s.GPU[poolTree]; ok {
		if _, ok := s.GPU[poolTree][addr]; ok {
			s.TotalGPU[poolTree] -= s.GPU[poolTree][addr]
			delete(s.GPU[poolTree], addr)
		}
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
		if _, ok := s.GPU[t.Name]; !ok {
			s.GPU[t.Name] = make(map[string]float64)
		}
		s.GPU[t.Name][n.GetAddr().String()] = n.ResourceLimits.GPU
		s.TotalGPU[t.Name] += n.ResourceLimits.GPU

		s.l.Debug("updated decommissioned tree resources",
			log.String("host", n.GetPhysicalHost()),
			log.String("addr", n.GetAddr().String()),
			log.String("tree", t.Name),
			log.Float64("added_cpu", n.ResourceLimits.CPU),
			log.Float64("current_cpu", s.CPU[t.Name]),
			log.Float64("added_gpu", n.ResourceLimits.GPU),
			log.Float64("current_gpu", s.TotalGPU[t.Name]))
	}
}
