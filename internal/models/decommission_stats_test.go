package models

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytlog"
	"go.ytsaurus.tech/yt/go/ytsys"
)

func TestDecommissionStats_TabletCellBundles(t *testing.T) {
	bs := NewDecommissionStats(ytlog.Must())
	require.Zero(t, bs.TabletCommonNodeCount)
	require.Empty(t, bs.Slots)

	tabletCommonNode := &ytsys.Node{
		Addr:         &ytsys.Addr{FQDN: "1.1.1.1", Port: "9012"},
		Annotations:  &ytsys.Annotations{PhysicalHost: "1.1.1.1"},
		ID:           yt.NodeID(guid.New()),
		Banned:       true,
		BanMessage:   "banned by gocms",
		State:        ytsys.NodeStateOnline,
		LastSeenTime: yson.Time{},
		Tags:         []string{ytsys.TabletCommonTag},
	}

	bundles := &discovery.CellBundles{
		Bundles: []*ytsys.TabletCellBundle{
			{
				Name:            "default",
				BalancerEnabled: new(ytsys.YTBool),
				Nodes:           []*ytsys.Addr{tabletCommonNode.Addr, {FQDN: "2.2.2.2", Port: "9012"}},
				Production:      nil,
				Slots: map[ytsys.Addr][]*ytsys.TabletSlot{
					*tabletCommonNode.Addr: {
						&ytsys.TabletSlot{
							State:  ytsys.TabletSlotStateLeading,
							CellID: yt.NodeID(guid.New()),
						},
						&ytsys.TabletSlot{
							State: ytsys.TabletSlotStateNone,
						},
						&ytsys.TabletSlot{
							State: ytsys.TabletSlotStateNone,
						},
					},
					ytsys.Addr{FQDN: "2.2.2.2", Port: "9012"}: {
						&ytsys.TabletSlot{
							State: ytsys.TabletSlotStateNone,
						},
					},
				},
			},
			{
				Name:            "sys",
				BalancerEnabled: new(ytsys.YTBool),
				Nodes:           []*ytsys.Addr{tabletCommonNode.Addr},
				Production:      nil,
				Slots: map[ytsys.Addr][]*ytsys.TabletSlot{
					*tabletCommonNode.Addr: {
						&ytsys.TabletSlot{
							State: ytsys.TabletSlotStateNone,
						},
					},
				},
			},
		},
		BalancerDisabled: false,
	}

	bs.OnNodeDecommission(tabletCommonNode, nil, bundles)
	require.Equal(t, 1, bs.TabletCommonNodeCount)
	require.Equal(t, 3, bs.Slots["default"])
}

func TestDecommissionStats_PoolTrees(t *testing.T) {
	bs := NewDecommissionStats(ytlog.Must())
	require.Empty(t, bs.CPU)
	require.Empty(t, bs.GPU)
	require.Empty(t, bs.TotalGPU)

	node := &ytsys.Node{
		Addr:           &ytsys.Addr{FQDN: "1.1.1.1", Port: "9012"},
		Annotations:    &ytsys.Annotations{PhysicalHost: "1.1.1.1"},
		ID:             yt.NodeID(guid.New()),
		State:          ytsys.NodeStateOnline,
		LastSeenTime:   yson.Time{},
		ResourceLimits: &ytsys.NodeResourceLimits{CPU: 100, GPU: 8},
	}

	tree := &ytsys.PoolTree{
		Name: "gpu_tesla_a100",
		Nodes: map[string]*ytsys.PoolTreeNode{
			"taxi-root": {
				Name:      "taxi-root",
				Resources: &ytsys.PoolTreeResourceLimits{CPU: 100.0, GPU: 8},
			},
			"research": {
				Name:      "research",
				Resources: &ytsys.PoolTreeResourceLimits{CPU: 100.0},
			},
		},
		AvailableResources: &ytsys.PoolTreeAvailableResources{CPU: 2000.0, GPU: 1000.0},
	}

	bs.OnNodeDecommission(node, tree, &discovery.CellBundles{})
	require.Equal(t, 100.0, bs.CPU["gpu_tesla_a100"])
	require.Equal(t, 8.0, bs.GPU["gpu_tesla_a100"][node.GetAddr().String()])
	require.Equal(t, 8.0, bs.TotalGPU["gpu_tesla_a100"])
}

func TestDecommissionStats_Reload(t *testing.T) {
	bs := NewDecommissionStats(ytlog.Must())
	require.Empty(t, bs.Slots)
	require.Empty(t, bs.CPU)
	require.Empty(t, bs.GPU)
	require.Empty(t, bs.TotalGPU)

	node1 := &ytsys.Node{
		Addr:           &ytsys.Addr{FQDN: "1.1.1.1", Port: "9012"},
		Annotations:    &ytsys.Annotations{PhysicalHost: "1.1.1.1"},
		ID:             yt.NodeID(guid.New()),
		State:          ytsys.NodeStateOnline,
		LastSeenTime:   yson.Time{},
		ResourceLimits: &ytsys.NodeResourceLimits{CPU: 100, GPU: 8},
	}
	node2 := &ytsys.Node{
		Addr:           &ytsys.Addr{FQDN: "2.2.2.2", Port: "9012"},
		Annotations:    &ytsys.Annotations{PhysicalHost: "2.2.2.2"},
		ID:             yt.NodeID(guid.New()),
		State:          ytsys.NodeStateOnline,
		LastSeenTime:   yson.Time{},
		ResourceLimits: &ytsys.NodeResourceLimits{CPU: 100, GPU: 8},
	}

	tree := &ytsys.PoolTree{
		Name: "gpu_tesla_a100",
		Nodes: map[string]*ytsys.PoolTreeNode{
			"taxi-root": {
				Name:      "taxi-root",
				Resources: &ytsys.PoolTreeResourceLimits{CPU: 100.0, GPU: 8},
			},
			"research": {
				Name:      "research",
				Resources: &ytsys.PoolTreeResourceLimits{CPU: 100.0},
			},
		},
		AvailableResources: &ytsys.PoolTreeAvailableResources{CPU: 2000.0, GPU: 1000.0},
	}

	bs.OnNodeDecommission(node1, tree, &discovery.CellBundles{})
	bs.OnNodeDecommission(node2, tree, &discovery.CellBundles{})
	bs.Reload()
	require.Empty(t, bs.Slots)
	require.Empty(t, bs.CPU)
	require.Equal(t, 8.0, bs.GPU["gpu_tesla_a100"][node1.GetAddr().String()])
	require.Equal(t, 8.0, bs.GPU["gpu_tesla_a100"][node2.GetAddr().String()])
	require.Equal(t, 16.0, bs.TotalGPU["gpu_tesla_a100"])
}

func TestDecommissionStats_Remove(t *testing.T) {
	bs := NewDecommissionStats(ytlog.Must())
	require.Empty(t, bs.CPU)
	require.Empty(t, bs.GPU)
	require.Empty(t, bs.TotalGPU)

	node1 := &ytsys.Node{
		Addr:           &ytsys.Addr{FQDN: "1.1.1.1", Port: "9012"},
		Annotations:    &ytsys.Annotations{PhysicalHost: "1.1.1.1"},
		ID:             yt.NodeID(guid.New()),
		State:          ytsys.NodeStateOnline,
		LastSeenTime:   yson.Time{},
		ResourceLimits: &ytsys.NodeResourceLimits{CPU: 100, GPU: 8},
	}
	node2 := &ytsys.Node{
		Addr:           &ytsys.Addr{FQDN: "2.2.2.2", Port: "9012"},
		Annotations:    &ytsys.Annotations{PhysicalHost: "2.2.2.2"},
		ID:             yt.NodeID(guid.New()),
		State:          ytsys.NodeStateOnline,
		LastSeenTime:   yson.Time{},
		ResourceLimits: &ytsys.NodeResourceLimits{CPU: 100, GPU: 8},
	}

	tree := &ytsys.PoolTree{
		Name: "gpu_tesla_a100",
		Nodes: map[string]*ytsys.PoolTreeNode{
			"taxi-root": {
				Name:      "taxi-root",
				Resources: &ytsys.PoolTreeResourceLimits{CPU: 100.0, GPU: 8},
			},
			"research": {
				Name:      "research",
				Resources: &ytsys.PoolTreeResourceLimits{CPU: 100.0},
			},
		},
		AvailableResources: &ytsys.PoolTreeAvailableResources{CPU: 2000.0, GPU: 1000.0},
	}

	bs.OnNodeDecommission(node1, tree, &discovery.CellBundles{})
	bs.OnNodeDecommission(node2, tree, &discovery.CellBundles{})
	bs.Remove("gpu_tesla_a100", node1.GetAddr().String())
	require.Empty(t, bs.GPU["gpu_tesla_a100"][node1.GetAddr().String()])
	require.Equal(t, 8.0, bs.TotalGPU["gpu_tesla_a100"])
	bs.Remove("gpu_tesla_a100", node2.GetAddr().String())
	require.Empty(t, bs.GPU["gpu_tesla_a100"][node2.GetAddr().String()])
	require.Empty(t, bs.TotalGPU["gpu_tesla_a100"])

}
