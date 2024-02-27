package models

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytsys"
)

func TestDecommissionStats_TabletCellBundles(t *testing.T) {
	bs := NewDecommissionStats()
	require.Zero(t, bs.TabletCommonNodeCount)
	require.Empty(t, bs.Slots)

	tabletCommonNode := &ytsys.Node{
		Addr:         &ytsys.Addr{FQDN: "1.1.1.1", Port: "80"},
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
				Nodes:           []*ytsys.Addr{tabletCommonNode.Addr, {FQDN: "2.2.2.2", Port: "80"}},
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
					ytsys.Addr{FQDN: "2.2.2.2", Port: "80"}: {
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
	bs := NewDecommissionStats()
	require.Empty(t, bs.CPU)
	require.Empty(t, bs.GPU)

	node := &ytsys.Node{
		Addr:           &ytsys.Addr{FQDN: "1.1.1.1", Port: "80"},
		ID:             yt.NodeID(guid.New()),
		State:          ytsys.NodeStateOnline,
		LastSeenTime:   yson.Time{},
		ResourceLimits: &ytsys.NodeResourceLimits{CPU: 100, GPU: 200},
	}

	tree := &ytsys.PoolTree{
		Name: "physical",
		Nodes: map[string]*ytsys.PoolTreeNode{
			"taxi-root": {
				Name:      "taxi-root",
				Resources: &ytsys.PoolTreeResourceLimits{CPU: 100.0, GPU: 200},
			},
			"research": {
				Name:      "research",
				Resources: &ytsys.PoolTreeResourceLimits{CPU: 100.0},
			},
		},
		AvailableResources: &ytsys.PoolTreeAvailableResources{CPU: 2000.0, GPU: 1000.0},
	}

	bs.OnNodeDecommission(node, tree, &discovery.CellBundles{})
	require.Equal(t, 100.0, bs.CPU["physical"])
	require.Equal(t, 200.0, bs.GPU["physical"])
}
