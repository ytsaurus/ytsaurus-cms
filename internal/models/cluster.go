package models

import (
	"context"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/ytsys"
)

//go:generate mockgen -destination=cluster_mock.go -package models . Cluster

type Cluster interface {
	Reload(ctx context.Context, dc *ytsys.Client) error

	GetComponents() map[ytsys.PhysicalHost]discovery.Components
	GetComponent(path ypath.Path) (ytsys.Component, bool)
	GetHostComponents(host ytsys.PhysicalHost) (discovery.Components, bool)
	LastReloadTime() time.Time
	Err() error

	GetNodes() (ytsys.NodeMap, error)
	GetHTTPProxies() (ytsys.HTTPProxyMap, error)
	GetRPCProxies() (ytsys.RPCProxyMap, error)
	GetSchedulers() (ytsys.SchedulerMap, error)
	GetControllerAgents() (ytsys.ControllerAgentMap, error)
	GetQueueAgents() (ytsys.QueueAgentMap, error)
	GetMasterCell(cellPath ypath.Path) ([]ytsys.Component, error)

	GetNodePoolTree(addr ytsys.Addr) (*ytsys.PoolTree, error)
	GetTabletCellBundles(n *ytsys.Node) (*discovery.CellBundles, error)
	GetTabletCommonNodeCount() (int, error)

	GetChunkIntegrity() (*ytsys.ChunkIntegrity, error)

	OnNodeDecommission(n *ytsys.Node)
	GetDecommissionStats() *DecommissionStats
}

// cluster is a Cluster implementation that stores cached cluster state
// and some temporary data invalidated on cache reload.
type cluster struct {
	l  log.Structured
	mu sync.RWMutex
	*discovery.Cluster
	decommissionStats *DecommissionStats
}

// NewCluster initializes new cluster.
func NewCluster(conf *discovery.ClusterConfig, l log.Structured) *cluster {
	return &cluster{
		l:                 l,
		Cluster:           discovery.NewCluster(conf),
		decommissionStats: NewDecommissionStats(l),
	}
}

// Reload queries cluster, updates cached state and reloads decommission stats.
func (c *cluster) Reload(ctx context.Context, dc *ytsys.Client) error {
	if err := c.Cluster.Reload(ctx, dc); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.decommissionStats.Reload()

	return nil
}

func (c *cluster) GetDecommissionStats() *DecommissionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.decommissionStats
}

func (c *cluster) OnNodeDecommission(n *ytsys.Node) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	t, err := c.GetNodePoolTree(*n.Addr)
	if err != nil || t == nil {
		return
	}

	bundles, err := c.GetTabletCellBundles(n)
	if err != nil {
		bundles = &discovery.CellBundles{}
	}

	c.decommissionStats.OnNodeDecommission(n, t, bundles)
}

func (c *cluster) GetTabletCommonNodeCount() (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	n, err := c.Cluster.GetTabletCommonNodeCount()
	if err != nil {
		return 0, err
	}

	return n - c.decommissionStats.TabletCommonNodeCount, nil
}
