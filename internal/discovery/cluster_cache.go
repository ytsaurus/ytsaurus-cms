package discovery

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/ytsys"
)

type Components map[ypath.Path]ytsys.Component

type CellBundles struct {
	Bundles          []*ytsys.TabletCellBundle
	BalancerDisabled bool
}

type ClusterConfig struct {
	UnmanagedNodeTags []string `yaml:"unmanaged_node_tags"`
}

// Cluster is a cached cluster state.
// That includes information about physical addresses, host roles and states.
type Cluster struct {
	conf *ClusterConfig

	// mu protects err and map fields.
	// Map entries are read-only.
	mu sync.RWMutex

	// err holds last reload result.
	err error

	// lastReloadTime stores time of a last reload finished without an error.
	lastReloadTime time.Time

	hostComponents map[ytsys.PhysicalHost]Components
	components     Components

	primaryMasters     ytsys.PrimaryMasterMap
	secondaryMasters   ytsys.SecondaryMasterMap
	timestampProviders ytsys.TimestampProviderMap
	nodes              ytsys.NodeMap
	httpProxies        ytsys.HTTPProxyMap
	rpcProxies         ytsys.RPCProxyMap
	schedulers         ytsys.SchedulerMap
	controllerAgents   ytsys.ControllerAgentMap

	poolTrees     ytsys.PoolTrees
	nodePoolTrees ytsys.NodePoolTrees

	tabletCells            ytsys.TabletCells
	tabletCellBundles      ytsys.TabletCellBundles
	bundleBalancerDisabled bool

	// tabletCommonNodeCount stores number of free and alive cluster nodes
	// tagged with tablet_common.
	tabletCommonNodeCount int

	chunkIntegrity *ytsys.ChunkIntegrity
}

// NewCluster initializes new cluster.
func NewCluster(conf *ClusterConfig) *Cluster {
	return &Cluster{
		conf:               conf,
		hostComponents:     make(map[ytsys.PhysicalHost]Components),
		components:         make(Components),
		primaryMasters:     make(ytsys.PrimaryMasterMap),
		secondaryMasters:   make(ytsys.SecondaryMasterMap),
		timestampProviders: make(ytsys.TimestampProviderMap),
		nodes:              make(ytsys.NodeMap),
		httpProxies:        make(ytsys.HTTPProxyMap),
		rpcProxies:         make(ytsys.RPCProxyMap),
		schedulers:         make(ytsys.SchedulerMap),
		controllerAgents:   make(ytsys.ControllerAgentMap),
		poolTrees:          make(ytsys.PoolTrees),
		nodePoolTrees:      make(ytsys.NodePoolTrees),
		tabletCells:        make(ytsys.TabletCells),
		tabletCellBundles:  make(ytsys.TabletCellBundles),
		chunkIntegrity:     &ytsys.ChunkIntegrity{},
	}
}

func (c *Cluster) Reload(ctx context.Context, dc *ytsys.Client) error {
	var firstError error
	updateError := func(err error) {
		c.setError(err)
		if firstError == nil {
			firstError = err
		}
	}

	if err := c.reloadPrimaryMasters(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadSecondaryMasters(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadTimestampProviders(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadNodes(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadHTTPProxies(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadRPCProxies(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadSchedulers(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadControllerAgents(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadPoolTrees(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadNodePoolTrees(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadTabletCells(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadTabletCellBundles(ctx, dc); err != nil {
		updateError(err)
	}

	if err := c.reloadChunkIntegrity(ctx, dc); err != nil {
		updateError(err)
	}

	c.rebuildComponentMap()

	if firstError == nil {
		c.setError(nil)
		c.setLastReloadTime(time.Now().UTC())
	}

	return firstError
}

func (c *Cluster) setError(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
}

func (c *Cluster) Err() error {
	c.mu.RLock()
	err := c.err
	c.mu.RUnlock()
	return err
}

func (c *Cluster) setLastReloadTime(t time.Time) {
	c.mu.Lock()
	c.lastReloadTime = t
	c.mu.Unlock()
}

// LastReloadTime returns the time of the last cluster state reload that finished without an error.
func (c *Cluster) LastReloadTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lastReloadTime
}

func (c *Cluster) rebuildComponentMap() {
	hostComponents := make(map[ytsys.PhysicalHost]Components)
	components := make(Components)

	addComponent := func(component ytsys.Component) {
		h := component.GetPhysicalHost()
		if _, ok := hostComponents[h]; !ok {
			hostComponents[h] = make(Components)
		}
		hostComponents[h][component.GetCypressPath()] = component
		components[component.GetCypressPath()] = component
	}

	for _, n := range c.primaryMasters {
		addComponent(n)
	}

	for _, n := range c.secondaryMasters {
		addComponent(n)
	}
	for _, p := range c.timestampProviders {
		addComponent(p)
	}

	for _, n := range c.nodes {
		addComponent(n)
	}

	for _, n := range c.httpProxies {
		addComponent(n)
	}

	for _, n := range c.rpcProxies {
		addComponent(n)
	}

	for _, n := range c.schedulers {
		addComponent(n)
	}

	for _, n := range c.controllerAgents {
		addComponent(n)
	}

	c.mu.Lock()
	c.hostComponents = hostComponents
	c.components = components
	c.mu.Unlock()
}

func (c *Cluster) reloadPrimaryMasters(ctx context.Context, dc *ytsys.Client) error {
	masters, err := dc.GetPrimaryMasters(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.primaryMasters = masters
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadSecondaryMasters(ctx context.Context, dc *ytsys.Client) error {
	masters, err := dc.GetSecondaryMasters(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.secondaryMasters = masters
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadTimestampProviders(ctx context.Context, dc *ytsys.Client) error {
	providers, err := dc.GetTimestampProviders(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.timestampProviders = providers
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadNodes(ctx context.Context, dc *ytsys.Client) error {
	nodes, err := dc.GetNodes(ctx)
	if err != nil {
		return err
	}

	managed := make(ytsys.NodeMap)
	for a, n := range nodes {
		if !n.HasAnyTag(c.conf.UnmanagedNodeTags) {
			managed[a] = n
		}
	}

	tabletCommonNodeCount := managed.GetFreeTabletCommonNodeCount()
	managed.SetSlotState()

	c.mu.Lock()
	c.nodes = managed
	c.tabletCommonNodeCount = tabletCommonNodeCount
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadHTTPProxies(ctx context.Context, dc *ytsys.Client) error {
	proxies, err := dc.GetHTTPProxies(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.httpProxies = proxies
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadRPCProxies(ctx context.Context, dc *ytsys.Client) error {
	proxies, err := dc.GetRPCProxies(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.rpcProxies = proxies
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadSchedulers(ctx context.Context, dc *ytsys.Client) error {
	schedulers, err := dc.GetSchedulers(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.schedulers = schedulers
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadControllerAgents(ctx context.Context, dc *ytsys.Client) error {
	agents, err := dc.GetControllerAgents(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.controllerAgents = agents
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadPoolTrees(ctx context.Context, dc *ytsys.Client) error {
	trees, err := dc.GetPoolTrees(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.poolTrees = trees
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadNodePoolTrees(ctx context.Context, dc *ytsys.Client) error {
	trees, err := dc.GetNodePoolTrees(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.nodePoolTrees = trees
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadTabletCells(ctx context.Context, dc *ytsys.Client) error {
	cells, err := dc.GetTabletCells(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.tabletCells = cells
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadTabletCellBundles(ctx context.Context, dc *ytsys.Client) error {
	bundles, err := dc.GetTabletCellBundles(ctx)
	if err != nil {
		return err
	}

	disabled, err := dc.GetDisableBundleBalancer(ctx)
	if err != nil {
		return err
	}

	if err := c.setBundleSlots(bundles); err != nil {
		return err
	}

	c.mu.Lock()
	c.tabletCellBundles = bundles
	c.bundleBalancerDisabled = disabled
	c.mu.Unlock()

	return nil
}

func (c *Cluster) reloadChunkIntegrity(ctx context.Context, dc *ytsys.Client) error {
	i, err := ytsys.LoadIntegrityIndicators(ctx, dc)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.chunkIntegrity = i
	c.mu.Unlock()

	return nil
}

// setBundleSlots initializes Slots field for given tablet cell bundles.
func (c *Cluster) setBundleSlots(bundles ytsys.TabletCellBundles) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, bundle := range bundles {
		slots := make(map[ytsys.Addr][]*ytsys.TabletSlot)
		for _, addr := range bundle.Nodes {
			node, ok := c.nodes[*addr]
			if !ok {
				return xerrors.Errorf("unable to resolve node: %s", *addr)
			}
			for _, slot := range node.TabletSlots {
				if slot.State == ytsys.TabletSlotStateNone {
					slots[*addr] = append(slots[*addr], slot)
					continue
				}
				cell, ok := c.tabletCells[slot.CellID]
				if !ok {
					return xerrors.Errorf("unable to resolve cell %s", slot.CellID)
				}
				if cell.Bundle == bundle.Name {
					slots[*addr] = append(slots[*addr], slot)
				}
			}
		}
		bundle.Slots = slots
	}

	return nil
}

func (c *Cluster) GetComponents() map[ytsys.PhysicalHost]Components {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hostComponentsCopy := make(map[ytsys.PhysicalHost]Components, len(c.hostComponents))
	for host, components := range c.hostComponents {
		hostComponentsCopy[host] = components
	}

	return hostComponentsCopy
}

func (c *Cluster) GetHostComponents(host ytsys.PhysicalHost) (Components, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hostComponents, ok := c.hostComponents[host]
	if !ok {
		return nil, false
	}

	hostComponentsCopy := make(Components)
	for path, component := range hostComponents {
		hostComponentsCopy[path] = component
	}

	return hostComponentsCopy, true
}

func (c *Cluster) GetComponent(path ypath.Path) (ytsys.Component, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	component, ok := c.components[path]
	if !ok {
		return nil, false
	}

	return component, true
}

func (c *Cluster) GetMasterCell(cellPath ypath.Path) ([]ytsys.Component, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return nil, err
	}

	var components []ytsys.Component

	if cellPath == ytsys.PrimaryMastersPath {
		for _, m := range c.primaryMasters {
			components = append(components, m)
		}
		return components, nil
	}

	if cellPath == ytsys.TimestampProvidersPath {
		for _, p := range c.timestampProviders {
			components = append(components, p)
		}
		return components, nil
	}

	if !strings.HasPrefix(cellPath.String(), ytsys.SecondaryMastersPath.String()) {
		return nil, xerrors.Errorf("invalid master cell path %q", cellPath)
	}

	for _, m := range c.secondaryMasters {
		if strings.HasPrefix(m.Path.String(), cellPath.String()) {
			components = append(components, m)
		}
	}

	return components, nil
}

func (c *Cluster) GetNodes() (ytsys.NodeMap, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return nil, err
	}

	return c.nodes, nil
}

func (c *Cluster) GetHTTPProxies() (ytsys.HTTPProxyMap, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return nil, err
	}

	return c.httpProxies, nil
}

func (c *Cluster) GetRPCProxies() (ytsys.RPCProxyMap, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return nil, err
	}

	return c.rpcProxies, nil
}

func (c *Cluster) GetSchedulers() (ytsys.SchedulerMap, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return nil, err
	}

	return c.schedulers, nil
}

func (c *Cluster) GetControllerAgents() (ytsys.ControllerAgentMap, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return nil, err
	}

	return c.controllerAgents, nil
}

func (c *Cluster) GetNodePoolTree(addr ytsys.Addr) (*ytsys.PoolTree, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return nil, err
	}

	name, ok := c.nodePoolTrees[addr]
	if !ok {
		return nil, nil
	}

	n, ok := c.poolTrees[name]
	if !ok {
		return nil, nil
	}

	return n, nil
}

func (c *Cluster) GetTabletCellBundles(n *ytsys.Node) (*CellBundles, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return nil, err
	}

	var bundles []*ytsys.TabletCellBundle
	for _, slot := range n.TabletSlots {
		if slot.State == ytsys.TabletSlotStateNone {
			continue
		}
		cell, ok := c.tabletCells[slot.CellID]
		if !ok {
			return nil, xerrors.Errorf("unable to resolve cell %s", slot.CellID)
		}
		bundle, ok := c.tabletCellBundles[cell.Bundle]
		if !ok {
			return nil, xerrors.Errorf("unable to resolve tablet cell bundle %s", cell.Bundle)
		}
		bundles = append(bundles, bundle)
	}

	ret := &CellBundles{
		Bundles:          bundles,
		BalancerDisabled: c.bundleBalancerDisabled,
	}

	return ret, nil
}

// GetTabletCommonNodeCount returns number of free cluster nodes having corresponding tag.
func (c *Cluster) GetTabletCommonNodeCount() (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return 0, err
	}

	return c.tabletCommonNodeCount, nil
}

// GetChunkIntegrity returns chunk integrity indicators.
func (c *Cluster) GetChunkIntegrity() (*ytsys.ChunkIntegrity, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.err; err != nil {
		return nil, err
	}

	return c.chunkIntegrity, nil
}
