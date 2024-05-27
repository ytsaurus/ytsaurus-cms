// Code generated by MockGen. DO NOT EDIT.
// Source: yt/admin/cms/internal/models/cluster.go

// Package models is a generated GoMock package.
package models

import (
	context "context"
	reflect "reflect"
	time "time"

	discovery "go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	ypath "go.ytsaurus.tech/yt/go/ypath"
	ytsys "go.ytsaurus.tech/yt/go/ytsys"
	gomock "github.com/golang/mock/gomock"
)

// MockCluster is a mock of Cluster interface.
type MockCluster struct {
	ctrl     *gomock.Controller
	recorder *MockClusterMockRecorder
}

// MockClusterMockRecorder is the mock recorder for MockCluster.
type MockClusterMockRecorder struct {
	mock *MockCluster
}

// NewMockCluster creates a new mock instance.
func NewMockCluster(ctrl *gomock.Controller) *MockCluster {
	mock := &MockCluster{ctrl: ctrl}
	mock.recorder = &MockClusterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCluster) EXPECT() *MockClusterMockRecorder {
	return m.recorder
}

// Err mocks base method.
func (m *MockCluster) Err() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Err")
	ret0, _ := ret[0].(error)
	return ret0
}

// Err indicates an expected call of Err.
func (mr *MockClusterMockRecorder) Err() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Err", reflect.TypeOf((*MockCluster)(nil).Err))
}

// GetChunkIntegrity mocks base method.
func (m *MockCluster) GetChunkIntegrity() (*ytsys.ChunkIntegrity, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetChunkIntegrity")
	ret0, _ := ret[0].(*ytsys.ChunkIntegrity)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetChunkIntegrity indicates an expected call of GetChunkIntegrity.
func (mr *MockClusterMockRecorder) GetChunkIntegrity() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetChunkIntegrity", reflect.TypeOf((*MockCluster)(nil).GetChunkIntegrity))
}

// GetComponent mocks base method.
func (m *MockCluster) GetComponent(path ypath.Path) (ytsys.Component, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetComponent", path)
	ret0, _ := ret[0].(ytsys.Component)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// GetComponent indicates an expected call of GetComponent.
func (mr *MockClusterMockRecorder) GetComponent(path interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetComponent", reflect.TypeOf((*MockCluster)(nil).GetComponent), path)
}

// GetComponents mocks base method.
func (m *MockCluster) GetComponents() map[ytsys.PhysicalHost]discovery.Components {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetComponents")
	ret0, _ := ret[0].(map[ytsys.PhysicalHost]discovery.Components)
	return ret0
}

// GetComponents indicates an expected call of GetComponents.
func (mr *MockClusterMockRecorder) GetComponents() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetComponents", reflect.TypeOf((*MockCluster)(nil).GetComponents))
}

// GetControllerAgents mocks base method.
func (m *MockCluster) GetControllerAgents() (ytsys.ControllerAgentMap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetControllerAgents")
	ret0, _ := ret[0].(ytsys.ControllerAgentMap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetControllerAgents indicates an expected call of GetControllerAgents.
func (mr *MockClusterMockRecorder) GetControllerAgents() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetControllerAgents", reflect.TypeOf((*MockCluster)(nil).GetControllerAgents))
}

// GetDecommissionStats mocks base method.
func (m *MockCluster) GetDecommissionStats() *DecommissionStats {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDecommissionStats")
	ret0, _ := ret[0].(*DecommissionStats)
	return ret0
}

// GetDecommissionStats indicates an expected call of GetDecommissionStats.
func (mr *MockClusterMockRecorder) GetDecommissionStats() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDecommissionStats", reflect.TypeOf((*MockCluster)(nil).GetDecommissionStats))
}

// GetHTTPProxies mocks base method.
func (m *MockCluster) GetHTTPProxies() (ytsys.HTTPProxyMap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHTTPProxies")
	ret0, _ := ret[0].(ytsys.HTTPProxyMap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetHTTPProxies indicates an expected call of GetHTTPProxies.
func (mr *MockClusterMockRecorder) GetHTTPProxies() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHTTPProxies", reflect.TypeOf((*MockCluster)(nil).GetHTTPProxies))
}

// GetHostComponents mocks base method.
func (m *MockCluster) GetHostComponents(host ytsys.PhysicalHost) (discovery.Components, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHostComponents", host)
	ret0, _ := ret[0].(discovery.Components)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// GetHostComponents indicates an expected call of GetHostComponents.
func (mr *MockClusterMockRecorder) GetHostComponents(host interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHostComponents", reflect.TypeOf((*MockCluster)(nil).GetHostComponents), host)
}

// GetMasterCell mocks base method.
func (m *MockCluster) GetMasterCell(cellPath ypath.Path) ([]ytsys.Component, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMasterCell", cellPath)
	ret0, _ := ret[0].([]ytsys.Component)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMasterCell indicates an expected call of GetMasterCell.
func (mr *MockClusterMockRecorder) GetMasterCell(cellPath interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMasterCell", reflect.TypeOf((*MockCluster)(nil).GetMasterCell), cellPath)
}

// GetNodePoolTree mocks base method.
func (m *MockCluster) GetNodePoolTree(addr ytsys.Addr) (*ytsys.PoolTree, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNodePoolTree", addr)
	ret0, _ := ret[0].(*ytsys.PoolTree)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNodePoolTree indicates an expected call of GetNodePoolTree.
func (mr *MockClusterMockRecorder) GetNodePoolTree(addr interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNodePoolTree", reflect.TypeOf((*MockCluster)(nil).GetNodePoolTree), addr)
}

// GetNodes mocks base method.
func (m *MockCluster) GetNodes() (ytsys.NodeMap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNodes")
	ret0, _ := ret[0].(ytsys.NodeMap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNodes indicates an expected call of GetNodes.
func (mr *MockClusterMockRecorder) GetNodes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNodes", reflect.TypeOf((*MockCluster)(nil).GetNodes))
}

// GetRPCProxies mocks base method.
func (m *MockCluster) GetRPCProxies() (ytsys.RPCProxyMap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRPCProxies")
	ret0, _ := ret[0].(ytsys.RPCProxyMap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetRPCProxies indicates an expected call of GetRPCProxies.
func (mr *MockClusterMockRecorder) GetRPCProxies() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRPCProxies", reflect.TypeOf((*MockCluster)(nil).GetRPCProxies))
}

// GetSchedulers mocks base method.
func (m *MockCluster) GetSchedulers() (ytsys.SchedulerMap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSchedulers")
	ret0, _ := ret[0].(ytsys.SchedulerMap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSchedulers indicates an expected call of GetSchedulers.
func (mr *MockClusterMockRecorder) GetSchedulers() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSchedulers", reflect.TypeOf((*MockCluster)(nil).GetSchedulers))
}

// GetTabletCellBundles mocks base method.
func (m *MockCluster) GetTabletCellBundles(n *ytsys.Node) (*discovery.CellBundles, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTabletCellBundles", n)
	ret0, _ := ret[0].(*discovery.CellBundles)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTabletCellBundles indicates an expected call of GetTabletCellBundles.
func (mr *MockClusterMockRecorder) GetTabletCellBundles(n interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTabletCellBundles", reflect.TypeOf((*MockCluster)(nil).GetTabletCellBundles), n)
}

// GetTabletCommonNodeCount mocks base method.
func (m *MockCluster) GetTabletCommonNodeCount() (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTabletCommonNodeCount")
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTabletCommonNodeCount indicates an expected call of GetTabletCommonNodeCount.
func (mr *MockClusterMockRecorder) GetTabletCommonNodeCount() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTabletCommonNodeCount", reflect.TypeOf((*MockCluster)(nil).GetTabletCommonNodeCount))
}

// LastReloadTime mocks base method.
func (m *MockCluster) LastReloadTime() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LastReloadTime")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// LastReloadTime indicates an expected call of LastReloadTime.
func (mr *MockClusterMockRecorder) LastReloadTime() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LastReloadTime", reflect.TypeOf((*MockCluster)(nil).LastReloadTime))
}

// OnNodeDecommission mocks base method.
func (m *MockCluster) OnNodeDecommission(n *ytsys.Node) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "OnNodeDecommission", n)
}

// OnNodeDecommission indicates an expected call of OnNodeDecommission.
func (mr *MockClusterMockRecorder) OnNodeDecommission(n interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnNodeDecommission", reflect.TypeOf((*MockCluster)(nil).OnNodeDecommission), n)
}

// Reload mocks base method.
func (m *MockCluster) Reload(ctx context.Context, dc *ytsys.Client) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Reload", ctx, dc)
	ret0, _ := ret[0].(error)
	return ret0
}

// Reload indicates an expected call of Reload.
func (mr *MockClusterMockRecorder) Reload(ctx, dc interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reload", reflect.TypeOf((*MockCluster)(nil).Reload), ctx, dc)
}
