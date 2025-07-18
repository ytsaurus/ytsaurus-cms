// Code generated by MockGen. DO NOT EDIT.
// Source: go.ytsaurus.tech/yt/admin/cms/internal/cms (interfaces: DiscoveryClient)
//
// Generated by this command:
//
//	mockgen -destination=discovery_client_mock.go -package cms . DiscoveryClient
//

// Package cms is a generated GoMock package.
package cms

import (
	context "context"
	reflect "reflect"

	guid "go.ytsaurus.tech/yt/go/guid"
	ypath "go.ytsaurus.tech/yt/go/ypath"
	yson "go.ytsaurus.tech/yt/go/yson"
	yt "go.ytsaurus.tech/yt/go/yt"
	ytsys "go.ytsaurus.tech/yt/go/ytsys"
	gomock "go.uber.org/mock/gomock"
)

// MockDiscoveryClient is a mock of DiscoveryClient interface.
type MockDiscoveryClient struct {
	ctrl     *gomock.Controller
	recorder *MockDiscoveryClientMockRecorder
	isgomock struct{}
}

// MockDiscoveryClientMockRecorder is the mock recorder for MockDiscoveryClient.
type MockDiscoveryClientMockRecorder struct {
	mock *MockDiscoveryClient
}

// NewMockDiscoveryClient creates a new mock instance.
func NewMockDiscoveryClient(ctrl *gomock.Controller) *MockDiscoveryClient {
	mock := &MockDiscoveryClient{ctrl: ctrl}
	mock.recorder = &MockDiscoveryClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDiscoveryClient) EXPECT() *MockDiscoveryClientMockRecorder {
	return m.recorder
}

// AddMaintenance mocks base method.
func (m *MockDiscoveryClient) AddMaintenance(ctx context.Context, component yt.MaintenanceComponent, addr *ytsys.Addr, maintenanceType yt.MaintenanceType, comment string, opts *yt.AddMaintenanceOptions) (*yt.MaintenanceID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddMaintenance", ctx, component, addr, maintenanceType, comment, opts)
	ret0, _ := ret[0].(*yt.MaintenanceID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddMaintenance indicates an expected call of AddMaintenance.
func (mr *MockDiscoveryClientMockRecorder) AddMaintenance(ctx, component, addr, maintenanceType, comment, opts any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddMaintenance", reflect.TypeOf((*MockDiscoveryClient)(nil).AddMaintenance), ctx, component, addr, maintenanceType, comment, opts)
}

// Ban mocks base method.
func (m *MockDiscoveryClient) Ban(ctx context.Context, c ytsys.Component, banMsg string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ban", ctx, c, banMsg)
	ret0, _ := ret[0].(error)
	return ret0
}

// Ban indicates an expected call of Ban.
func (mr *MockDiscoveryClientMockRecorder) Ban(ctx, c, banMsg any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ban", reflect.TypeOf((*MockDiscoveryClient)(nil).Ban), ctx, c, banMsg)
}

// DestroyChunkLocations mocks base method.
func (m *MockDiscoveryClient) DestroyChunkLocations(ctx context.Context, addr *ytsys.Addr, recoverUnlinkedDisks bool, uuids []guid.GUID) ([]guid.GUID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DestroyChunkLocations", ctx, addr, recoverUnlinkedDisks, uuids)
	ret0, _ := ret[0].([]guid.GUID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DestroyChunkLocations indicates an expected call of DestroyChunkLocations.
func (mr *MockDiscoveryClientMockRecorder) DestroyChunkLocations(ctx, addr, recoverUnlinkedDisks, uuids any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DestroyChunkLocations", reflect.TypeOf((*MockDiscoveryClient)(nil).DestroyChunkLocations), ctx, addr, recoverUnlinkedDisks, uuids)
}

// DisableChunkLocations mocks base method.
func (m *MockDiscoveryClient) DisableChunkLocations(ctx context.Context, addr *ytsys.Addr, uuids []guid.GUID) ([]guid.GUID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DisableChunkLocations", ctx, addr, uuids)
	ret0, _ := ret[0].([]guid.GUID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DisableChunkLocations indicates an expected call of DisableChunkLocations.
func (mr *MockDiscoveryClientMockRecorder) DisableChunkLocations(ctx, addr, uuids any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DisableChunkLocations", reflect.TypeOf((*MockDiscoveryClient)(nil).DisableChunkLocations), ctx, addr, uuids)
}

// DisableSchedulerJobs mocks base method.
func (m *MockDiscoveryClient) DisableSchedulerJobs(ctx context.Context, addr *ytsys.Addr) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DisableSchedulerJobs", ctx, addr)
	ret0, _ := ret[0].(error)
	return ret0
}

// DisableSchedulerJobs indicates an expected call of DisableSchedulerJobs.
func (mr *MockDiscoveryClientMockRecorder) DisableSchedulerJobs(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DisableSchedulerJobs", reflect.TypeOf((*MockDiscoveryClient)(nil).DisableSchedulerJobs), ctx, addr)
}

// DisableWriteSessions mocks base method.
func (m *MockDiscoveryClient) DisableWriteSessions(ctx context.Context, addr *ytsys.Addr) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DisableWriteSessions", ctx, addr)
	ret0, _ := ret[0].(error)
	return ret0
}

// DisableWriteSessions indicates an expected call of DisableWriteSessions.
func (mr *MockDiscoveryClientMockRecorder) DisableWriteSessions(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DisableWriteSessions", reflect.TypeOf((*MockDiscoveryClient)(nil).DisableWriteSessions), ctx, addr)
}

// DropRemovalSlotsOverride mocks base method.
func (m *MockDiscoveryClient) DropRemovalSlotsOverride(ctx context.Context, addr *ytsys.Addr) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DropRemovalSlotsOverride", ctx, addr)
	ret0, _ := ret[0].(error)
	return ret0
}

// DropRemovalSlotsOverride indicates an expected call of DropRemovalSlotsOverride.
func (mr *MockDiscoveryClientMockRecorder) DropRemovalSlotsOverride(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DropRemovalSlotsOverride", reflect.TypeOf((*MockDiscoveryClient)(nil).DropRemovalSlotsOverride), ctx, addr)
}

// EnableSchedulerJobs mocks base method.
func (m *MockDiscoveryClient) EnableSchedulerJobs(ctx context.Context, addr *ytsys.Addr) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EnableSchedulerJobs", ctx, addr)
	ret0, _ := ret[0].(error)
	return ret0
}

// EnableSchedulerJobs indicates an expected call of EnableSchedulerJobs.
func (mr *MockDiscoveryClientMockRecorder) EnableSchedulerJobs(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnableSchedulerJobs", reflect.TypeOf((*MockDiscoveryClient)(nil).EnableSchedulerJobs), ctx, addr)
}

// EnableWriteSessions mocks base method.
func (m *MockDiscoveryClient) EnableWriteSessions(ctx context.Context, addr *ytsys.Addr) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EnableWriteSessions", ctx, addr)
	ret0, _ := ret[0].(error)
	return ret0
}

// EnableWriteSessions indicates an expected call of EnableWriteSessions.
func (mr *MockDiscoveryClientMockRecorder) EnableWriteSessions(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnableWriteSessions", reflect.TypeOf((*MockDiscoveryClient)(nil).EnableWriteSessions), ctx, addr)
}

// GetDiskIDsMismatched mocks base method.
func (m *MockDiscoveryClient) GetDiskIDsMismatched(ctx context.Context, node *ytsys.Node) (*bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDiskIDsMismatched", ctx, node)
	ret0, _ := ret[0].(*bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDiskIDsMismatched indicates an expected call of GetDiskIDsMismatched.
func (mr *MockDiscoveryClientMockRecorder) GetDiskIDsMismatched(ctx, node any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDiskIDsMismatched", reflect.TypeOf((*MockDiscoveryClient)(nil).GetDiskIDsMismatched), ctx, node)
}

// GetHydraState mocks base method.
func (m *MockDiscoveryClient) GetHydraState(ctx context.Context, path ypath.Path) (*ytsys.HydraState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHydraState", ctx, path)
	ret0, _ := ret[0].(*ytsys.HydraState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetHydraState indicates an expected call of GetHydraState.
func (mr *MockDiscoveryClientMockRecorder) GetHydraState(ctx, path any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHydraState", reflect.TypeOf((*MockDiscoveryClient)(nil).GetHydraState), ctx, path)
}

// GetIntegrityIndicators mocks base method.
func (m *MockDiscoveryClient) GetIntegrityIndicators(ctx context.Context) (*ytsys.ChunkIntegrity, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetIntegrityIndicators", ctx)
	ret0, _ := ret[0].(*ytsys.ChunkIntegrity)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetIntegrityIndicators indicates an expected call of GetIntegrityIndicators.
func (mr *MockDiscoveryClientMockRecorder) GetIntegrityIndicators(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetIntegrityIndicators", reflect.TypeOf((*MockDiscoveryClient)(nil).GetIntegrityIndicators), ctx)
}

// GetNodeChunks mocks base method.
func (m *MockDiscoveryClient) GetNodeChunks(ctx context.Context, addr *ytsys.Addr, opts *ytsys.GetNodeChunksOption) ([]*ytsys.Chunk, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNodeChunks", ctx, addr, opts)
	ret0, _ := ret[0].([]*ytsys.Chunk)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNodeChunks indicates an expected call of GetNodeChunks.
func (mr *MockDiscoveryClientMockRecorder) GetNodeChunks(ctx, addr, opts any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNodeChunks", reflect.TypeOf((*MockDiscoveryClient)(nil).GetNodeChunks), ctx, addr, opts)
}

// GetNodeStartTime mocks base method.
func (m *MockDiscoveryClient) GetNodeStartTime(ctx context.Context, node *ytsys.Node) (*yson.Time, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNodeStartTime", ctx, node)
	ret0, _ := ret[0].(*yson.Time)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNodeStartTime indicates an expected call of GetNodeStartTime.
func (mr *MockDiscoveryClientMockRecorder) GetNodeStartTime(ctx, node any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNodeStartTime", reflect.TypeOf((*MockDiscoveryClient)(nil).GetNodeStartTime), ctx, node)
}

// GetReservePoolCMSLimits mocks base method.
func (m *MockDiscoveryClient) GetReservePoolCMSLimits(ctx context.Context, path ypath.Path) (*ytsys.ReservePoolCMSLimits, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetReservePoolCMSLimits", ctx, path)
	ret0, _ := ret[0].(*ytsys.ReservePoolCMSLimits)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetReservePoolCMSLimits indicates an expected call of GetReservePoolCMSLimits.
func (mr *MockDiscoveryClientMockRecorder) GetReservePoolCMSLimits(ctx, path any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReservePoolCMSLimits", reflect.TypeOf((*MockDiscoveryClient)(nil).GetReservePoolCMSLimits), ctx, path)
}

// GetStrongGuaranteeResources mocks base method.
func (m *MockDiscoveryClient) GetStrongGuaranteeResources(ctx context.Context, path ypath.Path) (*ytsys.StrongGuaranteeResources, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStrongGuaranteeResources", ctx, path)
	ret0, _ := ret[0].(*ytsys.StrongGuaranteeResources)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStrongGuaranteeResources indicates an expected call of GetStrongGuaranteeResources.
func (mr *MockDiscoveryClientMockRecorder) GetStrongGuaranteeResources(ctx, path any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStrongGuaranteeResources", reflect.TypeOf((*MockDiscoveryClient)(nil).GetStrongGuaranteeResources), ctx, path)
}

// HasMaintenanceAttr mocks base method.
func (m *MockDiscoveryClient) HasMaintenanceAttr(ctx context.Context, c ytsys.Component) (ytsys.YTBool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasMaintenanceAttr", ctx, c)
	ret0, _ := ret[0].(ytsys.YTBool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HasMaintenanceAttr indicates an expected call of HasMaintenanceAttr.
func (mr *MockDiscoveryClientMockRecorder) HasMaintenanceAttr(ctx, c any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasMaintenanceAttr", reflect.TypeOf((*MockDiscoveryClient)(nil).HasMaintenanceAttr), ctx, c)
}

// ListNodes mocks base method.
func (m *MockDiscoveryClient) ListNodes(ctx context.Context, attrs []string) ([]*ytsys.Node, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListNodes", ctx, attrs)
	ret0, _ := ret[0].([]*ytsys.Node)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListNodes indicates an expected call of ListNodes.
func (mr *MockDiscoveryClientMockRecorder) ListNodes(ctx, attrs any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListNodes", reflect.TypeOf((*MockDiscoveryClient)(nil).ListNodes), ctx, attrs)
}

// MarkNodeDecommissioned mocks base method.
func (m *MockDiscoveryClient) MarkNodeDecommissioned(ctx context.Context, addr *ytsys.Addr, msg string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MarkNodeDecommissioned", ctx, addr, msg)
	ret0, _ := ret[0].(error)
	return ret0
}

// MarkNodeDecommissioned indicates an expected call of MarkNodeDecommissioned.
func (mr *MockDiscoveryClientMockRecorder) MarkNodeDecommissioned(ctx, addr, msg any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkNodeDecommissioned", reflect.TypeOf((*MockDiscoveryClient)(nil).MarkNodeDecommissioned), ctx, addr, msg)
}

// OverrideRemovalSlots mocks base method.
func (m *MockDiscoveryClient) OverrideRemovalSlots(ctx context.Context, addr *ytsys.Addr, slots int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OverrideRemovalSlots", ctx, addr, slots)
	ret0, _ := ret[0].(error)
	return ret0
}

// OverrideRemovalSlots indicates an expected call of OverrideRemovalSlots.
func (mr *MockDiscoveryClientMockRecorder) OverrideRemovalSlots(ctx, addr, slots any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OverrideRemovalSlots", reflect.TypeOf((*MockDiscoveryClient)(nil).OverrideRemovalSlots), ctx, addr, slots)
}

// PathExists mocks base method.
func (m *MockDiscoveryClient) PathExists(ctx context.Context, path ypath.Path) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PathExists", ctx, path)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PathExists indicates an expected call of PathExists.
func (mr *MockDiscoveryClientMockRecorder) PathExists(ctx, path any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PathExists", reflect.TypeOf((*MockDiscoveryClient)(nil).PathExists), ctx, path)
}

// RemoveMaintenance mocks base method.
func (m *MockDiscoveryClient) RemoveMaintenance(ctx context.Context, component yt.MaintenanceComponent, addr *ytsys.Addr, opts *yt.RemoveMaintenanceOptions) (*yt.RemoveMaintenanceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveMaintenance", ctx, component, addr, opts)
	ret0, _ := ret[0].(*yt.RemoveMaintenanceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RemoveMaintenance indicates an expected call of RemoveMaintenance.
func (mr *MockDiscoveryClientMockRecorder) RemoveMaintenance(ctx, component, addr, opts any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveMaintenance", reflect.TypeOf((*MockDiscoveryClient)(nil).RemoveMaintenance), ctx, component, addr, opts)
}

// RemoveTag mocks base method.
func (m *MockDiscoveryClient) RemoveTag(ctx context.Context, path ypath.Path, tag string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveTag", ctx, path, tag)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveTag indicates an expected call of RemoveTag.
func (mr *MockDiscoveryClientMockRecorder) RemoveTag(ctx, path, tag any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveTag", reflect.TypeOf((*MockDiscoveryClient)(nil).RemoveTag), ctx, path, tag)
}

// RequestRestart mocks base method.
func (m *MockDiscoveryClient) RequestRestart(ctx context.Context, addr *ytsys.Addr) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RequestRestart", ctx, addr)
	ret0, _ := ret[0].(error)
	return ret0
}

// RequestRestart indicates an expected call of RequestRestart.
func (mr *MockDiscoveryClientMockRecorder) RequestRestart(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RequestRestart", reflect.TypeOf((*MockDiscoveryClient)(nil).RequestRestart), ctx, addr)
}

// ResurrectChunkLocations mocks base method.
func (m *MockDiscoveryClient) ResurrectChunkLocations(ctx context.Context, addr *ytsys.Addr, uuids []guid.GUID) ([]guid.GUID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResurrectChunkLocations", ctx, addr, uuids)
	ret0, _ := ret[0].([]guid.GUID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResurrectChunkLocations indicates an expected call of ResurrectChunkLocations.
func (mr *MockDiscoveryClientMockRecorder) ResurrectChunkLocations(ctx, addr, uuids any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResurrectChunkLocations", reflect.TypeOf((*MockDiscoveryClient)(nil).ResurrectChunkLocations), ctx, addr, uuids)
}

// SetMaintenance mocks base method.
func (m *MockDiscoveryClient) SetMaintenance(ctx context.Context, c ytsys.Component, req *ytsys.MaintenanceRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetMaintenance", ctx, c, req)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetMaintenance indicates an expected call of SetMaintenance.
func (mr *MockDiscoveryClientMockRecorder) SetMaintenance(ctx, c, req any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetMaintenance", reflect.TypeOf((*MockDiscoveryClient)(nil).SetMaintenance), ctx, c, req)
}

// SetStrongGuaranteeCPU mocks base method.
func (m *MockDiscoveryClient) SetStrongGuaranteeCPU(ctx context.Context, path ypath.Path, guarantee float64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetStrongGuaranteeCPU", ctx, path, guarantee)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetStrongGuaranteeCPU indicates an expected call of SetStrongGuaranteeCPU.
func (mr *MockDiscoveryClientMockRecorder) SetStrongGuaranteeCPU(ctx, path, guarantee any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetStrongGuaranteeCPU", reflect.TypeOf((*MockDiscoveryClient)(nil).SetStrongGuaranteeCPU), ctx, path, guarantee)
}

// SetTag mocks base method.
func (m *MockDiscoveryClient) SetTag(ctx context.Context, path ypath.Path, tag string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetTag", ctx, path, tag)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetTag indicates an expected call of SetTag.
func (mr *MockDiscoveryClientMockRecorder) SetTag(ctx, path, tag any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTag", reflect.TypeOf((*MockDiscoveryClient)(nil).SetTag), ctx, path, tag)
}

// Unban mocks base method.
func (m *MockDiscoveryClient) Unban(ctx context.Context, c ytsys.Component) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Unban", ctx, c)
	ret0, _ := ret[0].(error)
	return ret0
}

// Unban indicates an expected call of Unban.
func (mr *MockDiscoveryClientMockRecorder) Unban(ctx, c any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unban", reflect.TypeOf((*MockDiscoveryClient)(nil).Unban), ctx, c)
}

// UnmarkNodeDecommissioned mocks base method.
func (m *MockDiscoveryClient) UnmarkNodeDecommissioned(ctx context.Context, addr *ytsys.Addr) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnmarkNodeDecommissioned", ctx, addr)
	ret0, _ := ret[0].(error)
	return ret0
}

// UnmarkNodeDecommissioned indicates an expected call of UnmarkNodeDecommissioned.
func (mr *MockDiscoveryClientMockRecorder) UnmarkNodeDecommissioned(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnmarkNodeDecommissioned", reflect.TypeOf((*MockDiscoveryClient)(nil).UnmarkNodeDecommissioned), ctx, addr)
}

// UnsetMaintenance mocks base method.
func (m *MockDiscoveryClient) UnsetMaintenance(ctx context.Context, c ytsys.Component, requestID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnsetMaintenance", ctx, c, requestID)
	ret0, _ := ret[0].(error)
	return ret0
}

// UnsetMaintenance indicates an expected call of UnsetMaintenance.
func (mr *MockDiscoveryClientMockRecorder) UnsetMaintenance(ctx, c, requestID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnsetMaintenance", reflect.TypeOf((*MockDiscoveryClient)(nil).UnsetMaintenance), ctx, c, requestID)
}
