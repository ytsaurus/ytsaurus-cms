// Code generated by MockGen. DO NOT EDIT.
// Source: go.ytsaurus.tech/yt/admin/cms/internal/cms (interfaces: YTProxyCache)

// Package cms is a generated GoMock package.
package cms

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockYTProxyCache is a mock of YTProxyCache interface.
type MockYTProxyCache struct {
	ctrl     *gomock.Controller
	recorder *MockYTProxyCacheMockRecorder
}

// MockYTProxyCacheMockRecorder is the mock recorder for MockYTProxyCache.
type MockYTProxyCacheMockRecorder struct {
	mock *MockYTProxyCache
}

// NewMockYTProxyCache creates a new mock instance.
func NewMockYTProxyCache(ctrl *gomock.Controller) *MockYTProxyCache {
	mock := &MockYTProxyCache{ctrl: ctrl}
	mock.recorder = &MockYTProxyCacheMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockYTProxyCache) EXPECT() *MockYTProxyCacheMockRecorder {
	return m.recorder
}

// GetProxies mocks base method.
func (m *MockYTProxyCache) GetProxies() YTProxyMap {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetProxies")
	ret0, _ := ret[0].(YTProxyMap)
	return ret0
}

// GetProxies indicates an expected call of GetProxies.
func (mr *MockYTProxyCacheMockRecorder) GetProxies() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetProxies", reflect.TypeOf((*MockYTProxyCache)(nil).GetProxies))
}
