package models

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/ytsys"
)

func TestHost_UpdateRoles(t *testing.T) {
	for _, tc := range []struct {
		name       string
		h          *Host
		components discovery.Components
		expected   map[ypath.Path]*Component
	}{
		{
			name: "no-new-roles",
			h: &Host{
				Host:  "1.1.1.1",
				State: HostStateAccepted,
				Roles: map[ypath.Path]*Component{
					"//sys/cluster_nodes/node-1:80": {
						Type: ytsys.RoleNode,
						Role: &Node{
							Host:  "1.1.1.1",
							Addr:  &ytsys.Addr{FQDN: "node-1", Port: "80"},
							State: NodeStateAccepted,
						},
					},
				},
			},
			components: discovery.Components{
				"//sys/cluster_nodes/node-1:80": &ytsys.Node{
					Addr:        &ytsys.Addr{FQDN: "node-1", Port: "80"},
					Annotations: &ytsys.Annotations{PhysicalHost: "1.1.1.1"},
				},
			},
			expected: map[ypath.Path]*Component{
				"//sys/cluster_nodes/node-1:80": {
					Type: ytsys.RoleNode,
					Role: &Node{
						Host:  "1.1.1.1",
						Addr:  &ytsys.Addr{FQDN: "node-1", Port: "80"},
						State: NodeStateAccepted,
					},
				},
			},
		},
		{
			name: "new-role",
			h: &Host{
				Host:  "1.1.1.1",
				State: HostStateAccepted,
				Roles: map[ypath.Path]*Component{
					"//sys/cluster_nodes/node-1:80": {
						Type: ytsys.RoleNode,
						Role: &Node{
							Host:  "1.1.1.1",
							Addr:  &ytsys.Addr{FQDN: "node-1", Port: "80"},
							State: NodeStateAccepted,
						},
					},
				},
			},
			components: discovery.Components{
				"//sys/cluster_nodes/node-1:80": &ytsys.Node{
					Addr:        &ytsys.Addr{FQDN: "node-1", Port: "80"},
					Annotations: &ytsys.Annotations{PhysicalHost: "1.1.1.1"},
				},
				"//sys/rpc_proxies/rpc-proxy-1:80": &ytsys.Node{
					Addr:        &ytsys.Addr{FQDN: "rpc-proxy-1", Port: "80"},
					Annotations: &ytsys.Annotations{PhysicalHost: "1.1.1.1"},
				},
			},
			expected: map[ypath.Path]*Component{
				"//sys/cluster_nodes/node-1:80": {
					Type: ytsys.RoleNode,
					Role: &Node{
						Host:  "1.1.1.1",
						Addr:  &ytsys.Addr{FQDN: "node-1", Port: "80"},
						State: NodeStateAccepted,
					},
				},
				"//sys/rpc_proxies/rpc-proxy-1:80": {
					Type: ytsys.RoleNode,
					Role: &RPCProxy{
						Host:  "1.1.1.1",
						Addr:  &ytsys.Addr{FQDN: "node-1", Port: "80"},
						State: RPCProxyStateAccepted,
					},
				},
			},
		},
		{
			name: "remove-role",
			h: &Host{
				Host:  "1.1.1.1",
				State: HostStateAccepted,
				Roles: map[ypath.Path]*Component{
					"//sys/cluster_nodes/node-1:80": {
						Type: ytsys.RoleNode,
						Role: &Node{
							Host:  "1.1.1.1",
							Addr:  &ytsys.Addr{FQDN: "node-1", Port: "80"},
							State: NodeStateAccepted,
						},
					},
					"//sys/rpc_proxies/rpc-proxy-1:80": {
						Type: ytsys.RoleRPCProxy,
						Role: &RPCProxy{
							Host:  "1.1.1.1",
							Addr:  &ytsys.Addr{FQDN: "rpc-proxy-1", Port: "80"},
							State: RPCProxyStateAccepted,
						},
					},
				},
			},
			components: discovery.Components{
				"//sys/cluster_nodes/node-1:80": &ytsys.Node{
					Addr:        &ytsys.Addr{FQDN: "node-1", Port: "80"},
					Annotations: &ytsys.Annotations{PhysicalHost: "1.1.1.1"},
				},
			},
			expected: map[ypath.Path]*Component{
				"//sys/cluster_nodes/node-1:80": {
					Type: ytsys.RoleNode,
					Role: &Node{
						Host:  "1.1.1.1",
						Addr:  &ytsys.Addr{FQDN: "node-1", Port: "80"},
						State: NodeStateAccepted,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.h.UpdateRoles(tc.components)

			compareRoleSets(t, tc.expected, tc.h.Roles)
		})
	}
}

func compareRoleSets(t *testing.T, expected, actual map[ypath.Path]*Component) {
	t.Helper()

	require.Equal(t, len(expected), len(actual))
	for addr, c := range actual {
		require.Contains(t, expected, addr)

		require.Equal(t, expected[addr].Type, c.Type)
	}
}

func TestHost_state(t *testing.T) {
	h := &Host{
		Host:  "1.1.1.1",
		State: HostStateAccepted,
		Roles: map[ypath.Path]*Component{
			"//sys/cluster_nodes/node-1:9010": {
				Type: ytsys.RoleNode,
				Role: &Node{State: NodeStateAccepted},
			},
			"//sys/cluster_nodes/node-2:9010": {
				Type: ytsys.RoleNode,
				Role: &Node{State: NodeStateAccepted},
			},
		},
	}

	require.False(t, h.AllRolesDecommissioned())

	for _, r := range h.Roles {
		r.Role.(*Node).SetDecommissioned()
	}
	h.SetDecommissioned()

	require.Equal(t, HostStateDecommissioned, h.State)
	require.True(t, h.AllRolesDecommissioned())
	require.False(t, h.AllRolesProcessed())

	for _, r := range h.Roles {
		r.Role.(*Node).AllowWalle()
	}
	h.SetProcessed()

	require.Equal(t, HostStateProcessed, h.State)
	require.True(t, h.AllRolesProcessed())
	require.False(t, h.AllRolesFinished())

	for _, r := range h.Roles {
		r.Role.(*Node).SetFinished()
	}
	h.SetFinished()

	require.Equal(t, HostStateFinished, h.State)
	require.True(t, h.AllRolesFinished())
}

func TestHost_AllowAsForeign(t *testing.T) {
	h := &Host{
		Host:  "1.1.1.1",
		State: HostStateAccepted,
	}

	h.AllowAsForeign()
	require.Equal(t, HostStateProcessed, h.State)
}

func TestHost_AddTicket(t *testing.T) {
	h := &Host{}
	require.False(t, h.AddTicket(""))
	require.True(t, h.AddTicket("YTADMINREQ-1"))
	require.False(t, h.AddTicket("YTADMINREQ-1"))
	require.True(t, h.AddTicket("YTADMINREQ-2"))
}
