package models

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/ytsys"
)

func TestTask_ConfirmManually(t *testing.T) {
	task := &Task{
		Task: &walle.Task{
			ID:     "production-123",
			Type:   walle.TaskTypeManual,
			Issuer: "test@",
			Action: walle.ActionReboot,
			Hosts:  []string{"1.1.1.1", "2.2.2.2"},
		},
		Origin:          OriginWalle,
		ProcessingState: StatePending,
		WalleStatus:     walle.StatusInProcess,
	}

	task.ConfirmManually("test@")
	require.Equal(t, StateConfirmedManually, task.ProcessingState)
	require.NotZero(t, task.ProcessedAt)
	require.Equal(t, DecisionMaker("test@"), task.DecisionMaker)
}

func TestTask_state(t *testing.T) {
	task := &Task{
		Task: &walle.Task{
			ID:     "production-123",
			Type:   walle.TaskTypeManual,
			Issuer: "test@",
			Action: walle.ActionReboot,
			Hosts:  []string{"1.1.1.1", "2.2.2.2"},
		},
		Origin:          OriginWalle,
		ProcessingState: StatePending,
		HostStates: map[string]*Host{
			"1.1.1.1": {
				Host:  "1.1.1.1",
				State: HostStateAccepted,
			},
			"2.2.2.2": {
				Host:  "2.2.2.2",
				State: HostStateAccepted,
			},
		},
		WalleStatus: walle.StatusInProcess,
	}

	require.False(t, task.AllHostsDecommissioned())

	for _, h := range task.HostStates {
		h.SetDecommissioned()
	}
	task.SetDecommissioned()

	require.Equal(t, StateDecommissioned, task.ProcessingState)
	require.True(t, task.AllHostsDecommissioned())
	require.False(t, task.AllHostsProcessed())

	for _, h := range task.HostStates {
		h.SetProcessed()
	}
	task.SetProcessed()

	require.True(t, task.AllHostsProcessed())
	require.False(t, task.AllHostsFinished())
	require.Equal(t, StateProcessed, task.ProcessingState)
	require.NotZero(t, task.ProcessedAt)

	for _, h := range task.HostStates {
		h.SetFinished()
	}
	task.SetFinished()

	require.True(t, task.AllHostsFinished())
	require.Equal(t, StateFinished, task.ProcessingState)
	require.NotZero(t, task.FinishedAt)
}

func TestTask_GetMasters(t *testing.T) {
	for _, tc := range []struct {
		name    string
		task    *Task
		masters map[string]struct{}
	}{
		{
			name: "no-masters",
			task: &Task{
				HostStates: map[string]*Host{
					"h1.yandex.net": {
						Host: "h1.yandex.net",
						Roles: map[ypath.Path]*Component{
							"//sys/cluster_nodes/n1.yandex.net:9012": {
								Type: ytsys.RoleNode,
								Role: &Node{
									Host: "h1.yandex.net",
									Addr: &ytsys.Addr{FQDN: "n1.yandex.net", Port: "9012"},
								},
							},
						},
					},
				},
			},
			masters: nil,
		},
		{
			name: "single-master",
			task: &Task{
				HostStates: map[string]*Host{
					"h1.yandex.net": {
						Host: "h1.yandex.net",
						Roles: map[ypath.Path]*Component{
							"//sys/primary_masters/m1.yandex.net:9010": {
								Type: ytsys.RoleSecondaryMaster,
								Role: &Master{
									Host: "h1.yandex.net",
									Addr: &ytsys.Addr{FQDN: "m1.yandex.net", Port: "9010"},
								},
							},
							"//sys/cluster_nodes/n1.yandex.net:9012": {
								Type: ytsys.RoleNode,
								Role: &Node{
									Host: "h1.yandex.net",
									Addr: &ytsys.Addr{FQDN: "n1.yandex.net", Port: "9012"},
								},
							},
						},
					},
				},
			},
			masters: map[string]struct{}{"m1.yandex.net:9010": {}},
		},
		{
			name: "timestamp-provider",
			task: &Task{
				HostStates: map[string]*Host{
					"c1.yandex.net": {
						Host: "c1.yandex.net",
						Roles: map[ypath.Path]*Component{
							"//sys/timestamp_providers/c1.yandex.net:9016": {
								Type: ytsys.RoleTimestampProvider,
								Role: &Master{
									Host: "c1.yandex.net",
									Addr: &ytsys.Addr{FQDN: "c1.yandex.net", Port: "9016"},
								},
							},
						},
					},
				},
			},
			masters: map[string]struct{}{"c1.yandex.net:9016": {}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			masters := tc.task.GetMasters()
			require.Len(t, masters, len(tc.masters))

			for _, m := range masters {
				require.Contains(t, tc.masters, m.Addr.String())
			}

			if len(tc.masters) > 0 {
				require.True(t, tc.task.HasMasterRole())
			} else {
				require.False(t, tc.task.HasMasterRole())
			}
		})
	}
}

func TestTask_GetNodes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		task  *Task
		nodes map[string]struct{}
	}{
		{
			name: "no-nodes",
			task: &Task{
				HostStates: map[string]*Host{
					"man1-6063.search.yandex.net": {
						Host: "h1.yandex.net",
						Roles: map[ypath.Path]*Component{
							"//sys/primary_masters/m1.yandex.net:9010": {
								Type: ytsys.RoleSecondaryMaster,
								Role: &Master{
									Host: "h1.yandex.net",
									Addr: &ytsys.Addr{FQDN: "m1.yandex.net", Port: "9010"},
								},
							},
						},
					},
				},
			},
			nodes: nil,
		},
		{
			name: "single-node",
			task: &Task{
				HostStates: map[string]*Host{
					"h1.yandex.net": {
						Host: "h1.yandex.net",
						Roles: map[ypath.Path]*Component{
							"//sys/primary_masters/m1.yandex.net:9010": {
								Type: ytsys.RoleSecondaryMaster,
								Role: &Master{
									Host: "h1.yandex.net",
									Addr: &ytsys.Addr{FQDN: "m1.yandex.net", Port: "9010"},
								},
							},
							"//sys/cluster_nodes/n1.yandex.net:9012": {
								Type: ytsys.RoleNode,
								Role: &Node{
									Host: "h1.yandex.net",
									Addr: &ytsys.Addr{FQDN: "n1.yandex.net", Port: "9012"},
								},
							},
						},
					},
				},
			},
			nodes: map[string]struct{}{"n1.yandex.net:9012": {}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := tc.task.GetNodes()
			require.Len(t, nodes, len(tc.nodes))

			for _, n := range nodes {
				require.Contains(t, tc.nodes, n.Addr.String())
			}
		})
	}
}

func TestTaskGroup(t *testing.T) {
	var g TaskGroup

	require.Empty(t, g.GetGroupID())

	g = append(g, &Task{Task: &walle.Task{ID: "production-1", MaintenanceInfo: &walle.MaintenanceInfo{NodeSetID: "1234:1"}}})
	g = append(g, &Task{Task: &walle.Task{ID: "production-2", MaintenanceInfo: &walle.MaintenanceInfo{NodeSetID: "1234:1"}}})
	g = append(g, &Task{Task: &walle.Task{ID: "production-3", MaintenanceInfo: &walle.MaintenanceInfo{NodeSetID: "1234:1"}}})

	require.Equal(t, "1234:1", g.GetGroupID())
}
