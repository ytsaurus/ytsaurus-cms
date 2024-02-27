package models

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/ytsys"
)

func TestSchedulerMap_CountProcessed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		m        SchedulerMap
		expected int
	}{
		{
			name: "simple",
			m: SchedulerMap{
				ytsys.Addr{FQDN: "s1", Port: "80"}: &Scheduler{
					Host:  "1.1.1.1",
					Addr:  &ytsys.Addr{FQDN: "s1", Port: "80"},
					State: SchedulerStateAccepted,
				},
				ytsys.Addr{FQDN: "s2", Port: "80"}: &Scheduler{
					Host:  "1.1.1.1",
					Addr:  &ytsys.Addr{FQDN: "s2", Port: "80"},
					State: SchedulerStateAccepted,
				},
				ytsys.Addr{FQDN: "s3", Port: "80"}: &Scheduler{
					Host:  "1.1.1.1",
					Addr:  &ytsys.Addr{FQDN: "s3", Port: "80"},
					State: SchedulerStateProcessed,
				},
				ytsys.Addr{FQDN: "s4", Port: "80"}: &Scheduler{
					Host:  "1.1.1.1",
					Addr:  &ytsys.Addr{FQDN: "s4", Port: "80"},
					State: SchedulerStateAccepted,
				},
			},
			expected: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.m.CountProcessed())
		})
	}
}

func TestControllerAgentMap_CountProcessed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		m        ControllerAgentMap
		expected int
	}{
		{
			name: "simple",
			m: ControllerAgentMap{
				ytsys.Addr{FQDN: "s1", Port: "80"}: &ControllerAgent{
					Host:  "1.1.1.1",
					Addr:  &ytsys.Addr{FQDN: "s1", Port: "80"},
					State: ControllerAgentStateAccepted,
				},
				ytsys.Addr{FQDN: "s2", Port: "80"}: &ControllerAgent{
					Host:  "1.1.1.1",
					Addr:  &ytsys.Addr{FQDN: "s2", Port: "80"},
					State: ControllerAgentStateAccepted,
				},
				ytsys.Addr{FQDN: "s3", Port: "80"}: &ControllerAgent{
					Host:  "1.1.1.1",
					Addr:  &ytsys.Addr{FQDN: "s3", Port: "80"},
					State: ControllerAgentStateProcessed,
				},
				ytsys.Addr{FQDN: "s4", Port: "80"}: &ControllerAgent{
					Host:  "1.1.1.1",
					Addr:  &ytsys.Addr{FQDN: "s4", Port: "80"},
					State: ControllerAgentStateAccepted,
				},
			},
			expected: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.m.CountProcessed())
		})
	}
}

func TestNewTaskCache(t *testing.T) {
	type expected struct {
		schedulerCount       int
		controllerAgentCount int
		masterCellCount      int
	}

	for _, tc := range []struct {
		name     string
		tasks    []*Task
		expected *expected
	}{
		{
			name:     "empty",
			tasks:    nil,
			expected: &expected{schedulerCount: 0, controllerAgentCount: 0, masterCellCount: 0},
		},
		{
			name: "simple",
			tasks: []*Task{
				{
					Task: &walle.Task{
						ID:     "production-123",
						Type:   walle.TaskTypeManual,
						Issuer: "test@",
						Action: walle.ActionReboot,
						Hosts:  []string{"1.1.1.1", "2.2.2.2"},
					},
					ProcessingState: StatePending,
					HostStates: map[string]*Host{
						"1.1.1.1": {
							Host:  "1.1.1.1",
							State: HostStateAccepted,
							Roles: map[ypath.Path]*Component{
								"//sys/scheduler/instances/scheduler-1:80": {
									Type: ytsys.RoleScheduler,
									Role: &Scheduler{
										Host:  "1.1.1.1",
										Addr:  &ytsys.Addr{FQDN: "scheduler-1", Port: "80"},
										Path:  "//sys/scheduler/instances/scheduler-1:80",
										State: SchedulerStateAccepted,
									},
								},
								"//sys/primary_masters/primary-master-1:80": {
									Type: ytsys.RolePrimaryMaster,
									Role: &Master{
										Host:  "1.1.1.1",
										Addr:  &ytsys.Addr{FQDN: "primary-master-1", Port: "80"},
										Path:  "//sys/primary_masters/primary-master-1:80",
										State: MasterStateAccepted,
									},
								},
								"//sys/controller_agents/instances/ca-1:80": {
									Type: ytsys.RoleControllerAgent,
									Role: &ControllerAgent{
										Host:  "1.1.1.1",
										Addr:  &ytsys.Addr{FQDN: "ca-1", Port: "80"},
										Path:  "//sys/controller_agents/instances/ca-1:80",
										State: ControllerAgentStateAccepted,
									},
								},
							},
						},
						"2.2.2.2": {
							Host:  "2.2.2.2",
							State: HostStateAccepted,
							Roles: map[ypath.Path]*Component{
								"//sys/scheduler/instances/scheduler-2:80": {
									Type: ytsys.RoleScheduler,
									Role: &Scheduler{
										Host:  "2.2.2.2",
										Addr:  &ytsys.Addr{FQDN: "scheduler-2", Port: "80"},
										Path:  "//sys/scheduler/instances/scheduler-2:80",
										State: SchedulerStateAccepted,
									},
								},
							},
						},
					},
					WalleStatus: walle.StatusInProcess,
				},
			},
			expected: &expected{schedulerCount: 2, controllerAgentCount: 1, masterCellCount: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewTaskCache(tc.tasks)
			require.NoError(t, err)
			require.Len(t, c.Schedulers, tc.expected.schedulerCount)
			require.Len(t, c.ControllerAgents, tc.expected.controllerAgentCount)
			require.Len(t, c.MasterCellTasks, tc.expected.masterCellCount)
		})
	}
}

func TestNewTaskCache_taskUpgrades(t *testing.T) {
	tasks := []*Task{
		{
			Task: &walle.Task{
				ID:     "production-123",
				Type:   walle.TaskTypeManual,
				Issuer: "test@",
				Action: walle.ActionReboot,
				Hosts:  []string{"1.1.1.1", "2.2.2.2"},
			},
			ProcessingState: StatePending,
			HostStates: map[string]*Host{
				"1.1.1.1": {
					Host:  "1.1.1.1",
					State: HostStateAccepted,
					Roles: map[ypath.Path]*Component{
						"//sys/primary_masters/primary-master-1:80": {
							Type: ytsys.RolePrimaryMaster,
							Role: &Master{
								Host:  "1.1.1.1",
								Addr:  &ytsys.Addr{FQDN: "primary-master-1", Port: "80"},
								Path:  "//sys/primary_masters/primary-master-1:80",
								State: MasterStateAccepted,
							},
						},
					},
				},
				"2.2.2.2": {
					Host:  "2.2.2.2",
					State: HostStateAccepted,
					Roles: map[ypath.Path]*Component{
						"//sys/scheduler/instances/scheduler-2:80": {
							Type: ytsys.RoleScheduler,
							Role: &Scheduler{
								Host:  "2.2.2.2",
								Addr:  &ytsys.Addr{FQDN: "scheduler-2", Port: "80"},
								Path:  "//sys/scheduler/instances/scheduler-2:80",
								State: SchedulerStateAccepted,
							},
						},
					},
				},
			},
			WalleStatus: walle.StatusInProcess,
		},
		{
			Task: &walle.Task{
				ID:     "production-124",
				Type:   walle.TaskTypeAutomated,
				Issuer: "wall-e",
				Action: walle.ActionReboot,
				Hosts:  []string{"2.2.2.2"},
			},
			ProcessingState: StatePending,
			HostStates: map[string]*Host{
				"2.2.2.2": {
					Host:  "2.2.2.2",
					State: HostStateAccepted,
					Roles: map[ypath.Path]*Component{
						"//sys/scheduler/instances/scheduler-2:80": {
							Type: ytsys.RoleScheduler,
							Role: &Scheduler{
								Host:  "2.2.2.2",
								Addr:  &ytsys.Addr{FQDN: "scheduler-2", Port: "80"},
								Path:  "//sys/scheduler/instances/scheduler-2:80",
								State: SchedulerStateAccepted,
							},
						},
					},
				},
			},
			WalleStatus: walle.StatusInProcess,
		},
	}

	cache, err := NewTaskCache(tasks)
	require.NoError(t, err)
	_, ok := cache.TaskUpgrades["1.1.1.1"]
	require.False(t, ok)

	u, ok := cache.TaskUpgrades["2.2.2.2"]
	require.True(t, ok)
	require.Equal(t, walle.TaskID("production-123"), u.Old.ID)
	require.Equal(t, walle.TaskID("production-124"), u.New.ID)
}
