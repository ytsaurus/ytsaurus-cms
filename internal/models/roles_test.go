package models

import (
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/admin/cms/internal/startrek"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytsys"
)

func TestComponent_UnmarshalYSON(t *testing.T) {
	for _, tc := range []struct {
		c ytsys.Component
	}{
		{
			c: &ytsys.PrimaryMaster{
				Addr:        &ytsys.Addr{FQDN: "primary-master", Port: "9012"},
				ID:          yt.NodeID(guid.New()),
				Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
			},
		},
		{
			c: &ytsys.SecondaryMaster{
				Addr:        &ytsys.Addr{FQDN: "secondary-master", Port: "9012"},
				ID:          yt.NodeID(guid.New()),
				Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
			},
		},
		{
			c: &ytsys.TimestampProvider{
				Addr:        &ytsys.Addr{FQDN: "timestamp-provider", Port: "9016"},
				ID:          yt.NodeID(guid.New()),
				Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
			},
		},
		{
			c: &ytsys.HTTPProxy{
				Addr:        &ytsys.Addr{FQDN: "ca", Port: "9012"},
				ID:          yt.NodeID(guid.New()),
				Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
				Role:        "data",
				Banned:      false,
				BanMessage:  "",
				Liveness:    &ytsys.HTTPProxyLiveness{UpdatedAt: yson.Time(time.Now().UTC())},
			},
		},
		{
			c: &ytsys.RPCProxy{
				Addr:        &ytsys.Addr{FQDN: "ca", Port: "9012"},
				ID:          yt.NodeID(guid.New()),
				Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
				Role:        "yttank",
				Banned:      false,
				BanMessage:  "",
				Alive:       new(map[string]interface{}),
			},
		},
		{
			c: &ytsys.Node{
				Addr:         &ytsys.Addr{FQDN: "abc", Port: "9012"},
				Annotations:  &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
				ID:           yt.NodeID(guid.New()),
				LastSeenTime: yson.Time(time.Now().UTC()),
			},
		},
		{
			c: &ytsys.Scheduler{
				Addr:        &ytsys.Addr{FQDN: "scheduler", Port: "9012"},
				ID:          yt.NodeID(guid.New()),
				Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
				Connected:   false,
			},
		},
		{
			c: &ytsys.ControllerAgent{
				Addr:        &ytsys.Addr{FQDN: "ca", Port: "9012"},
				ID:          yt.NodeID(guid.New()),
				Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
				Connected:   true,
			},
		},
	} {
		t.Run(string(tc.c.GetRole()), func(t *testing.T) {
			c := NewComponent(tc.c)

			data, err := yson.Marshal(c)
			require.NoError(t, err)
			t.Logf("marshaled: %s", data)

			unmarshaled := &Component{}
			err = yson.Unmarshal(data, unmarshaled)
			require.NoError(t, err)
			t.Logf("unmarshaled: %s", spew.Sdump(unmarshaled))

			require.Equal(t, c.Type, unmarshaled.Type)
		})
	}
}

func TestMaster(t *testing.T) {
	testMaster := func(t *testing.T, m *Master) {
		t.Helper()

		require.Equal(t, m.State, MasterStateAccepted)
		require.False(t, m.InMaintenance)

		req := ytsys.NewMaintenanceRequest("gocms", "tester", "test", nil)
		m.StartMaintenance(req)
		require.True(t, m.InMaintenance)
		require.NotEqual(t, m.MaintenanceStartTime, time.Time{})
		require.Equal(t, req, m.MaintenanceRequest)

		ticketKey := startrek.TicketKey("YTADMINREQ-4229")
		m.OnTicketCreated(ticketKey)
		require.Equal(t, ticketKey, m.TicketKey)
		require.Equal(t, m.State, MasterStateTicketMade, "state should not change")
		require.False(t, m.Decommissioned())
		require.False(t, m.Processed())

		m.AllowWalle()
		require.True(t, m.Decommissioned(), "followers could be given to wall-e automatically")
		require.True(t, m.Processed(), "followers could be given to wall-e automatically")

		m.FinishMaintenance()
		require.False(t, m.InMaintenance)
		require.NotEqual(t, m.MaintenanceFinishTime, time.Time{})

		m.SetFinished()
		require.True(t, m.Finished())
		require.Equal(t, MasterStateFinished, m.State)
		require.Equal(t, MasterStateFinished, m.GetState())
	}

	t.Run("Primary", func(t *testing.T) {
		c := &ytsys.PrimaryMaster{
			Addr:        &ytsys.Addr{FQDN: "primary-master", Port: "9012"},
			ID:          yt.NodeID(guid.New()),
			Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
		}
		m := NewPrimaryMaster(c)
		require.Equal(t, c.Addr.String(), m.Addr.String())
		require.Equal(t, c.Annotations.PhysicalHost, m.Host)

		testMaster(t, m)
	})

	t.Run("Secondary", func(t *testing.T) {
		c := &ytsys.SecondaryMaster{
			Addr:        &ytsys.Addr{FQDN: "secondary-master", Port: "9012"},
			ID:          yt.NodeID(guid.New()),
			Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
		}
		m := NewSecondaryMaster(c)
		require.Equal(t, c.Addr.String(), m.Addr.String())
		require.Equal(t, c.Annotations.PhysicalHost, m.Host)

		testMaster(t, m)
	})

	t.Run("TimestampProvider", func(t *testing.T) {
		c := &ytsys.TimestampProvider{
			Addr:        &ytsys.Addr{FQDN: "timestamp-provider", Port: "9016"},
			ID:          yt.NodeID(guid.New()),
			Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
		}
		p := NewTimestampProvider(c)
		require.Equal(t, c.Addr.String(), p.Addr.String())
		require.Equal(t, c.Annotations.PhysicalHost, p.Host)

		testMaster(t, p)
	})
}

func TestScheduler(t *testing.T) {
	c := &ytsys.Scheduler{
		Addr:        &ytsys.Addr{FQDN: "scheduler", Port: "9012"},
		ID:          yt.NodeID(guid.New()),
		Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
		Connected:   false,
	}

	s := NewScheduler(c)
	require.Equal(t, c.Addr.String(), s.Addr.String())
	require.Equal(t, c.Annotations.PhysicalHost, s.Host)
	require.Equal(t, SchedulerStateAccepted, s.GetState())
	require.False(t, s.InMaintenance)
	require.False(t, s.Decommissioned())
	require.False(t, s.Processed())

	req := ytsys.NewMaintenanceRequest("gocms", "tester", "test", nil)
	s.StartMaintenance(req)
	require.True(t, s.InMaintenance)
	require.NotEqual(t, s.MaintenanceStartTime, time.Time{})
	require.Equal(t, req, s.MaintenanceRequest)

	s.AllowWalle()
	require.True(t, s.Decommissioned())
	require.True(t, s.Processed())
	require.False(t, s.Finished())
	require.Equal(t, SchedulerStateProcessed, s.State)

	s.FinishMaintenance()
	require.False(t, s.InMaintenance)
	require.NotEqual(t, s.MaintenanceFinishTime, time.Time{})

	s.SetFinished()
	require.True(t, s.Finished())
	require.Equal(t, SchedulerStateFinished, s.State)
}

func TestControllerAgent(t *testing.T) {
	c := &ytsys.ControllerAgent{
		Addr:        &ytsys.Addr{FQDN: "ca", Port: "9012"},
		ID:          yt.NodeID(guid.New()),
		Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
		Connected:   true,
	}

	ca := NewControllerAgent(c)
	require.Equal(t, c.Addr.String(), ca.Addr.String())
	require.Equal(t, c.Annotations.PhysicalHost, ca.Host)
	require.Equal(t, ControllerAgentStateAccepted, ca.GetState())
	require.False(t, ca.InMaintenance)
	require.False(t, ca.Decommissioned())
	require.False(t, ca.Processed())

	req := ytsys.NewMaintenanceRequest("gocms", "tester", "test", nil)
	ca.StartMaintenance(req)
	require.True(t, ca.InMaintenance)
	require.NotEqual(t, ca.MaintenanceStartTime, time.Time{})
	require.Equal(t, req, ca.MaintenanceRequest)

	ca.AllowWalle()
	require.True(t, ca.Decommissioned())
	require.True(t, ca.Processed())
	require.False(t, ca.Finished())
	require.Equal(t, ControllerAgentStateProcessed, ca.State)

	ca.FinishMaintenance()
	require.False(t, ca.InMaintenance)
	require.NotEqual(t, ca.MaintenanceFinishTime, time.Time{})

	ca.SetFinished()
	require.True(t, ca.Finished())
	require.Equal(t, ControllerAgentStateFinished, ca.State)
}

func TestHTTPProxy(t *testing.T) {
	c := &ytsys.HTTPProxy{
		Addr:        &ytsys.Addr{FQDN: "ca", Port: "9012"},
		ID:          yt.NodeID(guid.New()),
		Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
		Role:        "data",
		Banned:      false,
		BanMessage:  "",
		Liveness:    &ytsys.HTTPProxyLiveness{UpdatedAt: yson.Time(time.Now().UTC())},
	}

	p := NewHTTPProxy(c)
	require.Equal(t, c.Addr.String(), p.Addr.String())
	require.Equal(t, c.Annotations.PhysicalHost, p.Host)
	require.False(t, p.Banned)
	require.Equal(t, c.Role, p.Role)

	require.Equal(t, HTTPProxyStateAccepted, p.GetState())
	require.False(t, p.Decommissioned())
	require.False(t, p.Processed())

	p.Ban()
	require.True(t, p.Banned)
	require.NotEqual(t, p.BanTime, time.Time{})

	p.AllowWalle()
	require.True(t, p.Decommissioned())
	require.True(t, p.Processed())
	require.False(t, p.Finished())
	require.Equal(t, HTTPProxyStateProcessed, p.State)

	p.Unban()
	require.False(t, p.Banned)
	require.NotEqual(t, p.UnbanTime, time.Time{})

	p.SetFinished()
	require.True(t, p.Finished())
	require.Equal(t, HTTPProxyStateFinished, p.State)
}

func TestRPCProxy(t *testing.T) {
	c := &ytsys.RPCProxy{
		Addr:        &ytsys.Addr{FQDN: "ca", Port: "9012"},
		ID:          yt.NodeID(guid.New()),
		Annotations: &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
		Role:        "yttank",
		Banned:      false,
		BanMessage:  "",
		Alive:       new(map[string]interface{}),
	}

	p := NewRPCProxy(c)
	require.Equal(t, c.Addr.String(), p.Addr.String())
	require.Equal(t, c.Annotations.PhysicalHost, p.Host)
	require.False(t, p.Banned)
	require.Equal(t, c.Role, p.Role)

	require.Equal(t, RPCProxyStateAccepted, p.GetState())
	require.False(t, p.Decommissioned())
	require.False(t, p.Processed())

	req := ytsys.NewMaintenanceRequest("gocms", "tester", "test", nil)
	p.StartMaintenance(req)
	require.True(t, p.InMaintenance)
	require.NotEqual(t, p.MaintenanceStartTime, time.Time{})
	require.Equal(t, req, p.MaintenanceRequest)

	p.Ban()
	require.True(t, p.Banned)
	require.NotEqual(t, p.BanTime, time.Time{})

	p.AllowWalle()
	require.True(t, p.Decommissioned())
	require.True(t, p.Processed())
	require.False(t, p.Finished())
	require.Equal(t, RPCProxyStateProcessed, p.State)

	p.Unban()
	require.False(t, p.Banned)
	require.NotEqual(t, p.UnbanTime, time.Time{})

	p.SetFinished()
	require.True(t, p.Finished())
	require.Equal(t, RPCProxyStateFinished, p.State)
}

func TestNode(t *testing.T) {
	c := &ytsys.Node{
		Addr:         &ytsys.Addr{FQDN: "abc", Port: "9012"},
		Annotations:  &ytsys.Annotations{PhysicalHost: "1.2.3.4"},
		ID:           yt.NodeID(guid.New()),
		LastSeenTime: yson.Time(time.Now().UTC()),
		Flavors:      []string{ytsys.FlavorData, ytsys.FlavorExec},
	}

	n := NewNode(c)
	require.Equal(t, c.Addr.String(), n.Addr.String())
	require.Equal(t, c.Annotations.PhysicalHost, n.Host)
	require.False(t, n.Banned)
	require.False(t, n.SchedulerJobsDisabled)
	require.False(t, n.WriteSessionsDisabled)
	require.False(t, n.RemovalSlotsOverridden)
	require.False(t, n.DecommissionInProgress)

	require.Equal(t, NodeStateAccepted, n.GetState())
	require.False(t, n.Decommissioned())
	require.False(t, n.Processed())

	require.True(t, n.HasFlavor(ytsys.FlavorExec))
	require.False(t, n.HasFlavor(ytsys.FlavorTablet))

	req := ytsys.NewMaintenanceRequest("gocms", "tester", "test", nil)
	n.StartMaintenance(req)
	require.True(t, n.InMaintenance)
	require.NotEqual(t, n.MaintenanceStartTime, time.Time{})
	require.Equal(t, req, n.MaintenanceRequest)

	n.MarkDecommissioned(1000)
	require.True(t, n.DecommissionInProgress)
	require.NotEqual(t, n.MarkedDecommissionedTime, time.Time{})
	require.Equal(t, n.TotalStoredChunksInitial, int64(1000))
	require.Equal(t, n.TotalStoredChunks, int64(1000))

	n.UpdateTotalStoredChunks(500)
	require.Equal(t, n.TotalStoredChunks, int64(500))

	newSlots, oldSlots := 512, 1024
	n.OverrideRemovalSlots(&newSlots, &oldSlots)
	require.True(t, n.RemovalSlotsOverridden)
	require.Equal(t, oldSlots, *n.PrevRemovalSlotsOverride)

	n.DisableWriteSessions()
	require.True(t, n.WriteSessionsDisabled)
	require.NotEqual(t, n.WriteSessionsDisableTime, time.Time{})

	n.DisableSchedulerJobs()
	require.True(t, n.SchedulerJobsDisabled)
	require.NotEqual(t, n.SchedulerJobsDisableTime, time.Time{})

	banTime := yson.Time(time.Now().UTC())
	n.Ban(banTime)
	require.True(t, n.Banned)
	require.Equal(t, n.BanTime, banTime)
	n.BanNow()
	require.True(t, n.Banned)
	require.True(t, time.Time(n.BanTime).After(time.Time(banTime)))

	n.SetDecommissioned()
	require.True(t, n.Decommissioned())
	require.True(t, time.Time(n.DecommissionFinishTime).After(time.Time(banTime)))
	require.False(t, n.Processed())

	n.AllowWalle()
	require.True(t, n.Decommissioned())
	require.True(t, n.Processed())
	require.False(t, n.Finished())
	require.Equal(t, NodeStateProcessed, n.State)

	n.Unban()
	require.False(t, n.Banned)
	require.NotEqual(t, n.UnbanTime, time.Time{})

	n.EnableSchedulerJobs()
	require.False(t, n.SchedulerJobsDisabled)
	require.NotEqual(t, n.SchedulerJobsReenableTime, time.Time{})

	n.EnableWriteSessions()
	require.False(t, n.WriteSessionsDisabled)
	require.NotEqual(t, n.WriteSessionsReenableTime, time.Time{})

	n.DropRemovalSlotsOverride()
	require.False(t, n.RemovalSlotsOverridden)

	n.UnmarkDecommissioned()
	require.False(t, n.DecommissionInProgress)
	require.NotEqual(t, n.UnmarkedDecommissionedTime, time.Time{})

	n.SetFinished()
	require.True(t, n.Finished())
	require.Equal(t, NodeStateFinished, n.State)
}
