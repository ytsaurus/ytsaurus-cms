package models

import (
	"fmt"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/admin/cms/internal/startrek"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytsys"
)

type Component struct {
	Type ytsys.ClusterRole `json:"type" yson:"type"`
	Role `json:"role" yson:"role"`
}

func NewComponent(c ytsys.Component) *Component {
	r := c.GetRole()
	switch r {
	case ytsys.RolePrimaryMaster:
		return &Component{Type: r, Role: NewPrimaryMaster(c.(*ytsys.PrimaryMaster))}
	case ytsys.RoleSecondaryMaster:
		return &Component{Type: r, Role: NewSecondaryMaster(c.(*ytsys.SecondaryMaster))}
	case ytsys.RoleTimestampProvider:
		return &Component{Type: r, Role: NewTimestampProvider(c.(*ytsys.TimestampProvider))}
	case ytsys.RoleScheduler:
		return &Component{Type: r, Role: NewScheduler(c.(*ytsys.Scheduler))}
	case ytsys.RoleControllerAgent:
		return &Component{Type: r, Role: NewControllerAgent(c.(*ytsys.ControllerAgent))}
	case ytsys.RoleNode:
		return &Component{Type: r, Role: NewNode(c.(*ytsys.Node))}
	case ytsys.RoleHTTPProxy:
		return &Component{Type: r, Role: NewHTTPProxy(c.(*ytsys.HTTPProxy))}
	case ytsys.RoleRPCProxy:
		return &Component{Type: r, Role: NewRPCProxy(c.(*ytsys.RPCProxy))}
	default:
		panic(fmt.Errorf("unsupported role: %+v", r))
	}
}

func (c *Component) UnmarshalYSON(data []byte) error {
	header := struct {
		Type ytsys.ClusterRole `json:"type" yson:"type"`
	}{}
	if err := yson.Unmarshal(data, &header); err != nil {
		return err
	}
	c.Type = header.Type

	switch header.Type {
	case ytsys.RolePrimaryMaster, ytsys.RoleSecondaryMaster, ytsys.RoleTimestampProvider:
		p := struct {
			Role *Master `json:"role" yson:"role"`
		}{}
		if err := yson.Unmarshal(data, &p); err != nil {
			return err
		}
		c.Role = p.Role
	case ytsys.RoleNode:
		p := struct {
			Role *Node `json:"role" yson:"role"`
		}{}
		if err := yson.Unmarshal(data, &p); err != nil {
			return err
		}
		c.Role = p.Role
	case ytsys.RoleHTTPProxy:
		p := struct {
			Role *HTTPProxy `json:"role" yson:"role"`
		}{}
		if err := yson.Unmarshal(data, &p); err != nil {
			return err
		}
		c.Role = p.Role
	case ytsys.RoleRPCProxy:
		p := struct {
			Role *RPCProxy `json:"role" yson:"role"`
		}{}
		if err := yson.Unmarshal(data, &p); err != nil {
			return err
		}
		c.Role = p.Role
	case ytsys.RoleScheduler:
		p := struct {
			Role *Scheduler `json:"role" yson:"role"`
		}{}
		if err := yson.Unmarshal(data, &p); err != nil {
			return err
		}
		c.Role = p.Role
	case ytsys.RoleControllerAgent:
		p := struct {
			Role *ControllerAgent `json:"role" yson:"role"`
		}{}
		if err := yson.Unmarshal(data, &p); err != nil {
			return err
		}
		c.Role = p.Role
	default:
		return xerrors.Errorf("unexpected component type %s", header.Type)
	}

	return nil
}

type Role interface {
	GetState() any
	Decommissioned() bool
	Processed() bool
	Finished() bool
}

type MasterProcessingState string

const (
	MasterStateAccepted   MasterProcessingState = "accepted"
	MasterStateTicketMade MasterProcessingState = "ticket_made"
	MasterStateProcessed  MasterProcessingState = "processed"
	MasterStateFinished   MasterProcessingState = "finished"
)

type Master struct {
	Host ytsys.PhysicalHost `json:"host" yson:"host"`
	Addr *ytsys.Addr        `json:"addr" yson:"addr"`
	Path ypath.Path         `json:"path" yson:"path"`

	CellID yt.NodeID `json:"cell_id" yson:"cell_id"`

	TicketKey startrek.TicketKey `json:"ticket_key" yson:"ticket_key"`

	InMaintenance         bool                      `json:"in_maintenance" yson:"in_maintenance"`
	MaintenanceStartTime  yson.Time                 `json:"maintenance_start_time" yson:"maintenance_start_time"`
	MaintenanceFinishTime yson.Time                 `json:"maintenance_finish_time" yson:"maintenance_finish_time"`
	MaintenanceRequest    *ytsys.MaintenanceRequest `json:"maintenance_request" yson:"maintenance_request"`

	State MasterProcessingState `json:"state" yson:"state"`
}

func NewPrimaryMaster(m *ytsys.PrimaryMaster) *Master {
	return &Master{
		Host:          m.PhysicalHost,
		Addr:          m.Addr,
		Path:          m.GetCypressPath(),
		CellID:        m.CellID,
		InMaintenance: bool(m.InMaintenance),
		State:         MasterStateAccepted,
	}
}

func NewSecondaryMaster(m *ytsys.SecondaryMaster) *Master {
	return &Master{
		Host:          m.PhysicalHost,
		Addr:          m.Addr,
		Path:          m.GetCypressPath(),
		CellID:        m.CellID,
		InMaintenance: bool(m.InMaintenance),
		State:         MasterStateAccepted,
	}
}

func NewTimestampProvider(p *ytsys.TimestampProvider) *Master {
	return &Master{
		Host:          p.PhysicalHost,
		Addr:          p.Addr,
		Path:          p.GetCypressPath(),
		CellID:        p.CellID,
		InMaintenance: bool(p.InMaintenance),
		State:         MasterStateAccepted,
	}
}

func (m *Master) GetState() any {
	return m.State
}

func (m *Master) Decommissioned() bool {
	return m.Processed()
}

func (m *Master) Processed() bool {
	return m.State == MasterStateProcessed
}

// AllowWalle changes master state so that it can be taken by walle.
func (m *Master) AllowWalle() {
	m.State = MasterStateProcessed
}

func (m *Master) Finished() bool {
	return m.State == MasterStateFinished
}

func (m *Master) SetFinished() {
	m.State = MasterStateFinished
}

func (m *Master) OnTicketCreated(key startrek.TicketKey) {
	m.State = MasterStateTicketMade
	m.TicketKey = key
}

func (m *Master) StartMaintenance(req *ytsys.MaintenanceRequest) {
	m.InMaintenance = true
	m.MaintenanceStartTime = yson.Time(time.Now().UTC())
	m.MaintenanceRequest = req
}

func (m *Master) FinishMaintenance() {
	m.InMaintenance = false
	m.MaintenanceFinishTime = yson.Time(time.Now().UTC())
}

type SchedulerProcessingState string

const (
	SchedulerStateAccepted  SchedulerProcessingState = "accepted"
	SchedulerStateProcessed SchedulerProcessingState = "processed"
	SchedulerStateFinished  SchedulerProcessingState = "finished"
)

type Scheduler struct {
	Host ytsys.PhysicalHost `json:"host" yson:"host"`
	Addr *ytsys.Addr        `json:"addr" yson:"addr"`
	Path ypath.Path         `json:"path" yson:"path"`

	InMaintenance         bool                      `json:"in_maintenance" yson:"in_maintenance"`
	MaintenanceStartTime  yson.Time                 `json:"maintenance_start_time" yson:"maintenance_start_time"`
	MaintenanceFinishTime yson.Time                 `json:"maintenance_finish_time" yson:"maintenance_finish_time"`
	MaintenanceRequest    *ytsys.MaintenanceRequest `json:"maintenance_request" yson:"maintenance_request"`

	State SchedulerProcessingState `json:"state" yson:"state"`
}

func NewScheduler(s *ytsys.Scheduler) *Scheduler {
	return &Scheduler{
		Host:          s.PhysicalHost,
		Addr:          s.Addr,
		Path:          s.GetCypressPath(),
		InMaintenance: bool(s.InMaintenance),
		State:         SchedulerStateAccepted,
	}
}

func (s *Scheduler) GetState() any {
	return s.State
}

func (s *Scheduler) Decommissioned() bool {
	return s.Processed()
}

func (s *Scheduler) Processed() bool {
	return s.State == SchedulerStateProcessed
}

// AllowWalle changes scheduler state so that it can be taken by walle.
func (s *Scheduler) AllowWalle() {
	s.State = SchedulerStateProcessed
}

func (s *Scheduler) Finished() bool {
	return s.State == SchedulerStateFinished
}

func (s *Scheduler) SetFinished() {
	s.State = SchedulerStateFinished
}

func (s *Scheduler) StartMaintenance(req *ytsys.MaintenanceRequest) {
	s.InMaintenance = true
	s.MaintenanceStartTime = yson.Time(time.Now().UTC())
	s.MaintenanceRequest = req
}

func (s *Scheduler) FinishMaintenance() {
	s.InMaintenance = false
	s.MaintenanceFinishTime = yson.Time(time.Now().UTC())
}

type ControllerAgentProcessingState string

const (
	ControllerAgentStateAccepted  ControllerAgentProcessingState = "accepted"
	ControllerAgentStateProcessed ControllerAgentProcessingState = "processed"
	ControllerAgentStateFinished  ControllerAgentProcessingState = "finished"
)

type ControllerAgent struct {
	Host ytsys.PhysicalHost `json:"host" yson:"host"`
	Addr *ytsys.Addr        `json:"addr" yson:"addr"`
	Path ypath.Path         `json:"path" yson:"path"`

	InMaintenance         bool                      `json:"in_maintenance" yson:"in_maintenance"`
	MaintenanceStartTime  yson.Time                 `json:"maintenance_start_time" yson:"maintenance_start_time"`
	MaintenanceFinishTime yson.Time                 `json:"maintenance_finish_time" yson:"maintenance_finish_time"`
	MaintenanceRequest    *ytsys.MaintenanceRequest `json:"maintenance_request" yson:"maintenance_request"`

	State ControllerAgentProcessingState `json:"state" yson:"state"`
}

func NewControllerAgent(a *ytsys.ControllerAgent) *ControllerAgent {
	return &ControllerAgent{
		Host:          a.PhysicalHost,
		Addr:          a.Addr,
		Path:          a.GetCypressPath(),
		InMaintenance: bool(a.InMaintenance),
		State:         ControllerAgentStateAccepted,
	}
}

func (a *ControllerAgent) GetState() any {
	return a.State
}

func (a *ControllerAgent) Decommissioned() bool {
	return a.Processed()
}

func (a *ControllerAgent) Processed() bool {
	return a.State == ControllerAgentStateProcessed
}

// AllowWalle changes agent state so that it can be taken by walle.
func (a *ControllerAgent) AllowWalle() {
	a.State = ControllerAgentStateProcessed
}

func (a *ControllerAgent) Finished() bool {
	return a.State == ControllerAgentStateFinished
}

func (a *ControllerAgent) SetFinished() {
	a.State = ControllerAgentStateFinished
}

func (a *ControllerAgent) StartMaintenance(req *ytsys.MaintenanceRequest) {
	a.InMaintenance = true
	a.MaintenanceStartTime = yson.Time(time.Now().UTC())
	a.MaintenanceRequest = req
}

func (a *ControllerAgent) FinishMaintenance() {
	a.InMaintenance = false
	a.MaintenanceFinishTime = yson.Time(time.Now().UTC())
}

type NodeProcessingState string

const (
	NodeStateAccepted       NodeProcessingState = "accepted"
	NodeStateDecommissioned NodeProcessingState = "decommissioned"
	NodeStateProcessed      NodeProcessingState = "processed"
	NodeStateFinished       NodeProcessingState = "finished"
)

type Node struct {
	Host ytsys.PhysicalHost `json:"host" yson:"host"`
	Addr *ytsys.Addr        `json:"addr" yson:"addr"`
	Path ypath.Path         `json:"path" yson:"path"`

	InMaintenance         bool                      `json:"in_maintenance" yson:"in_maintenance"`
	MaintenanceStartTime  yson.Time                 `json:"maintenance_start_time" yson:"maintenance_start_time"`
	MaintenanceFinishTime yson.Time                 `json:"maintenance_finish_time" yson:"maintenance_finish_time"`
	MaintenanceRequest    *ytsys.MaintenanceRequest `json:"maintenance_request" yson:"maintenance_request"`

	Banned    bool      `json:"banned" yson:"banned"`
	BanTime   yson.Time `json:"ban_time" yson:"ban_time"`
	UnbanTime yson.Time `json:"unban_time" yson:"unban_time"`

	DecommissionInProgress     bool      `json:"decommission_in_progress" yson:"decommission_in_progress"` // todo remove json tags until needed
	MarkedDecommissionedTime   yson.Time `json:"marked_decommissioned_time" yson:"marked_decommissioned_time"`
	DecommissionFinishTime     yson.Time `json:"decommission_finish_time" yson:"decommission_finish_time"`
	UnmarkedDecommissionedTime yson.Time `json:"unmarked_decommissioned_time" yson:"unmarked_decommissioned_time"`

	TotalStoredChunksInitial int64 `json:"total_stored_chunks_initial" yson:"total_stored_chunks_initial"`
	TotalStoredChunks        int64 `json:"total_stored_chunks" yson:"total_stored_chunks"`

	ReservedCPU float64 `json:"reserved_cpu" yson:"reserved_cpu"`

	Flavors []string `json:"flavors" yson:"flavors"`

	// RemovalSlotsOverridden is true iff the corresponding node setting
	// was changed to ensure safe chunk decommission.
	RemovalSlotsOverridden bool `json:"removal_slots_overridden" yson:"removal_slots_overridden"`
	// RemovalSlotsOverride is a value of the corresponding setting
	// set by cms to ensure safe chunk decommission.
	RemovalSlotsOverride *int `json:"removal_slots_override,omitempty" yson:"removal_slots_override,omitempty"`
	// PrevRemovalSlotsOverride is value of the corresponding setting
	// before it was overridden by cms to ensure safe chunk decommission.
	PrevRemovalSlotsOverride *int `json:"prev_removal_slots_override,omitempty" yson:"prev_removal_slots_override,omitempty"`

	WriteSessionsDisabled     bool      `json:"write_sessions_disabled" yson:"write_sessions_disabled"`
	WriteSessionsDisableTime  yson.Time `json:"write_sessions_disable_time" yson:"write_sessions_disable_time"`
	WriteSessionsReenableTime yson.Time `json:"write_sessions_reenable_time" yson:"write_sessions_reenable_time"`

	SchedulerJobsDisabled     bool      `json:"scheduler_jobs_disabled" yson:"scheduler_jobs_disabled"`
	SchedulerJobsDisableTime  yson.Time `json:"scheduler_jobs_disable_time" yson:"scheduler_jobs_disable_time"`
	SchedulerJobsReenableTime yson.Time `json:"scheduler_jobs_reenable_time" yson:"scheduler_jobs_reenable_time"`

	State NodeProcessingState `json:"state" yson:"state"`
}

func NewNode(n *ytsys.Node) *Node {
	return &Node{
		Host:          n.PhysicalHost,
		Addr:          n.Addr,
		Path:          n.GetCypressPath(),
		InMaintenance: bool(n.InMaintenance),
		BanTime:       yson.Time(time.Now().UTC()),
		State:         NodeStateAccepted,
		Flavors:       n.Flavors,
	}
}

func (n *Node) GetState() any {
	return n.State
}

func (n *Node) StartMaintenance(req *ytsys.MaintenanceRequest) {
	n.InMaintenance = true
	n.MaintenanceStartTime = yson.Time(time.Now().UTC())
	n.MaintenanceRequest = req
}

func (n *Node) FinishMaintenance() {
	n.InMaintenance = false
	n.MaintenanceFinishTime = yson.Time(time.Now().UTC())
}

// SetDecommissioned finishes node decommission.
func (n *Node) SetDecommissioned() {
	n.State = NodeStateDecommissioned
	n.DecommissionFinishTime = yson.Time(time.Now().UTC())
}

func (n *Node) Decommissioned() bool {
	return n.Processed() || n.State == NodeStateDecommissioned
}

func (n *Node) Processed() bool {
	return n.State == NodeStateProcessed
}

func (n *Node) BanNow() {
	n.Ban(yson.Time(time.Now().UTC()))
}

func (n *Node) Ban(t yson.Time) {
	n.BanTime = t
	n.Banned = true
}

func (n *Node) Unban() {
	n.State = NodeStateAccepted
	n.Banned = false
	n.UnbanTime = yson.Time(time.Now().UTC())
}

// HasFlavor check that node has given flavor.
func (n *Node) HasFlavor(f string) bool {
	for _, flavor := range n.Flavors {
		if flavor == f {
			return true
		}
	}
	return false
}

// AllowWalle changes node state so that it can be taken by walle.
func (n *Node) AllowWalle() {
	n.State = NodeStateProcessed
}

func (n *Node) Finished() bool {
	return n.State == NodeStateFinished
}

func (n *Node) SetFinished() {
	n.State = NodeStateFinished
}

// MarkDecommissioned corresponds to the event of setting @decommissioned attribute to true.
func (n *Node) MarkDecommissioned(totalStoredChunks int64) {
	n.DecommissionInProgress = true
	n.MarkedDecommissionedTime = yson.Time(time.Now().UTC())
	n.TotalStoredChunksInitial = totalStoredChunks
	n.TotalStoredChunks = totalStoredChunks
}

// UnmarkDecommissioned corresponds to the event of setting @decommissioned attribute to false.
func (n *Node) UnmarkDecommissioned() {
	n.DecommissionInProgress = false
	n.UnmarkedDecommissionedTime = yson.Time(time.Now().UTC())
}

func (n *Node) UpdateTotalStoredChunks(v int64) {
	n.TotalStoredChunks = v
}

func (n *Node) OverrideRemovalSlots(new, old *int) {
	n.RemovalSlotsOverridden = true
	n.RemovalSlotsOverride = new
	n.PrevRemovalSlotsOverride = old
}

func (n *Node) DropRemovalSlotsOverride() {
	n.RemovalSlotsOverridden = false
}

func (n *Node) DisableWriteSessions() {
	n.WriteSessionsDisabled = true
	n.WriteSessionsDisableTime = yson.Time(time.Now().UTC())
}

func (n *Node) EnableWriteSessions() {
	n.WriteSessionsDisabled = false
	n.WriteSessionsReenableTime = yson.Time(time.Now().UTC())
}

func (n *Node) DisableSchedulerJobs() {
	n.SchedulerJobsDisabled = true
	n.SchedulerJobsDisableTime = yson.Time(time.Now().UTC())
}

func (n *Node) EnableSchedulerJobs() {
	n.SchedulerJobsDisabled = false
	n.SchedulerJobsReenableTime = yson.Time(time.Now().UTC())
}

type HTTPProxyProcessingState string

const (
	HTTPProxyStateAccepted  HTTPProxyProcessingState = "accepted"
	HTTPProxyStateProcessed HTTPProxyProcessingState = "processed"
	HTTPProxyStateFinished  HTTPProxyProcessingState = "finished"
)

type HTTPProxy struct {
	Host ytsys.PhysicalHost `json:"host" yson:"host"`
	Addr *ytsys.Addr        `json:"addr" yson:"addr"`
	Path ypath.Path         `json:"path" yson:"path"`

	Role ytsys.YTProxyRole `json:"role" yson:"role"`

	Banned    bool      `json:"banned" yson:"banned"`
	BanTime   yson.Time `json:"ban_time" yson:"ban_time"`
	UnbanTime yson.Time `json:"unban_time" yson:"unban_time"`

	State HTTPProxyProcessingState `json:"state" yson:"state"`
}

func NewHTTPProxy(p *ytsys.HTTPProxy) *HTTPProxy {
	return &HTTPProxy{
		Host:   p.PhysicalHost,
		Addr:   p.Addr,
		Path:   p.GetCypressPath(),
		Role:   p.Role,
		Banned: bool(p.Banned),
		State:  HTTPProxyStateAccepted,
	}
}

func (p *HTTPProxy) GetState() any {
	return p.State
}

func (p *HTTPProxy) Decommissioned() bool {
	return p.Processed()
}

func (p *HTTPProxy) Processed() bool {
	return p.State == HTTPProxyStateProcessed
}

func (p *HTTPProxy) Finished() bool {
	return p.State == HTTPProxyStateFinished
}

func (p *HTTPProxy) SetFinished() {
	p.State = HTTPProxyStateFinished
}

// AllowWalle changes proxy state so that it can be taken by walle.
func (p *HTTPProxy) AllowWalle() {
	p.State = HTTPProxyStateProcessed
}

func (p *HTTPProxy) Ban() {
	p.Banned = true
	p.BanTime = yson.Time(time.Now().UTC())
}

func (p *HTTPProxy) Unban() {
	p.Banned = false
	p.UnbanTime = yson.Time(time.Now().UTC())
}

type RPCProxyProcessingState string

const (
	RPCProxyStateAccepted  RPCProxyProcessingState = "accepted"
	RPCProxyStateProcessed RPCProxyProcessingState = "processed"
	RPCProxyStateFinished  RPCProxyProcessingState = "finished"
)

type RPCProxy struct {
	Host ytsys.PhysicalHost `json:"host" yson:"host"`
	Addr *ytsys.Addr        `json:"addr" yson:"addr"`
	Path ypath.Path         `json:"path" yson:"path"`

	Role ytsys.YTProxyRole `json:"role" yson:"role"`

	InMaintenance         bool                      `json:"in_maintenance" yson:"in_maintenance"`
	MaintenanceStartTime  yson.Time                 `json:"maintenance_start_time" yson:"maintenance_start_time"`
	MaintenanceFinishTime yson.Time                 `json:"maintenance_finish_time" yson:"maintenance_finish_time"`
	MaintenanceRequest    *ytsys.MaintenanceRequest `json:"maintenance_request" yson:"maintenance_request"`

	Banned    bool      `json:"banned" yson:"banned"`
	BanTime   yson.Time `json:"ban_time" yson:"ban_time"`
	UnbanTime yson.Time `json:"unban_time" yson:"unban_time"`

	State RPCProxyProcessingState `json:"state" yson:"state"`
}

func NewRPCProxy(p *ytsys.RPCProxy) *RPCProxy {
	return &RPCProxy{
		Host:          p.PhysicalHost,
		Addr:          p.Addr,
		Path:          p.GetCypressPath(),
		Role:          p.Role,
		InMaintenance: bool(p.InMaintenance),
		Banned:        bool(p.Banned),
		State:         RPCProxyStateAccepted,
	}
}

func (p *RPCProxy) GetState() any {
	return p.State
}

func (p *RPCProxy) StartMaintenance(req *ytsys.MaintenanceRequest) {
	p.InMaintenance = true
	p.MaintenanceStartTime = yson.Time(time.Now().UTC())
	p.MaintenanceRequest = req
}

func (p *RPCProxy) FinishMaintenance() {
	p.InMaintenance = false
	p.MaintenanceFinishTime = yson.Time(time.Now().UTC())
}

func (p *RPCProxy) Decommissioned() bool {
	return p.Processed()
}

func (p *RPCProxy) Processed() bool {
	return p.State == RPCProxyStateProcessed
}

// AllowWalle changes proxy state so that it can be taken by walle.
func (p *RPCProxy) AllowWalle() {
	p.State = RPCProxyStateProcessed
}

func (p *RPCProxy) Ban() {
	p.Banned = true
	p.BanTime = yson.Time(time.Now().UTC())
}

func (p *RPCProxy) Unban() {
	p.Banned = false
	p.UnbanTime = yson.Time(time.Now().UTC())
}

func (p *RPCProxy) Finished() bool {
	return p.State == RPCProxyStateFinished
}

func (p *RPCProxy) SetFinished() {
	p.State = RPCProxyStateFinished
}
