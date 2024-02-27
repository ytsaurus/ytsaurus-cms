package models

import (
	"fmt"
	"time"

	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/ytsys"
)

type DecisionMaker string

const (
	// DecisionMakerCMS is an entity used as Task's DecisionMaker field
	// when the decision is made by cms automation.
	DecisionMakerCMS DecisionMaker = "gocms"
)

// TaskProcessingState is type to hold all available internal processing states.
type TaskProcessingState string

const (
	// StateNew is a state of an accepted task not yet being processed.
	StateNew TaskProcessingState = "new"
	// StatePending is a state of an in-progress task.
	StatePending TaskProcessingState = "pending"
	// StateDecommissioned is a state of task with decommissioned components.
	// Tasks in this state can not be given to Wall-e
	// until some external conditions (e.g. LVC=0) are met.
	StateDecommissioned TaskProcessingState = "decommissioned"
	// StateProcessed is a state of a task ready to be given to walle.
	StateProcessed TaskProcessingState = "processed"
	// StateProcessed is a state of a task confirmed manually via API.
	// When task enters this state it can be taken by Wall-e immediately.
	StateConfirmedManually TaskProcessingState = "confirmed_manually"
	// StateFinished is a state of a task deleted by walle.
	StateFinished TaskProcessingState = "finished"
)

// TaskProcessingStates stores all available task processing states.
var TaskProcessingStates = []TaskProcessingState{
	StateNew, StatePending, StateDecommissioned, StateProcessed, StateConfirmedManually, StateFinished,
}

// TaskOrigin is type to hold all available task origins.
type TaskOrigin string

const (
	// OriginWalle is a source of all tasks created via post http request by Wall-e directly.
	OriginWalle TaskOrigin = "wall-e"
	// OriginYP is a source of all tasks polled from YP.
	OriginYP TaskOrigin = "yp"
)

type Task struct {
	*walle.Task

	// IsGroupTask tells whether task belongs to some group or not.
	IsGroupTask bool `json:"is_group_task" yson:"is_group_task"`

	// Origin is a source the task was obtained from.
	Origin TaskOrigin `json:"origin" yson:"origin"`
	// YPInfo stores additional yp-related task fields
	// Filled only for OriginYP.
	YPInfo *YPMaintenanceInfo `json:"yp_info" yson:"yp_info"`

	// CreatedAt is a UTC time of task creation.
	CreatedAt yson.Time `json:"created_at" yson:"created_at"`
	// UpdatedAt is a last update UTC time. Changes on any field update.
	UpdatedAt yson.Time `json:"updated_at" yson:"updated_at"`
	// ProcessedAt is a UTC time when the task hosts were allowed to be given to Wall-e.
	ProcessedAt yson.Time `json:"processed_at" yson:"processed_at"`
	// FinishedAt is a UTC time of a deleted task when all decommission work is undone.
	FinishedAt yson.Time `json:"finished_at" yson:"finished_at"`

	// ConfirmationRequested flag signals that someone has manually confirmed this task
	// and it can be given to Wall-e with no further conditions to wait.
	ConfirmationRequested bool `json:"confirmation_requested" yson:"confirmation_requested"`
	// ConfirmationRequestedAt is a UTC time of the manual confirmation request.
	ConfirmationRequestedAt yson.Time `json:"confirmation_requested_at" yson:"confirmation_requested_at"`
	// ConfirmationRequestedBy stores login of user who wants to confirmed task manually.
	ConfirmationRequestedBy string `json:"confirmation_requested_by" yson:"confirmation_requested_by"`

	// DeletionRequestedAt is a UTC time of the deletion request.
	DeletionRequestedAt yson.Time `json:"deletion_requested_at" yson:"deletion_requested_at"`
	// DeletionRequested flag signals that Wall-e is no longer interested in this task.
	DeletionRequested bool `json:"deletion_requested" yson:"deletion_requested"`

	// ProcessingState is an internal task state.
	ProcessingState TaskProcessingState `json:"processing_state" yson:"processing_state"`
	// DecisionMaker is an entity responsible for current task state.
	DecisionMaker DecisionMaker `json:"decision_maker" yson:"decision_maker"`

	// HostStates stores processing states of all task hosts.
	HostStates map[string]*Host `json:"host_states" yson:"host_states"`

	// WalleStatus is a task status in Wall-e format.
	WalleStatus walle.TaskStatus `json:"walle_status" yson:"walle_status"`
	// WalleStatusDescription is an additional status info.
	WalleStatusDescription string `json:"walle_status_description" yson:"walle_status_description"`
}

// ConfirmManually sets task's processing state to StateConfirmedManually.
// DecisionMaker is set to provided argument iff any task field is updated.
//
// If task's WalleStatus is non-terminal it is set to walle.StatusOK.
func (t *Task) ConfirmManually(decisionMaker DecisionMaker) {
	if t.ProcessingState != StateConfirmedManually {
		t.ProcessingState = StateConfirmedManually
		t.ProcessedAt = yson.Time(time.Now().UTC())
		t.DecisionMaker = decisionMaker
	}

	if t.WalleStatus == walle.StatusInProcess {
		t.DecisionMaker = decisionMaker
		t.WalleStatus = walle.StatusOK
		t.WalleStatusDescription = fmt.Sprintf("set manually by %s", decisionMaker)
	}
}

// HasMasterRole checks whether task contains master role or not.
func (t *Task) HasMasterRole() bool {
	return len(t.GetMasters()) > 0
}

// GetMasters returns all masters of the task.
func (t *Task) GetMasters() []*Master {
	var masters []*Master
	for _, h := range t.HostStates {
		for _, r := range h.Roles {
			switch r.Type {
			case ytsys.RolePrimaryMaster, ytsys.RoleSecondaryMaster, ytsys.RoleTimestampProvider:
				masters = append(masters, r.Role.(*Master))
			}
		}
	}
	return masters
}

// GetNodes returns all nodes of the task.
func (t *Task) GetNodes() []*Node {
	var nodes []*Node
	for _, h := range t.HostStates {
		for _, r := range h.Roles {
			switch r.Type {
			case ytsys.RoleNode:
				nodes = append(nodes, r.Role.(*Node))
			}
		}
	}
	return nodes
}

func (t *Task) SetDecommissioned() {
	t.ProcessingState = StateDecommissioned
}

func (t *Task) SetProcessed() {
	if t.ProcessingState == StateProcessed {
		return
	}

	t.ProcessingState = StateProcessed
	t.ProcessedAt = yson.Time(time.Now().UTC())
	t.DecisionMaker = DecisionMakerCMS

	t.WalleStatus = walle.StatusOK
	t.WalleStatusDescription = fmt.Sprintf("allowed by %s", t.DecisionMaker)
}

func (t *Task) SetFinished() {
	if t.ProcessingState != StateFinished {
		t.ProcessingState = StateFinished
		t.FinishedAt = yson.Time(time.Now().UTC())
	}
}

func (t *Task) AllHostsFinished() bool {
	for _, h := range t.HostStates {
		if h.State != HostStateFinished {
			return false
		}
	}
	return true
}

func (t *Task) AllHostsProcessed() bool {
	for _, h := range t.HostStates {
		if h.State != HostStateProcessed {
			return false
		}
	}
	return true
}

func (t *Task) AllHostsDecommissioned() bool {
	for _, h := range t.HostStates {
		if h.State != HostStateDecommissioned && h.State != HostStateProcessed {
			return false
		}
	}
	return true
}

type ClusterInfo map[string]HostInfo

type HostInfo map[ypath.Path]ComponentInfo

type ComponentInfo interface{}

func (t *Task) GetClusterInfo(c Cluster) ClusterInfo {
	info := make(ClusterInfo)

	for _, h := range t.Hosts {
		info[h] = make(HostInfo)
		components, ok := c.GetHostComponents(h)
		if !ok {
			continue
		}
		for _, c := range components {
			info[h][c.GetCypressPath()] = c
		}
	}

	return info
}

func (t *Task) IsUrgent() bool {
	if !t.ScenarioInfo.HasMaintenanceStartTime() {
		return true
	}

	const urgentTaskTimeout = time.Hour * 2
	return time.Until(t.ScenarioInfo.MaintenanceStart()) < urgentTaskTimeout
}

// TaskGroup is an alias for a task slice.
type TaskGroup []*Task

// GetGroupID returns maintenance_info/node_set_id for non-empty task group.
//
// Returns empty string for empty task group.
func (g TaskGroup) GetGroupID() string {
	if len(g) == 0 {
		return ""
	}
	return g[0].MaintenanceInfo.NodeSetID
}

// YTRole is a string alias to hold all available values of /labels/yt_role.
type YTRole string

const (
	YTRoleNode            YTRole = "ytnode"
	YTRoleHTTPProxy       YTRole = "ytproxy"
	YTRoleRPCProxy        YTRole = "ytrpcproxy"
	YTRoleControllerAgent YTRole = "ytcontrolleragent"
	YTRoleScheduler       YTRole = "ytscheduler"
	YTRoleMaster          YTRole = "ytmaster"
	YTRoleSolomonBridge   YTRole = "ytsolomonbridge"
)

func (r YTRole) Compare(cr ytsys.ClusterRole) bool {
	switch cr {
	case ytsys.RoleNode:
		return r == YTRoleNode
	case ytsys.RoleHTTPProxy:
		return r == YTRoleHTTPProxy
	case ytsys.RoleRPCProxy:
		return r == YTRoleRPCProxy
	case ytsys.RoleControllerAgent:
		return r == YTRoleControllerAgent
	case ytsys.RoleScheduler:
		return r == YTRoleScheduler
	case ytsys.RolePrimaryMaster, ytsys.RoleSecondaryMaster:
		return r == YTRoleMaster
	default:
		return false
	}
}

// YPMaintenanceInfo type holds additional data of the single pod maintenance request.
type YPMaintenanceInfo struct {
	// Backend is an address of YP cluster e.g. "man".
	Backend string `json:"backend" yson:"backend"`
	PodID   string `json:"pod_id" yson:"pod_id"`
	// PodUUID is a unique yp pod identifier.
	PodUUID string `json:"pod_uuid" yson:"pod_uuid"`
	// NodeID stores yp node id.
	NodeID string `json:"node_id" yson:"node_id"`
	// YTRole corresponds to pod's yt_role label.
	YTRole YTRole `json:"yt_role" yson:"yt_role"`
}
