package walle

import "time"

// TaskID type hold task ids.
type TaskID string

// TaskType is a type that holds all available task types.
type TaskType string

const (
	// TaskTypeManual is the one initiated by user.
	TaskTypeManual TaskType = "manual"
	// TaskTypeAutomated is the one initiated by robot.
	TaskTypeAutomated TaskType = "automated"
)

// HostAction type holds all available host actions.
type HostAction string

const (
	ActionPrepare              HostAction = "prepare"
	ActionDeactivate           HostAction = "deactivate"
	ActionPowerOff             HostAction = "power-off"
	ActionReboot               HostAction = "reboot"
	ActionProfile              HostAction = "profile"
	ActionRedeploy             HostAction = "redeploy"
	ActionRepairLink           HostAction = "repair-link"
	ActionChangeDisk           HostAction = "change-disk"
	ActionTemporaryUnreachable HostAction = "temporary-unreachable"
)

// HostActions stores all available host actions.
var HostActions = []HostAction{
	ActionPrepare, ActionDeactivate, ActionPowerOff, ActionReboot,
	ActionProfile, ActionRedeploy, ActionRepairLink, ActionChangeDisk,
	ActionTemporaryUnreachable,
}

// FailureType type holds all available failure types.
type FailureType string

// Task is a request from the user or wall-E automation
// to perform an operation with one or more machines.
type Task struct {
	// ID is a task identifier unique among all projects.
	ID TaskID `json:"id" yson:"id,key"`
	// TaskGroup identifies group the task belongs to.
	TaskGroup string   `json:"task_group" yson:"task_group"`
	Type      TaskType `json:"type" yson:"type"`
	// Issuer is a task author.
	Issuer string `json:"issuer" yson:"issuer"`
	// Action is requested action to be performed on the host.
	Action HostAction `json:"action" yson:"action"`
	// Hosts to perform an action on.
	Hosts []string `json:"hosts" yson:"hosts"`

	// Comment is an optional message from issuer.
	Comment string `json:"comment,omitempty" yson:"comment,omitempty"`
	// Extra contains additional task parameters.
	Extra map[string]interface{} `json:"extra,omitempty" yson:"extra,omitempty"`
	// ScenarioInfo contains additional information about wall-e scenario.
	ScenarioInfo *ScenarioInfo `json:"scenario_info,omitempty" yson:"scenario_info,omitempty"`

	Failure string `json:"failure,omitempty" yson:"failure,omitempty"`
	// FailureType is an optional field that might have information about task cause.
	FailureType FailureType `json:"failure_type,omitempty" yson:"failure_type,omitempty"`
	// CheckNames consists of the names of the checks that triggered the creation of this repair task.
	// Problematic host(s) might have more failed checks than the task.
	// Only the checks used in the failed rule are sent.
	CheckNames []string `json:"check_names,omitempty" yson:"check_names,omitempty"`
	// MaintenanceInfo is a YP compatibility layer.
	MaintenanceInfo *MaintenanceInfo `json:"maintenance_info,omitempty" yson:"maintenance_info,omitempty"`
}

type ScenarioType string

const (
	ScenarioTypeITDCMaintenance ScenarioType = "itdc-maintenance"
	ScenarioTypeNOCHard         ScenarioType = "noc-hard"
	ScenarioTypeNOCSoft         ScenarioType = "noc-soft"
	ScenarioTypeNewNOCSoft      ScenarioType = "new-noc-soft"
	ScenarioTypeHardwareUpgrade ScenarioType = "hardware-upgrade"
	ScenarioTypeHostTransfer    ScenarioType = "hosts-transfer"
)

type ScenarioInfo struct {
	ID                   int          `json:"scenario_id,omitempty" yson:"scenario_id,omitempty"`
	Type                 ScenarioType `json:"scenario_type,omitempty" yson:"scenario_type,omitempty"`
	MaintenanceStartTime int          `json:"maintenance_start_time,omitempty" yson:"maintenance_start_time,omitempty"`
	MaintenanceEndTime   int          `json:"maintenance_end_time,omitempty" yson:"maintenance_end_time,omitempty"`
}

func (i *ScenarioInfo) HasMaintenanceStartTime() bool {
	return i != nil && i.MaintenanceStartTime > 0
}

func (i *ScenarioInfo) MaintenanceStart() time.Time {
	return time.Unix(int64(i.MaintenanceStartTime), 0)
}

func (i *ScenarioInfo) MaintenanceEnd() time.Time {
	return time.Unix(int64(i.MaintenanceEndTime), 0)
}

func (i *ScenarioInfo) IsNOCScenario() bool {
	if i == nil {
		return false
	}
	switch i.Type {
	case ScenarioTypeNOCHard, ScenarioTypeNOCSoft, ScenarioTypeNewNOCSoft:
		return true
	default:
		return false
	}
}

// MaintenanceKind type holds all available maintenance kind values.
type MaintenanceKind = HostAction

// MaintenancePriority type holds all available maintenance priority values.
type MaintenancePriority string

const (
	MaintenancePriorityNone   MaintenancePriority = "none"
	MaintenancePriorityHigh   MaintenancePriority = "high"
	MaintenancePriorityNormal MaintenancePriority = "normal"
)

// MaintenanceSource type holds all available maintenance source values.
type MaintenanceSource string

const (
	MaintenanceSourceNone        MaintenanceSource = "none"
	MaintenanceSourceManual      MaintenanceSource = "manual"
	MaintenanceSourceAutohealing MaintenanceSource = "autohealing"
)

func MakeTaskType(s MaintenanceSource) TaskType {
	switch s {
	case MaintenanceSourceManual:
		return TaskTypeManual
	case MaintenanceSourceAutohealing:
		return TaskTypeAutomated
	default:
		return TaskTypeManual // todo replace by "" when YP-3210 is resolved.
	}
}

// MaintenanceInfo stores almost the same data as the task in a different format.
type MaintenanceInfo struct {
	// UUID is a randomly generated by YP globally unique identifier.
	UUID string `json:"uuid,omitempty" yson:"uuid,omitempty"`
	// ID is an identifier defined by wall-e.
	ID string `json:"id,omitempty" yson:"id,omitempty"`
	// Kind identifies maintenance type. Can be considered as urgency on the project's terms.
	Kind MaintenanceKind `json:"kind,omitempty" yson:"kind,omitempty"`
	// NodeSetID stores node set identifier, e.g. "switch-iva5-s32", which allows to group tasks.
	NodeSetID string `json:"node_set_id,omitempty" yson:"node_set_id,omitempty"`
	// Message stores human-readable description.
	Message string `json:"message,omitempty" yson:"message,omitempty"`
	// Disruptive indicates whether this maintenance can corrupt data.
	Disruptive bool `json:"disruptive,omitempty" yson:"disruptive,omitempty"`
	// EstimatedDuration stores estimated repair duration in seconds.
	EstimatedDuration float64 `json:"estimated_duration,omitempty" yson:"estimated_duration,omitempty"`
	// Labels store dynamically typed description.
	Labels   map[string]interface{} `json:"labels,omitempty" yson:"labels,omitempty"`
	Priority MaintenancePriority    `json:"priority,omitempty" yson:"priority,omitempty"`
	Source   MaintenanceSource      `json:"source,omitempty" yson:"source,omitempty"`
}

// AddTaskRequest is a type corresponding to "POST /tasks" request.
type AddTaskRequest = Task

// TaskStatus type holds all available task statuses.
type TaskStatus string

const (
	// StatusOK is a status of an accepted request.
	StatusOK TaskStatus = "ok"
	// StatusInProcess is a status of an accepted request being processed.
	StatusInProcess TaskStatus = "in-process"
	// StatusRejected is a status of a declined request.
	StatusRejected TaskStatus = "rejected"
)

// AddTaskResponse is a response to the "POST /tasks" request.system.
type AddTaskResponse struct {
	*Task
	// Status is request status.
	Status TaskStatus `json:"status" yson:"status"`
	// Message is an additional description of the status.
	Message string `json:"message,omitempty" yson:"message,omitempty"`
}

// GetTaskResponse is a response to the "GET /tasks/<task_id>" request.
type GetTaskResponse = AddTaskResponse

// GetTasksResponse is a response to the "GET /tasks" request.
type GetTasksResponse struct {
	Result []*AddTaskResponse `json:"result" yson:"result"`
}

// ErrorResponse is a response in case of internal error.
type ErrorResponse struct {
	// Problem description.
	Message string `json:"message" yson:"message"`
}

// NewErrorResponse creates new error response with given message.
func NewErrorResponse(msg string) *ErrorResponse {
	return &ErrorResponse{Message: msg}
}
