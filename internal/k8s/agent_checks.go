package k8s

import (
	"slices"
	"strings"

	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	corev1 "k8s.io/api/core/v1"
)

var ActiveChecks = []NodeCheck{NodeDrainAgentCheck}

// NodeCheck is a signature for check functions that make decisions about what actions should be done on node.
type NodeCheck = func(node corev1.Node) CheckResult

// CheckResult is a check result, whose fields are used for task creation.
type CheckResult struct {
	Name           CheckName // Unique check ID. Must be filled in any check. Used for task creation as [walle.Task.Failure].
	ActionRequired bool      // Action is required by this check.

	Action  walle.HostAction // Action that should be "done" on node because of check. Decomission speed depends on action type.
	Comment string           // Additional comment.
}

// CheckName is a string, that identifies automated node check.
type CheckName = string

const (
	NodeDrainAgentCheckName CheckName = "node_drain_agent_check"
	MemoryCheckName         CheckName = "memory_check"

	StatusFailed   = "FAILED"
	StatusCritical = "CRITICAL"
	StatusError    = "ERROR"
	StatusWarning  = "WARNING"
)

var DrainStatuses = []string{StatusFailed, StatusCritical, StatusError, StatusWarning}

// NodeDrainAgentCheck is a universal check that decommissions node if any of node-drain-agent' check status is one of [DrainStatuses].
func NodeDrainAgentCheck(node corev1.Node) CheckResult {
	for key, value := range node.Annotations {
		if strings.HasPrefix(key, AgentAnnotationPrefix) && slices.Contains(DrainStatuses, value) {
			return CheckResult{
				ActionRequired: true,
				Name:           NodeDrainAgentCheckName,
				Action:         walle.ActionReboot,
				Comment:        key + ": " + value,
			}
		}
	}
	return CheckResult{Name: NodeDrainAgentCheckName}
}

// Memory check is an example for more specific check.
func MemoryCheck(node corev1.Node) CheckResult {
	value, ok := node.Annotations["node-drain-agent/hw_watcher.mem.status"]
	if ok && value == "FAILED" {
		return CheckResult{
			ActionRequired: true,
			Name:           MemoryCheckName,
			Action:         walle.ActionReboot,
			Comment:        node.Annotations["node-drain-agent/hw_watcher.mem.code"],
		}
	}

	return CheckResult{Name: MemoryCheckName}
}
