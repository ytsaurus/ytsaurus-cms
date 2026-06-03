# YT Cluster Management System in K8S

CMS creates "tasks" on k8s-nodes according to their [annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/). There are two types of tasks - manual and automated. No new task on k8s-node will be created until old task on this node exists. When CMS starts processing k8s-node, it annotates it with key `yt-cms/status` and periodically updates annotation value with corresponding task' `processing_state` (`new`, `pending`, `decomissioned`, `processed`). When node is annotated with `yt-cms/status: processed`, it can be taken safely. CMS' tasks and annotations are deleted when task reason (annotation) is deleted. 

## Manual
Manual task can be created by annotation with key prefix `manual-drain-request`. For example, `manual-drain-request/any-data/power-off: "Some comment"`. Annotation's key becomes `task.Failure`, in this case `manual-drain-request/any-data/power-off`. Last part of annotation's key (after last `/`) becomes `task.Action` (affects task processing speed), in this case `power-off`. Annotation's value becomes `task.Comment`, in this case `Some comment`.

The simplest way to create manual task is annotation `manual-drain-request`. `task.Action` will be `reboot`. Kubectl command (comment can be set in quotes):
```
kubectl annotate node my-node manual-drain-request=""
```

Kubectl command for task removing:
```
kubectl annotate node my-node manual-drain-request-
```

## Automated
Automated tasks are created by annotation with key prefix `node-drain-agent` and value equal to one of: `FAILED`, `CRITICAL`, `ERROR`, `WARNING`. For example, `node-drain-agent/hw_watcher.mem.status: "FAILED"`. Full annotation becomes `task.Comment`, `task.Action` becomes `reboot`, `task.Failure` becomes `node_drain_agent_check`. CMS will create the task and will not delete it until k8s-node has any annotation like this.
Automated tasks' processing rules can be changed in [internal/k8s/agent_checks.go](../internal/k8s/agent_checks.go).
