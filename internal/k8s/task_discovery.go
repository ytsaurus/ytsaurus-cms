package k8s

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"go.ytsaurus.tech/yt/go/ypath"
	corev1 "k8s.io/api/core/v1"
)

const (
	AgentTaskIssuer        = "node-drain-agent"
	AgentAnnotationPrefix  = AgentTaskIssuer // E.g. `node-drain-agent/hw_watcher.mem.status: "FAILED"`.
	ManualTaskIssuer       = "manual-drain-request"
	ManualAnnotationPrefix = ManualTaskIssuer // E.g. `manual-drain-request/any-data/power-off: "Some comment"`.

	defaultTaskUpdatePeriod    = 10 * time.Second
	defaultTaskDeletionTimeout = 20 * time.Minute
)

type Storage interface {
	// Add stores new task.
	Add(ctx context.Context, task *models.Task) error
	// GetAll returns all CMS tasks.
	GetAll(ctx context.Context) ([]*models.Task, error)
	// Delete removes task with given id.
	Delete(ctx context.Context, id walle.TaskID) error
}

type TaskDiscoveryConfig struct {
	UpdatePeriod time.Duration
}

type TaskDiscovery struct {
	conf    *TaskDiscoveryConfig
	l       log.Structured
	storage Storage
	poller  *Poller
}

func NewTaskDiscovery(conf *TaskDiscoveryConfig, l log.Structured, storage Storage, poller *Poller) *TaskDiscovery {
	if conf.UpdatePeriod == 0 {
		conf.UpdatePeriod = defaultTaskUpdatePeriod
	}
	return &TaskDiscovery{
		conf:    conf,
		l:       l,
		storage: storage,
		poller:  poller,
	}
}

// Run starts periodical process that synchronizes tasks from storage
// with maintenance requests from node cache.
func (d *TaskDiscovery) Run(ctx context.Context) error {
	t := time.NewTicker(d.conf.UpdatePeriod)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if err := d.updateTasks(ctx); err != nil {
				d.l.Error("tasks update failed", log.Error(err),
					log.Duration("next_update_after", d.conf.UpdatePeriod))
				break
			}
			d.l.Debug("tasks update succeeded", log.Duration("next_update_after", d.conf.UpdatePeriod))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// updateTasks retrieves tasks from storage and nodes from node cache,
// creates new tasks in storage for new requests and
// deletes tasks from storage for deleted requests.
func (d *TaskDiscovery) updateTasks(ctx context.Context) error {
	d.l.Debug("retrieving tasks")
	tasks, err := d.storage.GetAll(ctx)
	if err != nil {
		return err
	}
	d.l.Debug("retrieved tasks", log.Int("count", len(tasks)))

	var clusterTasks []*models.Task
	for _, t := range tasks {
		if t.Origin == models.OriginWalle {
			clusterTasks = append(clusterTasks, t)
		}
	}
	d.l.Debug("filtered tasks", log.Int("count", len(clusterTasks)))

	d.l.Debug("retrieving nodes from node cache")
	nodes, err := d.poller.GetNodes()
	if err != nil {
		return err
	}
	d.l.Debug("retrieved nodes from node cache", log.Int("count", len(nodes.Items)))

	plan := d.makeUpdatePlan(clusterTasks, nodes)
	d.l.Debug("made task update plan", log.Any("plan", plan))

	var firstError error
	for _, t := range plan.Created {
		d.l.Debug("adding new task", log.Any("task", t))
		if err := d.storage.Add(ctx, t); err != nil {
			d.l.Error("task addition failed", log.Any("task", t), log.Error(err))
			if firstError == nil {
				firstError = err
			}
		} else {
			for _, host := range t.Hosts {
				d.l.Info("new task added", log.Any("task", t), log.String("host", host))
			}
		}
	}

	for _, t := range plan.Deleted {
		d.l.Debug("deleting task", log.Any("task", t))
		if err := d.storage.Delete(ctx, t.ID); err != nil {
			d.l.Error("task deletion failed", log.Any("task", t), log.Error(err))
			if firstError == nil {
				firstError = err
			}
		} else {
			d.l.Info("task deleted", log.Any("task", t))
		}
	}

	return firstError
}

type UpdatePlan struct {
	// Created stores new tasks.
	Created []*models.Task
	// Deleted stores finished or canceled tasks.
	Deleted []*models.Task
}

// makeUpdatePlan finds difference between maintenance requests
// represented as tasks and node maintenance requests.
func (d *TaskDiscovery) makeUpdatePlan(tasks []*models.Task, nodes *corev1.NodeList) *UpdatePlan {
	plan := &UpdatePlan{}

	tasksByNode := make(map[string][]*models.Task)
	for _, t := range tasks {
		node := t.Hosts[0]
		tasksByNode[node] = append(tasksByNode[node], t)
	}

	// Add new tasks.
	for _, node := range nodes.Items {
		if ts := tasksByNode[node.Name]; len(ts) > 0 {
			continue // Task for this node already exists.
		}

		if t := createManualTask(node); t != nil {
			plan.Created = append(plan.Created, t)
			continue
		}
		if t := createAgentTask(node); t != nil {
			plan.Created = append(plan.Created, t)
			continue
		}
	}

	sort.Slice(plan.Created, func(i, j int) bool {
		return plan.Created[i].ID < plan.Created[j].ID
	})

	// Delete finished or canceled tasks.
	for _, task := range tasks {
		if task.Origin != models.OriginWalle {
			continue
		}

		node := findNode(nodes, task.Hosts[0])
		if node == nil {
			d.l.Info("no such node, deleting task", log.String("node", task.Hosts[0]), log.Any("nodes", nodes), log.Any("task", task))
			plan.Deleted = append(plan.Deleted, task)
			continue
		}

		switch task.Issuer {
		case ManualTaskIssuer:
			if _, ok := node.Annotations[task.Failure]; !ok {
				d.l.Info("manual task annotation is missing, deleting task", log.String("node", task.Hosts[0]), log.Any("task", task))
				plan.Deleted = append(plan.Deleted, task)
				continue
			}
		case AgentTaskIssuer:
			for _, check := range ActiveChecks {
				if checkRes := check(*node); task.Failure == checkRes.Name && !checkRes.ActionRequired {
					d.l.Info("check action is not required, deleting task", log.String("node", task.Hosts[0]), log.Any("check_result", checkRes), log.Any("task", task))
					plan.Deleted = append(plan.Deleted, task)
					continue
				}
			}
		}
	}

	sort.Slice(plan.Deleted, func(i, j int) bool {
		return plan.Deleted[i].ID < plan.Deleted[j].ID
	})

	return plan
}

// createManualTask processes manual node maintenance requests,
// given as node annotations, and creates task if annotation with [ManualAnnotationPrefix] exists.
//
// For example, given the annotation `manual-drain-request/power-off: "Manual request due to host maintenance: ticket_key"`.
// Annotation key will become [walle.Task.Failure] and value will become [walle.Task.Comment].
// Key's part after last '/' will become [walle.Task.Action] (affects node decommission speed). If action is unknown, action will be [walle.ActionReboot].
//
// Between two '/' in annotation key may be any, e.g. `manual-drain-request/any-data/power-off: ...`.
// It can be filled if it is necessary for some reason to create two tasks with same action.
func createManualTask(node corev1.Node) *models.Task {
	for key, value := range node.Annotations {
		if strings.HasPrefix(key, ManualAnnotationPrefix) {
			parts := strings.Split(key, "/")
			action := walle.HostAction(parts[len(parts)-1])
			if !slices.Contains(walle.HostActions, action) {
				action = walle.ActionReboot
			}
			id := generateTaskID()
			task := &walle.Task{
				ID:      walle.TaskID(id),
				Type:    walle.TaskTypeManual,
				Issuer:  ManualTaskIssuer,
				Action:  action,
				Hosts:   []string{node.Name},
				Comment: value,
				Failure: key,
				MaintenanceInfo: &walle.MaintenanceInfo{
					NodeSetID: id,
				},
			}

			return newCMSTask(task)
		}
	}
	return nil
}

// createAgentTask processes agent (automatic) node maintenance requests,
// given as node annotations, and creates task if necessary.
//
// Each check from [ActiveChecks] is done on node and if it's [CheckResult] requires action, new task is returned.
func createAgentTask(node corev1.Node) *models.Task {
	for _, check := range ActiveChecks {
		if checkRes := check(node); checkRes.ActionRequired {
			id := generateTaskID()
			task := &walle.Task{
				ID:      walle.TaskID(id),
				Type:    walle.TaskTypeAutomated,
				Issuer:  AgentTaskIssuer,
				Action:  checkRes.Action,
				Hosts:   []string{node.Name},
				Comment: checkRes.Comment,
				Failure: checkRes.Name,
				MaintenanceInfo: &walle.MaintenanceInfo{
					NodeSetID: id,
				},
			}
			return newCMSTask(task)
		}
	}
	return nil
}

func newCMSTask(task *walle.Task) *models.Task {
	return &models.Task{
		Task:            task,
		Origin:          models.OriginWalle,
		YPInfo:          &models.YPMaintenanceInfo{},
		ProcessingState: models.StateNew,
		HostStates: map[string]*models.Host{
			task.Hosts[0]: &models.Host{
				Host:  task.Hosts[0],
				State: models.HostStateAccepted,
				Roles: make(map[ypath.Path]*models.Component),
			},
		},
		WalleStatus: walle.StatusInProcess,
	}
}

func generateTaskID() string {
	return uuid.New().String()
}

func findNode(nodes *corev1.NodeList, name string) *corev1.Node {
	for _, node := range nodes.Items {
		if name == node.Name {
			return &node
		}
	}
	return nil
}
