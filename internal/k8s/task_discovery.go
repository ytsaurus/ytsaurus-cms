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
	TaskAnnotationPrefix = "yt-cms-request" // For example, full annotation: `yt-cms-request/any-issuer: "Comment"`.

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
	HostSuffix   string
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
		if t.Origin == models.OriginK8S {
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

	// Add new tasks.
	for _, node := range nodes.Items {
		for key, value := range node.Annotations {
			host := node.Name + d.conf.HostSuffix
			if findTask(tasks, host, key) != nil {
				continue
			}
			if t := createTask(host, key, value); t != nil {
				plan.Created = append(plan.Created, t)
				continue
			}
		}
	}

	sort.Slice(plan.Created, func(i, j int) bool {
		return plan.Created[i].ID < plan.Created[j].ID
	})

	// Delete finished or canceled tasks.
	for _, task := range tasks {
		if task.Origin != models.OriginK8S {
			continue
		}

		var foundNode *corev1.Node
		for _, h := range task.Hosts {
			node := findNode(nodes, strings.TrimSuffix(h, d.conf.HostSuffix))
			if node != nil {
				foundNode = node
				break
			}
		}
		if foundNode == nil {
			d.l.Info("no such hosts, deleting task", log.Strings("hosts", task.Hosts), log.Any("nodes", nodes), log.Any("task", task))
			plan.Deleted = append(plan.Deleted, task)
			continue
		}

		if _, ok := foundNode.Annotations[task.Failure]; !ok {
			d.l.Info("task annotation is missing, deleting task", log.Any("node", foundNode), log.Strings("hosts", task.Hosts), log.Any("task", task))
			plan.Deleted = append(plan.Deleted, task)
			continue
		}
	}

	sort.Slice(plan.Deleted, func(i, j int) bool {
		return plan.Deleted[i].ID < plan.Deleted[j].ID
	})

	return plan
}

// createTask processes node maintenance requests,
// given as node annotations, and creates task if annotation with [TaskAnnotationPrefix] exists.
//
// For example, given the annotation `yt-cms-request/any-text: "Manual request due to host maintenance: ticket_key"`.
// Annotation key will become [walle.Task.Failure] and value will become [walle.Task.Comment].
// Task action will be [walle.ActionReboot].
func createTask(host, key, value string) *models.Task {
	if strings.HasPrefix(key, TaskAnnotationPrefix) {
		parts := strings.Split(key, "/")
		issuer := TaskAnnotationPrefix
		if len(parts) > 1 {
			issuer = parts[1]
		}
		id := generateTaskID()
		task := &walle.Task{
			ID:      walle.TaskID(id),
			Type:    walle.TaskTypeManual,
			Issuer:  issuer,
			Action:  walle.ActionReboot,
			Hosts:   []string{host},
			Comment: value,
			Failure: key,
			MaintenanceInfo: &walle.MaintenanceInfo{
				NodeSetID: id,
			},
		}
		return newCMSTask(task)
	}
	return nil
}

func newCMSTask(task *walle.Task) *models.Task {
	hosts := map[string]*models.Host{}
	for _, h := range task.Hosts {
		hosts[h] = &models.Host{
			Host:  h,
			State: models.HostStateAccepted,
			Roles: make(map[ypath.Path]*models.Component),
		}
	}
	return &models.Task{
		Task:            task,
		Origin:          models.OriginK8S,
		YPInfo:          &models.YPMaintenanceInfo{},
		ProcessingState: models.StateNew,
		HostStates:      hosts,
		WalleStatus:     walle.StatusInProcess,
	}
}

func generateTaskID() string {
	return uuid.New().String()
}

func findTask(tasks []*models.Task, host, failure string) *models.Task {
	for _, t := range tasks {
		if t.Failure == failure && slices.Contains(t.Hosts, host) {
			return t
		}
	}
	return nil
}

func findNode(nodes *corev1.NodeList, name string) *corev1.Node {
	for _, node := range nodes.Items {
		if name == node.Name {
			return &node
		}
	}
	return nil
}
