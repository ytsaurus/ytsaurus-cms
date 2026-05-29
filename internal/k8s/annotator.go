package k8s

import (
	"context"
	"encoding/json"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/cms"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	cmsStatusAnnotation          = "yt-cms/status"
	defaultAnnotatorUpdatePeriod = 10 * time.Second
)

type NodeAnnotatorConfig struct {
	UpdatePeriod time.Duration
}

type NodeAnnotator struct {
	conf    *NodeAnnotatorConfig
	l       log.Structured
	storage Storage
	cs      *kubernetes.Clientset
	poller  *Poller
}

func NewNodeAnnotator(conf *NodeAnnotatorConfig, l log.Structured, storage cms.Storage, cs *kubernetes.Clientset, poller *Poller) *NodeAnnotator {
	if conf.UpdatePeriod == 0 {
		conf.UpdatePeriod = defaultAnnotatorUpdatePeriod
	}
	return &NodeAnnotator{
		conf:    conf,
		l:       l,
		storage: storage,
		cs:      cs,
		poller:  poller,
	}
}

// Run starts periodical process that synchronizes tasks from storage
// with cms status annotations on nodes.
func (a *NodeAnnotator) Run(ctx context.Context) error {
	t := time.NewTicker(a.conf.UpdatePeriod)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if err := a.annotateNodes(ctx); err != nil {
				a.l.Error("node annotation loop failed", log.Error(err),
					log.Duration("next_update_after", a.conf.UpdatePeriod))
				break
			}
			a.l.Debug("node annotation loop succeeded",
				log.Duration("next_update_after", a.conf.UpdatePeriod))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// annotateNodes actualizes `yt-cms/status` node annotations according to tasks' [models.Task.ProcessingState].
//
// If [models.Task.ProcessingState] == [models.StateConfirmedManually], CMS annotates node with `yt-cms/status: processed`.
func (a *NodeAnnotator) annotateNodes(ctx context.Context) error {
	a.l.Debug("retrieving tasks")
	tasks, err := a.storage.GetAll(ctx)
	if err != nil {
		return err
	}
	a.l.Debug("retrieved tasks", log.Int("count", len(tasks)), log.Any("tasks", tasks))

	nodes, err := a.poller.GetNodes()
	if err != nil {
		return err
	}
	a.l.Debug("retrieved nodes", log.Int("count", len(nodes.Items)), log.Any("nodes", nodes))

	tasksByNode := make(map[string][]*models.Task)
	for _, t := range tasks {
		if t.Origin != models.OriginWalle {
			continue
		}
		node := t.Hosts[0]
		tasksByNode[node] = append(tasksByNode[node], t)
	}

	// Actualizing nodes' annotations according to task status.
	var lastError error
	for _, n := range nodes.Items {
		ts := tasksByNode[n.Name]
		mustBe := ""
		if len(ts) > 0 {
			mustBe = string(ts[0].ProcessingState)
			if ts[0].ProcessingState == models.StateConfirmedManually {
				mustBe = string(models.StateProcessed)
			}
		}
		status, annotated := n.Annotations[cmsStatusAnnotation]
		if mustBe == "" && annotated {
			if err := a.removeNodeAnnotation(ctx, n.Name, cmsStatusAnnotation); err != nil {
				a.l.Error("failed to remove node status annotation", log.Any("node", n), log.Error(err))
				lastError = err
			}
		} else if status != mustBe {
			if err := a.annotateNode(ctx, n.Name, cmsStatusAnnotation, mustBe); err != nil {
				a.l.Error("failed to annotate node status", log.Any("must_be", mustBe), log.Any("node", n), log.Error(err))
				lastError = err
			}
		}
	}
	return lastError
}

type Metadata struct {
	Annotations map[string]any `json:"annotations"`
}
type Patch struct {
	Metadata `json:"metadata"`
}

func (a *NodeAnnotator) annotateNode(ctx context.Context, nodeName, key, value string) error {
	patchBytes, err := json.Marshal(Patch{Metadata: Metadata{Annotations: map[string]any{key: value}}})
	if err != nil {
		return err
	}
	_, err = a.cs.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

func (a *NodeAnnotator) removeNodeAnnotation(ctx context.Context, nodeName, key string) error {
	patchBytes, err := json.Marshal(Patch{Metadata: Metadata{Annotations: map[string]any{key: nil}}})
	if err != nil {
		return err
	}
	_, err = a.cs.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}
