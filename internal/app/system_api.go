package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.uber.org/atomic"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/yt/admin/cms/internal/cms"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"go.ytsaurus.tech/yt/go/ypath"
	"golang.org/x/xerrors"
)

const taskIDURLParam = "taskID"

type SystemAPIConfig struct {
	Proxy string `yaml:"-"`
}

// SystemAPI implements CMS API v1.4.
type SystemAPI struct {
	conf    *SystemAPIConfig
	l       log.Structured
	storage cms.Storage

	ready atomic.Bool

	// Metrics.
	wrongClusterRequests metrics.Counter
}

// NewSystemAPI creates new system api.
func NewSystemAPI(conf *SystemAPIConfig, l log.Structured, s cms.Storage) *SystemAPI {
	return &SystemAPI{
		conf:    conf,
		l:       l,
		storage: s,
	}
}

// Routes creates a REST router for the CMS API v1.4.
func (a *SystemAPI) Routes() chi.Router {
	r := chi.NewRouter()

	r.Route("/tasks", func(r chi.Router) {
		r.Use(waitReady(&a.ready))

		r.Post("/", a.addTask)
		r.Get("/", a.getTasks)

		r.Route(fmt.Sprintf("/{%s}", taskIDURLParam), func(r chi.Router) {
			r.Get("/", a.getTask)
			r.Delete("/", a.deleteTask)
		})
	})

	return r
}

func (a *SystemAPI) RegisterMetrics(r metrics.Registry) {
	a.wrongClusterRequests = r.Counter("wrong_cluster_requests")
}

func (a *SystemAPI) SetReady() {
	a.ready.Store(true)
	a.l.Info("CMS API is ready to serve!")
}

// addTask implements "POST /tasks" from CMS API v1.4.
func (a *SystemAPI) addTask(w http.ResponseWriter, r *http.Request) {
	var req walle.AddTaskRequest
	if err := parseJSONRequest(r, walle.PostTaskRequestSchema, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	task := &models.Task{
		Task:            &req,
		Origin:          models.OriginWalle,
		ProcessingState: models.StateNew,
		HostStates:      make(map[string]*models.Host),
		WalleStatus:     walle.StatusInProcess,
	}
	for _, h := range req.Hosts {
		task.HostStates[h] = &models.Host{
			Host:  h,
			State: models.HostStateAccepted,
			Roles: make(map[ypath.Path]*models.Component),
		}
	}

	dryRun := r.URL.Query().Get("dry_run") == "true"
	a.l.Info(fmt.Sprintf("dry_run: %t", dryRun))

	w.Header().Set("Content-Type", "application/json")

	if dryRun {
		resp := &walle.GetTaskResponse{
			Task:    &req,
			Status:  task.WalleStatus,
			Message: task.WalleStatusDescription,
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(mustMarshalJSON(resp))
		return
	}

	if err := a.storage.Add(r.Context(), task); err != nil {
		internalError(w, err)
		return
	}

	chi.RouteContext(r.Context()).URLParams.Add(taskIDURLParam, string(req.ID))
	a.getTask(w, r)
}

// getTasks implements "GET /tasks" from CMS API v1.4.
func (a *SystemAPI) getTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	tasks, err := a.storage.GetAll(r.Context())
	if err != nil {
		internalError(w, err)
		return
	}

	// Convert to Walle's format.
	results := make([]*walle.GetTaskResponse, 0, len(tasks))
	for _, t := range tasks {
		if t.Origin != models.OriginWalle && t.Origin != "" {
			continue
		}
		if t.DeletionRequested {
			continue
		}
		results = append(results, &walle.GetTaskResponse{
			Task:    t.Task,
			Status:  t.WalleStatus,
			Message: t.WalleStatusDescription,
		})
	}
	resp := &walle.GetTasksResponse{Result: results}

	data := mustMarshalJSON(resp)
	err = walle.Check(walle.GetTasksResponseSchema, data)
	if err != nil {
		panic(xerrors.Errorf("invalid response JSON %s: %w", data, err))
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// getTask implements "GET /tasks/<task_id>" from CMS API v1.4.
func (a *SystemAPI) getTask(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	taskID := chi.URLParam(r, taskIDURLParam)
	task, err := a.storage.Get(r.Context(), walle.TaskID(taskID))
	if err != nil {
		if errors.Is(err, cms.TaskNotFoundErr) {
			http.NotFound(w, r)
		} else {
			internalError(w, err)
		}
		return
	}

	resp := &walle.GetTaskResponse{
		Task:    task.Task,
		Status:  task.WalleStatus,
		Message: task.WalleStatusDescription,
	}

	data := mustMarshalJSON(resp)
	err = walle.Check(walle.GetTaskResponseSchema, data)
	if err != nil {
		panic(err)
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// deleteTask implements "DELETE /tasks/<task_id>" from CMS API v1.4.
func (a *SystemAPI) deleteTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, taskIDURLParam)
	err := a.storage.Delete(r.Context(), walle.TaskID(taskID))
	if err != nil {
		if errors.Is(err, cms.TaskNotFoundErr) {
			http.NotFound(w, r)
		} else {
			w.Header().Set("Content-Type", "application/json")
			internalError(w, err)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// parseJSONRequest extracts (to out) and validates (against schema) json request.
func parseJSONRequest(r *http.Request, schema string, out interface{}) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return xerrors.Errorf("can't read body %w", err)
	}

	err = walle.Check(schema, body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, out)
}

// internalError replies to request with specified internal error
// encoded into Wall-e's format.
func internalError(w http.ResponseWriter, err error) {
	resp := mustMarshalJSON(walle.NewErrorResponse(err.Error()))
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write(resp)
}

// mustMarshalJSON returns JSON encoding of v or panics in case of any error.
func mustMarshalJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
