package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.uber.org/atomic"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/yt/admin/cms/internal/cms"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	OAuthClientID     = "db9b9641566f4d718e31090a25de19ff"
	OAuthClientSecret = "d31d64c29eb9402c8afc9c1c845bd58a"

	sysUsers = ypath.Path("//sys/users")
)

// API provides http endpoints to interact with the service.
type API struct {
	w LeaderWatcher
	// bb      blackbox.Client
	cliAuth auth
	yc      yt.Client
	storage cms.Storage
	cluster models.Cluster

	l log.Structured

	ready atomic.Bool

	// Metrics.
	failedManualConfirmations   metrics.Counter
	timedOutManualConfirmations metrics.Counter
}

// NewAPI creates new API.
func NewAPI(w LeaderWatcher, cliAuth auth, yc yt.Client, s cms.Storage, c models.Cluster, l log.Structured) *API {
	return &API{w: w, cliAuth: cliAuth, yc: yc, storage: s, cluster: c, l: l}
}

func (a *API) Routes() chi.Router {
	r := chi.NewRouter()

	r.Route("/ready", func(r chi.Router) {
		r.Use(waitReady(&a.ready))
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {})
	})

	var apiRouter chi.Router
	if a.cliAuth != nil {
		apiRouter = a.cliAuth(r, a.yc, a.l)
	} else {
		apiRouter = r
	}

	apiRouter.Route("/tasks", func(r chi.Router) {
		r.Use(waitReady(&a.ready))

		r.Get("/", a.getTasks)

		r.Route("/confirm", func(r chi.Router) {
			r.Use(ForwardToLeader(a.w, a.l))
			r.Post("/", a.confirmTask)
		})
	})

	return r
}

func (a *API) RegisterMetrics(r metrics.Registry) {
	a.failedManualConfirmations = r.Counter("failed_manual_confirmations")
	a.timedOutManualConfirmations = r.Counter("timed_out_manual_confirmations")
}

func (a *API) SetReady() {
	a.ready.Store(true)
	a.l.Info("api is ready to serve!")
}

// getTasks returns active cms tasks.
func (a *API) getTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	tasks, err := a.storage.GetAll(r.Context())
	if err != nil {
		internalError(w, err)
		return
	}

	// Convert to Walle's format.
	results := make([]*walle.GetTaskResponse, 0, len(tasks))
	for _, t := range tasks {
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

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// confirmTask marks task as confirmed.
//
// Actual confirmation will happen on the next task processing loop.
func (a *API) confirmTask(w http.ResponseWriter, r *http.Request) {
	requester, ok := ContextRequester(r.Context())
	if !ok {
		ctxlog.Error(r.Context(), a.l.Logger(), "missing requester")
		http.Error(w, "missing requester", http.StatusForbidden)
		return
	}

	request := &ConfirmationRequest{}
	if err := json.NewDecoder(r.Body).Decode(request); err != nil {
		ctxlog.Error(r.Context(), a.l.Logger(), "error reading confirmation request", log.Error(err))
		http.Error(w, "error reading confirmation request", http.StatusBadRequest)
		return
	}

	var responses ConfirmationResponses

	taskIDs := request.TaskIDs
	for _, h := range request.Hosts {
		tasks, err := a.storage.GetHostTasks(r.Context(), h)
		if err != nil {
			ctxlog.Error(r.Context(), a.l.Logger(), "unable to get host tasks",
				log.String("host", h), log.Error(err))
			responses = append(responses, &ConfirmationResponse{Host: h, Error: err.Error()})
			continue
		}

		if len(tasks) == 0 {
			ctxlog.Error(r.Context(), a.l.Logger(), "no unconfirmed active tasks found for the host",
				log.String("host", h), log.Error(err))
			responses = append(responses, &ConfirmationResponse{
				Host:  h,
				Error: "no unconfirmed active tasks found for the host",
			})
			continue
		}

		for _, t := range tasks {
			taskIDs = append(taskIDs, t.ID)
		}
	}

	for _, taskID := range taskIDs {
		var task *models.Task
		var err error

		if request.DryRun {
			task, err = a.storage.Get(r.Context(), taskID)
		} else {
			task, err = a.storage.Confirm(r.Context(), taskID, requester)
		}

		if err != nil {
			if errors.Is(err, cms.TaskNotFoundErr) {
				ctxlog.Error(r.Context(), a.l.Logger(), "task not found",
					log.String("task_id", string(taskID)), log.Error(err))
				responses = append(responses, &ConfirmationResponse{TaskID: taskID, Error: "task id not found"})
				continue
			}

			ctxlog.Error(r.Context(), a.l.Logger(), "manual confirmation failed", log.Error(err))
			a.failedManualConfirmations.Inc()
			responses = append(responses, &ConfirmationResponse{TaskID: taskID, Error: err.Error()})
			continue
		}

		resp := NewSuccessfulConfirmationResponse(task, a.cluster)
		if request.DryRun {
			resp.Confirmed = false
		}
		responses = append(responses, resp)
	}

	data := mustMarshalJSON(responses)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)

	ctxlog.Info(r.Context(), a.l.Logger(), "manual confirmation done",
		log.Any("request", request), log.String("requester", requester),
		log.Any("response", responses))
}

// YTAdmin is a middleware that extracts user login from request's context
// and check that user belongs to cluster's admins group.
func YTAdmin(yc yt.Client, l log.Structured) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			login, ok := ContextRequester(r.Context())
			if !ok {
				ctxlog.Error(r.Context(), l.Logger(), "missing requester")
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}

			req := sysUsers.Child(login).Attr("member_of_closure")
			var groups []string
			if err := yc.GetNode(r.Context(), req, &groups, nil); err != nil {
				http.Error(w, "acl check failed", http.StatusInternalServerError)
				return
			}

			ytAdmin := false
			for _, g := range groups {
				if g == "admins" {
					ytAdmin = true
					break
				}
			}

			if !ytAdmin {
				ctxlog.Error(r.Context(), l.Logger(), "only yt admins can perform this action")
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

type ConfirmationRequest struct {
	DryRun  bool           `json:"dry_run"`
	TaskIDs []walle.TaskID `json:"task_ids,omitempty"`
	Hosts   []string       `json:"hosts,omitempty"`
}

type ConfirmationResponses []*ConfirmationResponse

type ConfirmationResponse struct {
	TaskID       walle.TaskID  `json:"task_id,omitempty"`
	Host         string        `json:"host,omitempty"`
	Confirmed    bool          `json:"confirmed"`
	Comment      string        `json:"comment,omitempty"`
	Error        string        `json:"error,omitempty"`
	Task         *walle.Task   `json:"task,omitempty"`
	ClusterState *clusterState `json:"cluster_state,omitempty"`
}

func NewSuccessfulConfirmationResponse(t *models.Task, c models.Cluster) *ConfirmationResponse {
	var tickets []string
	for _, m := range t.GetMasters() {
		tickets = append(tickets, string(m.TicketKey))
	}
	sort.Strings(tickets)

	comment := "Task confirmation accepted."
	if len(tickets) == 1 {
		comment += fmt.Sprintf(" Ticket %s will be resolved automatically.", tickets[0])
	} else if len(tickets) > 1 {
		comment += fmt.Sprintf(" Tickets [%s] will be resolved automatically.", strings.Join(tickets, ", "))
	}

	return &ConfirmationResponse{
		Task:         t.Task,
		Confirmed:    true,
		Comment:      comment,
		ClusterState: newClusterState(c, t),
	}
}

type clusterState struct {
	LVC int64 `json:"lvc"`
	QMC int64 `json:"qmc"`
	URC int64 `json:"urc"`
	PMC int64 `json:"pmc"`
	DMC int64 `json:"dmc"`

	ClusterInfo models.ClusterInfo `json:"cluster_info,omitempty"`
}

func newClusterState(c models.Cluster, t *models.Task) *clusterState {
	s := &clusterState{}

	if indicators, err := c.GetChunkIntegrity(); err == nil {
		s = &clusterState{
			LVC: indicators.LVC,
			QMC: indicators.QMC,
			URC: indicators.URC,
			PMC: indicators.PMC,
			DMC: indicators.DMC,
		}
	}

	s.ClusterInfo = t.GetClusterInfo(c)

	return s
}
