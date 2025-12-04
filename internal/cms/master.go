package cms

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/startrek"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytsys"
)

const CMSRobot = "robot-yt-cms"

//go:generate mockgen -destination=startrek_client_mock.go -package=cms . StartrekClient

type StartrekClient interface {
	CreateTicket(ctx context.Context, t *startrek.Ticket) (startrek.TicketKey, error)
	StartProgress(ctx context.Context, key startrek.TicketKey, assignee string) error
	CloseTicket(ctx context.Context, key startrek.TicketKey) error
	LinkTicket(ctx context.Context, source, target startrek.TicketKey, r startrek.TicketRelationship) error
}

func (p *TaskProcessor) processMaster(ctx context.Context, r *models.Master) {
	task := ctx.Value(taskKey).(*models.Task)
	p.l.Debug("processing master", p.masterLogFields(task, r)...)

	if task.DeletionRequested {
		p.l.Debug("walle deleted task -> setting master role as finished", p.masterLogFields(task, r)...)
		p.activateMaster(ctx, r)
		return
	}

	if task.ProcessingState == models.StateConfirmedManually {
		p.ensureMasterMaintenance(ctx, task, r)
	}

	switch r.State {
	case models.MasterStateAccepted:
		p.processAcceptedMaster(ctx, task, r)
		return
	case models.MasterStateTicketMade:
		p.processPendingMaster(ctx, task, r)
		return
	case models.MasterStateProcessed:
		p.ensureMasterMaintenance(ctx, task, r)
		return
	}
}

func (p *TaskProcessor) processAcceptedMaster(ctx context.Context, task *models.Task, r *models.Master) {
	if !p.conf.EnableFollowerProcessing {
		p.createTicket(ctx, task, r)
		return
	}

	if s, err := p.dc.GetHydraState(ctx, r.Path); err == nil {
		if s.ActiveLeader || s.State == ytsys.PeerStateLeading {
			p.createTicket(ctx, task, r)
			return
		}
	}

	p.processFollower(ctx, task, r)
}

func (p *TaskProcessor) processPendingMaster(ctx context.Context, task *models.Task, r *models.Master) {
	p.linkMasterRelatedTickets(ctx, task, r)
	p.processFollower(ctx, task, r)
}

func (p *TaskProcessor) processFollower(ctx context.Context, task *models.Task, r *models.Master) {
	if !p.conf.EnableFollowerProcessing {
		p.l.Debug("follower processing disabled", p.masterLogFields(task, r)...)
		return
	}

	f, ok := p.getPendingFollower(ctx, task, r)
	if !ok {
		p.l.Debug("no followers to process", p.masterLogFields(task, r)...)
		return
	}

	if f != r.Addr.String() {
		p.l.Debug("skipping follower in favor of another",
			p.masterLogFields(task, r, log.String("pending_follower", f))...)
		return
	}

	if !p.checkCellHealth(ctx, task, r) {
		p.l.Debug("skipping master processing", p.masterLogFields(task, r)...)
		return
	}

	p.ensureMasterMaintenance(ctx, task, r)

	if r.TicketKey != "" {
		p.startProcessingTicket(ctx, task, r)
	}

	p.l.Info("allowing walle to take master", p.masterLogFields(task, r)...)
	r.AllowWalle()
	p.tryUpdateTaskInStorage(ctx, task)
}

func (p *TaskProcessor) createTicket(ctx context.Context, t *models.Task, r *models.Master) {
	if r.TicketKey != "" {
		p.l.Debug("startrek ticket already exists", p.masterLogFields(t, r)...)
		return
	}

	role := ytsys.ClusterRole("master")
	if c, ok := p.resolveMaster(t, r); ok {
		role = c.GetRole()
	}

	description := startrek.MustExecuteTemplate(startrek.ConfirmMasterTicketTemplate, struct {
		*models.Task
		Proxy                    string
		EnableFollowerProcessing bool
		CellID                   yt.NodeID
		ClusterRole              ytsys.ClusterRole
	}{Task: t, Proxy: p.conf.Proxy, EnableFollowerProcessing: p.conf.EnableFollowerProcessing, CellID: r.CellID, ClusterRole: role})

	hosts := strings.Join(t.Hosts, " ")

	ticket := &startrek.Ticket{
		Description: description,
		Queue:       &startrek.Queue{Key: p.Conf().StartrekConfig.Queue},
		Summary:     fmt.Sprintf("Заявка на обслуживание оборудования кластера %s: %s (%s)", p.conf.Proxy, hosts, role),
	}

	key, err := p.startrekClient.CreateTicket(ctx, ticket)
	if err != nil {
		p.l.Error("startrek ticket creation failed", p.masterLogFields(t, r, log.Error(err))...)
		p.startrekErrors.Inc()
		return
	}

	p.l.Info("created startrek ticket for master", p.masterLogFields(t, r, log.Any("ticket_key", key))...)

	r.OnTicketCreated(key)
	p.tryUpdateTaskInStorage(ctx, t)
}

func (p *TaskProcessor) linkMasterRelatedTickets(ctx context.Context, task *models.Task, r *models.Master) {
	for _, h := range task.HostStates {
		for _, t := range h.Tickets {
			err := p.startrekClient.LinkTicket(ctx, r.TicketKey, t, startrek.RelationRelates)
			if err != nil {
				p.l.Error("unable to link startrek ticket",
					p.masterLogFields(task, r, log.Any("ticket", t), log.Error(err))...)
				if !errors.Is(err, startrek.ErrAlreadyRelated) {
					p.startrekErrors.Inc()
				}
			} else {
				p.l.Info("linked startrek ticket", p.masterLogFields(task, r, log.Any("ticket", t))...)
			}
		}
	}
}

func (p *TaskProcessor) ensureMasterMaintenance(ctx context.Context, task *models.Task, r *models.Master) {
	master, ok := p.resolveMaster(task, r)
	if !ok {
		p.l.Error("unable to find master", p.masterLogFields(task, r)...)
		return
	}

	if !master.HasMaintenanceAttr() {
		p.l.Debug("starting master maintenance", p.masterLogFields(task, r)...)

		req := r.MaintenanceRequest
		if req == nil {
			req = p.makeMaintenanceRequest(task)
		}

		if err := p.dc.SetMaintenance(ctx, master, req); err != nil {
			p.l.Error("error starting master maintenance",
				p.masterLogFields(task, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return
		}

		p.l.Info("master maintenance started", p.masterLogFields(task, r)...)
		r.StartMaintenance(req)
		p.tryUpdateTaskInStorage(ctx, task)
	}
}

func (p *TaskProcessor) startProcessingTicket(ctx context.Context, task *models.Task, r *models.Master) {
	p.l.Debug("updating status of the startrek ticket for master", p.masterLogFields(task, r)...)

	decisionMaker := CMSRobot
	if task.DecisionMaker != "" {
		decisionMaker = string(task.DecisionMaker)
	}

	if err := p.startrekClient.StartProgress(ctx, r.TicketKey, decisionMaker); err != nil {
		p.l.Error("startrek ticket status editing failed", p.masterLogFields(task, r, log.Error(err))...)
		p.startrekErrors.Inc()
		return
	}

	p.l.Debug("updated status of the startrek ticket for master", p.masterLogFields(task, r)...)
}

func (p *TaskProcessor) activateMaster(ctx context.Context, r *models.Master) {
	task := ctx.Value(taskKey).(*models.Task)

	master, ok := p.resolveMaster(task, r)
	if !ok {
		p.l.Error("unable to find master", p.masterLogFields(task, r)...)
		return
	}

	if !p.hasTaskUpgrade(task) {
		p.l.Debug("master has no upgrade tasks", p.masterLogFields(task, r)...)
		if !p.checkMasterUp(ctx, task, r) {
			p.l.Debug("waiting for master to become active", p.masterLogFields(task, r)...)
			return
		}
	}

	hasMaintenanceAttr, err := p.dc.HasMaintenanceAttr(ctx, master)
	if err != nil {
		p.l.Error("master maintenance status unknown", p.masterLogFields(task, r, log.Error(err))...)
		return
	}

	if hasMaintenanceAttr {
		p.l.Debug("finishing master maintenance", p.masterLogFields(task, r)...)
		if err := p.dc.UnsetMaintenance(ctx, master, r.MaintenanceRequest.GetID()); err != nil {
			p.l.Error("error finishing master maintenance",
				p.masterLogFields(task, r, log.Error(err))...)
			p.failedMaintenanceRequestUpdates.Inc()
			return
		}
		p.l.Info("master maintenance finished", p.masterLogFields(task, r)...)
	}

	if r.InMaintenance {
		p.l.Info("master maintenance finished in storage", p.masterLogFields(task, r)...)
		r.FinishMaintenance()
		p.tryUpdateTaskInStorage(ctx, task)
	}

	r.SetFinished()
	p.tryUpdateTaskInStorage(ctx, task)
	p.closeTicket(ctx, task, r)
}

func (p *TaskProcessor) checkMasterUp(ctx context.Context, task *models.Task, r *models.Master) bool {
	s, err := p.dc.GetHydraState(ctx, r.Path)
	if err != nil {
		p.l.Debug("hydra state unknown", p.masterLogFields(task, r, log.Error(err))...)
	} else {
		p.l.Debug("got hydra state", p.masterLogFields(task, r, log.Any("hydra_state", s))...)
	}

	if err == nil && (s.State == ytsys.PeerStateFollowing || s.State == ytsys.PeerStateLeading) {
		return true
	}

	timeAfterDeletionRequest := time.Since(time.Time(task.DeletionRequestedAt))
	if timeAfterDeletionRequest < p.conf.MaintenanceAttrRemoveTimeout {
		p.l.Debug("waiting for master to become active", p.masterLogFields(task, r,
			log.Time("deletion_requested_at", time.Time(task.DeletionRequestedAt)),
			log.Duration("time_after_deletion_request", timeAfterDeletionRequest),
			log.Duration("maintenance_attr_remove_timeout", p.conf.MaintenanceAttrRemoveTimeout),
		)...)
		return false
	}

	return true
}

func (p *TaskProcessor) closeTicket(ctx context.Context, task *models.Task, r *models.Master) {
	p.l.Debug("closing startrek ticket for master", p.masterLogFields(task, r)...)

	if err := p.startrekClient.CloseTicket(ctx, r.TicketKey); err != nil {
		p.l.Error("startrek ticket closing failed", p.masterLogFields(task, r, log.Error(err))...)
		p.startrekErrors.Inc()
		return
	}

	p.l.Info("closed startrek ticket for master", p.masterLogFields(task, r)...)
}

func (p *TaskProcessor) makeMaintenanceRequest(task *models.Task) *ytsys.MaintenanceRequest {
	actionTime := time.Now().UTC().Format(banMessageTimeLayout)
	comment := fmt.Sprintf("Maintenance requested by %s at %s; wall-e task_id: %s;",
		models.DecisionMakerCMS, actionTime, task.ID)

	issuer := string(models.DecisionMakerCMS)
	req := ytsys.NewMaintenanceRequest(issuer, issuer, comment, map[string]any{
		"walle_task_id": task.ID,
		"action":        task.Action,
		"issuer":        task.Issuer,
	})

	return req
}

func (p *TaskProcessor) resolveMaster(t *models.Task, r *models.Master) (masterComponent, bool) {
	c, ok := p.cluster.GetComponent(r.Path)
	if !ok {
		p.l.Error("unable to resolve master component", p.masterLogFields(t, r)...)
		return nil, false
	}
	return p.convertMasterComponent(c)
}

func (p *TaskProcessor) convertMasterComponent(c ytsys.Component) (masterComponent, bool) {
	if m, ok := c.(*ytsys.PrimaryMaster); ok {
		return m, true
	}
	if m, ok := c.(*ytsys.SecondaryMaster); ok {
		return m, true
	}
	m, ok := c.(*ytsys.TimestampProvider)
	return m, ok
}

// getPendingFollower returns next follower to be processed.
//
// If there are no followers with @maintenance set the one with min task id is chosen.
// If there is exactly one follower with @maintenance set it is returned.
// Otherwise, false is returned.
//
// Argument r is used to resolve master cell and in logging.
func (p *TaskProcessor) getPendingFollower(ctx context.Context, task *models.Task, r *models.Master) (string, bool) {
	leader, followers, err := p.resolveMasterCell(ctx, task, r)
	if err != nil {
		p.l.Error("unable to resolve master cell", p.masterLogFields(task, r, log.Error(err))...)
		return "", false
	}

	var inMaintenance []masterComponent
	for _, m := range followers {
		if ok, err := p.dc.HasMaintenanceAttr(ctx, m); err != nil || bool(ok) || m.HasMaintenanceAttr() {
			inMaintenance = append(inMaintenance, m)
			continue
		}
	}

	if len(inMaintenance) > 1 {
		p.l.Debug("can not process follower; multiple masters have maintenance attr", p.masterLogFields(task, r,
			log.String("first_master_addr", inMaintenance[0].GetAddr().String()),
			log.String("second_master_addr", inMaintenance[1].GetAddr().String()),
		)...)
		return "", false
	}

	if len(inMaintenance) == 1 {
		return inMaintenance[0].GetAddr().String(), true
	}

	cellPath, err := p.getMasterCellPath(r)
	if err != nil {
		p.l.Error("unable to resolve master cell", p.masterLogFields(task, r, log.Error(err))...)
		return "", false
	}

	tasks := p.getMasterCellTasks(cellPath)
	for _, t := range tasks {
		for _, m := range t.GetMasters() {
			if !strings.HasPrefix(m.Path.String(), cellPath.String()) {
				continue
			}
			if m.Addr.String() == leader.GetAddr().String() {
				continue
			}
			return m.Addr.String(), true
		}
	}

	return "", false
}

func (p *TaskProcessor) getMasterCellTasks(cellPath ypath.Path) []*models.Task {
	tasks, ok := p.taskCache.MasterCellTasks[cellPath]
	if !ok {
		return nil
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID < tasks[j].ID
	})

	return tasks
}

func (p *TaskProcessor) checkCellHealth(ctx context.Context, task *models.Task, r *models.Master) bool {
	leader, followers, err := p.resolveMasterCell(ctx, task, r)
	if err != nil {
		p.l.Error("unable to resolve master cell", p.masterLogFields(task, r, log.Error(err))...)
		return false
	}

	if leader.GetAddr().String() == r.Addr.String() {
		p.l.Error("can not process leader", p.masterLogFields(task, r)...)
		return false
	}

	if leader.HasMaintenanceAttr() {
		p.l.Debug("can not process follower; leader has maintenance attr", p.masterLogFields(task, r,
			log.String("master_addr", leader.GetAddr().String()),
		)...)
		return false
	}

	if len(followers) < 2 {
		p.l.Error("can not process follower; too few followers left")
		return false
	}

	for _, m := range followers {
		if m.GetAddr().String() == r.Addr.String() {
			continue
		}

		if m.HasMaintenanceAttr() {
			p.l.Debug("can not process follower; other master has maintenance attr", p.masterLogFields(task, r,
				log.String("master_addr", m.GetAddr().String()),
			)...)
			return false
		}

		if ok, err := p.dc.HasMaintenanceAttr(ctx, m); err != nil || ok {
			p.l.Debug("can not process follower; other master has maintenance attr", p.masterLogFields(task, r,
				log.String("master_addr", m.GetAddr().String()),
			)...)
			return false
		}

		s, err := p.dc.GetHydraState(ctx, m.GetCypressPath())
		if err != nil {
			p.l.Error("hydra state unknown", p.masterLogFields(task, r,
				log.String("master_addr", m.GetAddr().String()),
				log.Error(err))...)
			return false
		}

		if s.ReadOnly {
			p.l.Debug("can not process follower; other master is in read_only state",
				p.masterLogFields(task, r, log.String("master_addr", m.GetAddr().String()))...)
			return false
		}

		if s.State != ytsys.PeerStateFollowing {
			p.l.Debug("can not process follower; other master is in a bad state",
				p.masterLogFields(task, r,
					log.String("master_addr", m.GetAddr().String()),
					log.String("state", string(s.State)))...)
			return false
		}
	}

	return true
}

func (p *TaskProcessor) resolveMasterCell(
	ctx context.Context,
	task *models.Task,
	r *models.Master,
) (leader masterComponent, followers []masterComponent, err error) {
	cellPath, err := p.getMasterCellPath(r)
	if err != nil {
		return nil, nil, err
	}

	components, err := p.cluster.GetMasterCell(cellPath)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to retrieve master cell: %w", err)
	}

	for _, c := range components {
		m, ok := p.convertMasterComponent(c)
		if !ok {
			p.l.Error("unexpected master component", p.masterLogFields(task, r,
				log.String("master_addr", c.GetCypressPath().String()),
			)...)
			return nil, nil, xerrors.Errorf("unable to retrieve master component")
		}

		s, err := p.dc.GetHydraState(ctx, m.GetCypressPath())
		if err != nil {
			if m.GetCypressPath().String() != r.Path.String() {
				p.l.Debug("hydra state unknown", p.masterLogFields(task, r,
					log.String("master_addr", m.GetCypressPath().String()),
					log.Error(err))...)
				return nil, nil, xerrors.Errorf("hydra state for %q is unknown: %w", m.GetCypressPath().String(), err)
			}
			followers = append(followers, m)
			continue
		}

		if s.ActiveLeader {
			if leader == nil {
				leader = m
			} else {
				err := xerrors.Errorf("multiple leaders: %q and %q",
					leader.GetCypressPath().String(), m.GetCypressPath().String())
				return nil, nil, err
			}
			if s.State != ytsys.PeerStateLeading {
				err := xerrors.Errorf("leader %q is in a bad state %q",
					leader.GetCypressPath().String(), s.State)
				return nil, nil, err
			}
		} else {
			followers = append(followers, m)
		}
	}

	if leader == nil {
		return nil, nil, xerrors.Errorf("leader not found")
	}

	return leader, followers, nil
}

// getMasterCellPath returns cypress master cell path of given master.
func (p *TaskProcessor) getMasterCellPath(r *models.Master) (ypath.Path, error) {
	cellPath, _, err := ypath.Split(r.Path)
	if err != nil {
		return "", xerrors.Errorf("unable to resolve master cell path: %w", err)
	}
	return cellPath, nil
}

// masterLogFields creates a slice with task and master fields to log.
func (p *TaskProcessor) masterLogFields(t *models.Task, n *models.Master, extra ...log.Field) []log.Field {
	fields := []log.Field{
		log.String("task_id", string(t.ID)),
		log.String("host", n.Host),
		log.String("addr", n.Addr.String()),
		log.String("path", n.Path.String()),
		log.Bool("group_task", t.IsGroupTask),
		log.String("group_id", t.MaintenanceInfo.NodeSetID),
	}
	fields = append(fields, extra...)
	return fields
}

type masterComponent interface {
	ytsys.Component
	HasMaintenanceAttr() bool
}
