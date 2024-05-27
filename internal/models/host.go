package models

import (
	"go.ytsaurus.tech/yt/admin/cms/internal/discovery"
	"go.ytsaurus.tech/yt/admin/cms/internal/startrek"
	"go.ytsaurus.tech/yt/go/ypath"
)

type HostProcessingState string

const (
	HostStateAccepted       HostProcessingState = "accepted"
	HostStateDecommissioned HostProcessingState = "decommissioned"
	HostStateProcessed      HostProcessingState = "processed"
	HostStateFinished       HostProcessingState = "finished"
)

type Host struct {
	Host string `json:"host" yson:"host"`

	State HostProcessingState `json:"state" yson:"state"`

	Roles map[ypath.Path]*Component `json:"roles" yson:"role_states"`

	Tickets []startrek.TicketKey `json:"tickets" yson:"tickets"`
}

func (h *Host) UpdateRoles(components discovery.Components) (changed bool) {
	// Add new roles.
	for path, c := range components {
		if _, ok := h.Roles[path]; !ok {
			h.Roles[path] = NewComponent(c)
			changed = true
		}
	}

	// Remove missing roles.
	if len(components) > 0 {
		for path := range h.Roles {
			if _, ok := components[path]; !ok {
				delete(h.Roles, path)
				changed = true
			}
		}
	}

	return
}

// AllowAsForeign marks non-yt host as allowed to be given to walle.
func (h *Host) AllowAsForeign() {
	h.SetProcessed()
}

func (h *Host) SetDecommissioned() {
	h.State = HostStateDecommissioned
}

func (h *Host) SetProcessed() {
	h.State = HostStateProcessed
}

func (h *Host) SetFinished() {
	h.State = HostStateFinished
}

func (h *Host) AllRolesFinished() bool {
	for _, r := range h.Roles {
		if !r.Finished() {
			return false
		}
	}
	return true
}

func (h *Host) AllRolesProcessed() bool {
	for _, r := range h.Roles {
		if !r.Processed() {
			return false
		}
	}
	return true
}

func (h *Host) AllRolesDecommissioned() bool {
	for _, r := range h.Roles {
		if !r.Decommissioned() && !r.Processed() {
			return false
		}
	}
	return true
}

func (h *Host) AddTicket(k startrek.TicketKey) bool {
	if k == "" {
		return false
	}
	for _, t := range h.Tickets {
		if k == t {
			return false
		}
	}
	h.Tickets = append(h.Tickets, k)
	return true
}
