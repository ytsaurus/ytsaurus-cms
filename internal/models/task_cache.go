package models

import (
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/ytsys"
)

type SchedulerMap map[ytsys.Addr]*Scheduler

func (m SchedulerMap) CountProcessed() int {
	cnt := 0
	for _, s := range m {
		if s.Processed() {
			cnt++
		}
	}
	return cnt
}

type ControllerAgentMap map[ytsys.Addr]*ControllerAgent

func (m ControllerAgentMap) CountProcessed() int {
	cnt := 0
	for _, a := range m {
		if a.Processed() {
			cnt++
		}
	}
	return cnt
}

type TaskUpgrade struct {
	Old, New *Task
}

// TaskCache stores tasks states grouped by different params.
type TaskCache struct {
	Tasks []*Task

	// TaskUpgrades stores task with max ID for each host
	// for hosts with more than one task.
	TaskUpgrades map[string]*TaskUpgrade

	// MasterCellTasks stores master tasks grouped by cypress cell path.
	//
	// Host with timestamp provider and master will be stored in two copies.
	MasterCellTasks map[ypath.Path][]*Task
	// Schedulers contain all scheduler roles.
	Schedulers SchedulerMap
	// ControllerAgents contain all controller agent roles.
	ControllerAgents ControllerAgentMap
}

// NewTaskCache initializes task cache with given tasks.
func NewTaskCache(tasks []*Task) (*TaskCache, error) {
	c := &TaskCache{
		Tasks:            tasks,
		TaskUpgrades:     make(map[string]*TaskUpgrade),
		MasterCellTasks:  make(map[ypath.Path][]*Task),
		Schedulers:       make(SchedulerMap),
		ControllerAgents: make(ControllerAgentMap),
	}

	c.findTaskUpgrades()
	if err := c.groupMasterTasks(); err != nil {
		return nil, err
	}
	c.groupByRole()

	return c, nil
}

// findTaskUpgrades fills task upgrade map.
func (c *TaskCache) findTaskUpgrades() {
	upgrades := make(map[string]*TaskUpgrade)
	for _, t := range c.Tasks {
		for _, h := range t.Hosts {
			if _, ok := upgrades[h]; !ok {
				upgrades[h] = &TaskUpgrade{Old: t, New: t}
			} else if upgrades[h].New.ID < t.ID {
				upgrades[h].New = t
			} else if t.ID < upgrades[h].Old.ID {
				upgrades[h].Old = t
			}
		}
	}

	// Remove hosts without upgrade.
	for h, u := range upgrades {
		if u.Old.ID == u.New.ID {
			delete(upgrades, h)
		}
	}

	c.TaskUpgrades = upgrades
}

// groupMasterTasks groups tasks by cypress cell path.
func (c *TaskCache) groupMasterTasks() error {
	for _, t := range c.Tasks {
		for _, h := range t.HostStates {
			for _, r := range h.Roles {
				switch r.Type {
				case ytsys.RolePrimaryMaster,
					ytsys.RoleSecondaryMaster,
					ytsys.RoleTimestampProvider:
					m := r.Role.(*Master)
					cellPath, _, err := ypath.Split(m.Path)
					if err != nil {
						return xerrors.Errorf("unable to resolve master cell path: %w", err)
					}
					c.MasterCellTasks[cellPath] = append(c.MasterCellTasks[cellPath], t)
				}
			}
		}
	}

	return nil
}

// groupByRole initializes cache fields that group component state by role.
func (c *TaskCache) groupByRole() {
	for _, t := range c.Tasks {
		for _, h := range t.HostStates {
			for _, r := range h.Roles {
				switch r.Type {
				case ytsys.RoleScheduler:
					s := r.Role.(*Scheduler)
					c.Schedulers[*s.Addr] = s
				case ytsys.RoleControllerAgent:
					ca := r.Role.(*ControllerAgent)
					c.ControllerAgents[*ca.Addr] = ca
				}
			}
		}
	}
}
