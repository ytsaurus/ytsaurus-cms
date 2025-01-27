package cms

import (
	"regexp"
	"strings"
	"time"

	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
)

// AllowedTaskHours contains hour range in which task processing is allowed.
//
// From and To are both in range [0, 23].
type AllowedTaskHours struct {
	From int `yaml:"from"`
	To   int `yaml:"to"`
}

// TaskScheduleMatcherConfig contains several task matching fields.
type TaskScheduleMatcherConfig struct {
	Action    walle.HostAction `yaml:"action"`
	CommentRE string           `yaml:"comment_re"`
}

// TaskScheduleItem consists of task matcher and matched task operating hours.
type TaskScheduleItem struct {
	Matcher      *TaskScheduleMatcherConfig `yaml:"matcher"`
	AllowedHours *AllowedTaskHours          `yaml:"allowed_hours"`
}

// Allow checks if given task could be added to processing at given time.
func (i *TaskScheduleItem) Allow(t *models.Task, at time.Time) bool {
	if t.Action != i.Matcher.Action {
		return true
	}
	re := regexp.MustCompile(i.Matcher.CommentRE)
	if !re.MatchString(strings.ToLower(t.Comment)) {
		return true
	}
	if h := at.Hour(); h < i.AllowedHours.From || h >= i.AllowedHours.To {
		return false
	}
	return true
}

// TaskScheduleConfig determines which task not to process at which times.
//
// Config consists of 0 or more schedule items.
// If any of them matches given task, then task processing is postponed.
type TaskScheduleConfig []*TaskScheduleItem

// Allow checks if there are no restricting schedule items to process given task at given time.
func (c TaskScheduleConfig) Allow(t *models.Task, at time.Time) bool {
	for _, item := range c {
		if !item.Allow(t, at) {
			return false
		}
	}
	return true
}
