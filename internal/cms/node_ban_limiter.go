package cms

import (
	"time"
)

// NodeBanLimiter stores node ban limiter.
//
// Unbanned() method must be called when a node was unbanned because of LVC,
// Allow() method can be called to check if another node can be banned.
type NodeBanLimiter struct {
	LastBanTime time.Time
	minTimeout  time.Duration
	maxTimeout  time.Duration
	unbanTimes  []time.Time
}

// NewNodeBanLimiter creates and initializes new node ban limiter.
func NewNodeBanLimiter(minTimeout, maxTimeout time.Duration) *NodeBanLimiter {
	l := &NodeBanLimiter{
		minTimeout: minTimeout,
		maxTimeout: maxTimeout,
	}
	return l
}

// Allow returns true if another node can be banned.
//
// It checks that "timeout" has expired since LastBanTime,
// "timeout" is calculated as follows:
// if Unbanned() method wasn't called during 3*maxTimeout duration,
// "timeout" is minTimeout, if Unbanned() method was called once
// during 3*maxTimeout duration, "timeout" is (minTimeout + maxTimeout) / 2,
// otherwise "timeout" is maxTimeout.
func (l *NodeBanLimiter) Allow() bool {
	return l.allow(l.LastBanTime, time.Now().UTC())
}

func (l *NodeBanLimiter) allow(lastBanTime, now time.Time) bool {
	l.update(now)
	var timeout = l.minTimeout
	switch len(l.unbanTimes) {
	case 0:
		timeout = l.minTimeout
	case 1:
		timeout = (l.minTimeout + l.maxTimeout) / 2
	default:
		timeout = l.maxTimeout
	}
	return now.Sub(lastBanTime) >= timeout
}

// Unbanned must be called when a node was unbanned because of LVC.
func (l *NodeBanLimiter) Unbanned() {
	l.unbanned(time.Now().UTC())
}

func (l *NodeBanLimiter) unbanned(now time.Time) {
	l.update(now)
	l.unbanTimes = append(l.unbanTimes, now)
}

func (l *NodeBanLimiter) update(now time.Time) {
	var newQueue []time.Time
	for _, t := range l.unbanTimes {
		if now.Sub(t) >= 3*l.maxTimeout {
			continue
		}
		newQueue = append(newQueue, t)
	}
	l.unbanTimes = newQueue
}
