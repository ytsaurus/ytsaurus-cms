package cms

import (
	"context"
	"sync"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
)

// HostAnnotations stores additional host info retrieved from wall-e.
type HostAnnotations struct {
	storage     Storage
	walleClient WalleClient

	l log.Structured

	mu       sync.RWMutex
	hostInfo map[string]*walle.HostInfo
}

// NewHostAnnotations simply creates HostAnnotations instance.
func NewHostAnnotations(s Storage, c WalleClient, l log.Structured) *HostAnnotations {
	return &HostAnnotations{
		storage:     s,
		walleClient: c,
		l:           l,
	}
}

// Reload queries Wall-e for host info for all active tasks and updates internal state.
func (a *HostAnnotations) Reload(ctx context.Context) error {
	tasks, err := a.storage.GetAll(ctx)
	if err != nil {
		return err
	}

	hosts := make([]string, 0, len(tasks))
	for _, t := range tasks {
		hosts = append(hosts, t.Hosts...)
	}

	info, err := a.walleClient.GetHostInfo(ctx, hosts...)
	if err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.hostInfo = info
	return nil
}

// GetHostInfo returns stored information for given host.
func (a *HostAnnotations) GetHostInfo(host string) (*walle.HostInfo, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	info, ok := a.hostInfo[host]
	return info, ok
}
