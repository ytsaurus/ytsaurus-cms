package cms

import (
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/ytsys"
)

type YTProxyMap map[ytsys.Addr]ytsys.YTProxy

//go:generate mockgen -destination=proxy_cache_mock.go -package cms . YTProxyCache

type YTProxyCache interface {
	GetProxies() YTProxyMap
}

// httpProxyCache implements YTProxyCache that returns http proxies acquired from cluster.
type httpProxyCache struct {
	c models.Cluster
}

// GetProxies asks cluster for http proxies.
func (c *httpProxyCache) GetProxies() YTProxyMap {
	m := make(YTProxyMap)

	proxies, err := c.c.GetHTTPProxies()
	if err != nil {
		return nil
	}

	for addr, p := range proxies {
		m[addr] = p
	}

	return m
}

// rpcProxyCache implements YTProxyCache that returns rpc proxies acquired from cluster.
type rpcProxyCache struct {
	c models.Cluster
}

// GetProxies asks cluster for rpc proxies.
func (c *rpcProxyCache) GetProxies() YTProxyMap {
	m := make(YTProxyMap)

	proxies, err := c.c.GetRPCProxies()
	if err != nil {
		return nil
	}

	for addr, p := range proxies {
		m[addr] = p
	}

	return m
}

type addrSet map[ytsys.Addr]struct{}

type banStats struct {
	banned addrSet
	all    addrSet
}

func newBanStats() *banStats {
	return &banStats{
		banned: make(addrSet),
		all:    make(addrSet),
	}
}

func (s *banStats) addBanned(addr ytsys.Addr) {
	s.banned[addr] = struct{}{}
	s.all[addr] = struct{}{}
}

func (s *banStats) add(addr ytsys.Addr) {
	s.all[addr] = struct{}{}
}

// ProxyRoleLimits stores per-role proxy ban limits.
type ProxyRoleLimits struct {
	maxProxiesPerRole float64

	cache YTProxyCache
	stats map[ytsys.YTProxyRole]*banStats
}

// NewProxyRoleLimits creates and initializes new proxy role limits.
func NewProxyRoleLimits(maxProxiesPerRole float64, c YTProxyCache) *ProxyRoleLimits {
	l := &ProxyRoleLimits{
		maxProxiesPerRole: maxProxiesPerRole,
		cache:             c,
		stats:             make(map[ytsys.YTProxyRole]*banStats),
	}
	l.Reload()
	return l
}

// Ban tells whether banning breaks ban limits.
//
// Returns true if proxy is already banned.
//
// Empty role is treated as a separate role.
func (l *ProxyRoleLimits) Ban(p ytsys.YTProxy) bool {
	role := p.ProxyRole()
	addr := *p.GetAddr()

	if _, ok := l.stats[role]; !ok {
		l.stats[role] = newBanStats()
	}

	s := l.stats[role]
	if _, ok := s.banned[addr]; ok {
		return true
	}

	if len(s.all) == 0 {
		// It seems that cache is stale.
		return false
	}

	bannedFrac := float64(len(s.banned)+1) / float64(len(s.all))
	allow := bannedFrac <= l.maxProxiesPerRole || len(s.all) > 1 && len(s.banned) == 0
	if allow {
		s.banned[addr] = struct{}{}
	}

	return allow
}

func (l *ProxyRoleLimits) Reload() {
	stats := make(map[ytsys.YTProxyRole]*banStats)

	proxies := l.cache.GetProxies()
	for addr, p := range proxies {
		r := p.ProxyRole()
		if _, ok := stats[r]; !ok {
			stats[r] = newBanStats()
		}
		if p.IsBanned() {
			stats[r].addBanned(addr)
		} else {
			stats[r].add(addr)
		}
	}

	l.stats = stats
}
