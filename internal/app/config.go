package app

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/cms"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/xerrors"
)

const (
	defaultHTTPHandlerTimeout = 30 * time.Second
	defaultYPPollPeriod       = 10 * time.Second

	defaultCypressRoot = ypath.Path("//sys/cms")
)

type auth func(r chi.Router, yc yt.Client, l log.Structured) chi.Router

// Config is an app config.
type Config struct {
	YTToken     string `yaml:"-"`
	YPToken     string `yaml:"-"`
	UseRPCProxy bool   `yaml:"use_rpc_proxy"`

	HTTPAddr           string        `yaml:"http_addr"`
	DebugHTTPAddr      string        `yaml:"debug_http_addr"`
	HTTPHandlerTimeout time.Duration `yaml:"http_handler_timeout"`

	selfURL string `yaml:"-"`

	CypressRootStr string     `yaml:"cypress_root"`
	CypressRoot    ypath.Path `yaml:"-"`

	StartrekConfig *cms.StartrekConfig `yaml:"startrek"`

	Clusters        []*SystemConfig          `yaml:"clusters"`
	clustersByProxy map[string]*SystemConfig `yaml:"-"`

	AuthConfig AuthConfig `yaml:"auth_config"`

	ColocationConfigs []*cms.ColocationConfig `yaml:"colocation"`
	// colocationConfigs contains colocation configs grouped by Wall-e project.
	colocationConfigs cms.ColocationConfigs `yaml:"-"`
}

func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.HTTPAddr == "" {
		return xerrors.New("http addr can not be empty")
	}
	if c.DebugHTTPAddr == "" {
		return xerrors.New("debug http addr can not be empty")
	}

	if c.HTTPHandlerTimeout == 0 {
		c.HTTPHandlerTimeout = defaultHTTPHandlerTimeout
	}

	selfURL, err := os.Hostname()
	if err != nil {
		return xerrors.Errorf("unable to determine hostname: %w", err)
	}
	c.selfURL = selfURL

	if c.CypressRootStr == "" {
		c.CypressRootStr = defaultCypressRoot.String()
	}
	p, err := ypath.Parse(c.CypressRootStr)
	if err != nil {
		return xerrors.Errorf("invalid cypress root: %w", err)
	}
	c.CypressRoot = p.Path

	if c.StartrekConfig == nil {
		return xerrors.New("startrek config can not be empty")
	}

	if len(c.Clusters) == 0 {
		return xerrors.New("clusters can not be empty")
	}

	c.colocationConfigs = make(cms.ColocationConfigs)
	for _, conf := range c.ColocationConfigs {
		c.colocationConfigs[conf.WalleProject] = conf
	}

	c.AuthConfig.auth = func(next http.Handler) http.Handler { return next }

	byProxy := make(map[string]*SystemConfig)
	for _, conf := range c.Clusters {
		conf.selfURL = selfURL
		conf.CypressRoot = c.CypressRoot
		conf.StartrekConfig = c.StartrekConfig
		conf.ColocationConfigs = c.colocationConfigs
		conf.AuthConfig = c.AuthConfig

		if _, ok := byProxy[conf.Proxy]; ok {
			return fmt.Errorf("duplicate cluster %s", conf.Proxy)
		}
		byProxy[conf.Proxy] = conf

	}
	c.clustersByProxy = byProxy

	return nil
}
