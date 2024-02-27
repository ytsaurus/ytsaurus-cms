package app

import (
	"context"
	"net"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

type AuthConfig struct {
	DisableAuth bool `yaml:"disable_auth"`

	cliAuth auth `yaml:"-"`
	// auth is a auth middleware.
	auth func(next http.Handler) http.Handler `yaml:"-"`
}

// Init initializes some derivative config fields e.g. (blackbox).
func (c *Config) Init(l log.Structured) error {
	if c.AuthConfig.DisableAuth {
		l.Info("auth is disabled")
		return nil
	}

	c.AuthConfig.cliAuth = initAuth()

	return nil
}

type requester struct{}

var requesterKey requester

func WithRequester(ctx context.Context, requester string) context.Context {
	return context.WithValue(ctx, &requesterKey, requester)
}

func ContextRequester(ctx context.Context) (requester string, ok bool) {
	if v := ctx.Value(&requesterKey); v != nil {
		requester = v.(string)
		ok = requester != ""
	}
	return
}

func initAuth() auth {
	return func(router chi.Router, yc yt.Client, l log.Structured) chi.Router {
		return router.With(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := WithRequester(r.Context(), r.Header.Get("X-YT-TestUser"))
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			})
		})
	}
}

func CORS() func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			// CORS middleware

			next.ServeHTTP(w, r)
		})
	}
}

var (
	xForwardedForY = "X-Forwarded-For-Y"
	xForwardedFor  = "X-Forwarded-For"
)

// Origin extracts original IP address of a client.
//
// When connecting to a web server through an HTTP proxy or a load balancer
// the address from X-Forwarded-For-Y or standard X-Forwarded-For header is used.
func Origin(r *http.Request) string {
	if h := r.Header.Get(xForwardedForY); h != "" {
		return h
	} else if h := r.Header.Get(xForwardedFor); h != "" {
		return h
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}
