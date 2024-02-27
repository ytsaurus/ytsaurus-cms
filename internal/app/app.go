package app

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/httputil/middleware/httpmetrics"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
	"golang.org/x/sync/errgroup"
)

const (
	httpGracefulStopTimeout = 30 * time.Second
)

// App is a god object managing service lifetime.
type App struct {
	l log.Structured

	conf *Config

	registry *MetricsRegistry
}

// NewApp creates new app.
func NewApp(conf *Config, l log.Structured) *App {
	return &App{
		conf:     conf,
		l:        l,
		registry: NewMetricsRegistry(),
	}
}

// Run performs initialization and starts all components.
//
// Can be canceled via context.
func (a *App) Run(ctx context.Context) error {
	a.l.Info("starting app")

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		a.runHTTPServer(gctx, a.newDebugHTTPServer())
		return gctx.Err()
	})

	g.Go(func() error {
		return a.initTaskDiscovery(gctx)
	})

	r := chi.NewMux()
	r.Use(httpmetrics.New(a.registry.WithPrefix("http")))
	r.Use(timeout(a.conf.HTTPHandlerTimeout))
	r.Use(requestLog(a.l))
	r.Use(CORS())

	for _, c := range a.conf.Clusters {
		l := log.With(a.l.Logger(), log.String("cluster", c.Proxy)).Structured()
		yc, err := ythttp.NewClient(&yt.Config{
			Proxy:  c.Proxy,
			Token:  a.conf.YTToken,
			Logger: l,
		})
		if a.conf.UseRPCProxy {
			yc, err = ytrpc.NewClient(&yt.Config{
				Proxy:  c.Proxy,
				Token:  a.conf.YTToken,
				Logger: l,
			})
		}
		if err != nil {
			return err
		}

		s := NewSystem(c, yc, l)
		systemMetrics := a.registry.WithTags(map[string]string{"yt-cluster": c.Proxy})
		s.RegisterMetrics(systemMetrics)
		s.RegisterAPI(r)

		g.Go(func() error {
			return s.Run(gctx)
		})
	}

	server := &http.Server{
		Addr:    a.conf.HTTPAddr,
		Handler: r,
	}

	g.Go(func() error {
		a.runHTTPServer(gctx, server)
		return gctx.Err()
	})

	return g.Wait()
}

func (a *App) newDebugHTTPServer() *http.Server {
	debugRouter := chi.NewMux()
	debugRouter.Handle("/debug/*", http.DefaultServeMux)
	a.registry.HandleMetrics(debugRouter)
	return &http.Server{
		Addr:    a.conf.DebugHTTPAddr,
		Handler: debugRouter,
	}
}

// runHTTPServer runs http server and gracefully stop it when the context is closed.
func (a *App) runHTTPServer(ctx context.Context, s *http.Server) {
	a.l.Info("starting http server", log.String("addr", s.Addr))

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := s.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	<-ctx.Done()

	a.l.Info("waiting for http server to stop",
		log.String("addr", a.conf.HTTPAddr), log.Duration("timeout", httpGracefulStopTimeout))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), httpGracefulStopTimeout)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		if err == context.DeadlineExceeded {
			a.l.Warn("http server shutdown deadline exceeded",
				log.String("addr", a.conf.HTTPAddr))
		} else {
			panic(err)
		}
	}

	wg.Wait()

	a.l.Info("http server stopped", log.String("addr", s.Addr))
}
