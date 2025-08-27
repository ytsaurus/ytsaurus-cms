package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"go.ytsaurus.tech/library/go/core/log"
	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/admin/cms/internal/app"
	"go.ytsaurus.tech/yt/go/ytlog"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"gopkg.in/yaml.v2"
)

func main() {
	Adjust()

	logToStderr := flag.Bool("log-to-stderr", false, "write logs to stderr")
	logDir := flag.String("log-dir", "/logs", "log output directory")
	configPath := flag.String("config", "", "path to the yaml config")
	flag.Parse()

	logger, stop := newLogger(*logToStderr, *logDir)
	defer stop()

	conf, err := readConfig(*configPath)
	if err != nil {
		logger.Fatalf("unable to read config from %s: %s", *configPath, err)
	}
	conf.YTToken = os.Getenv("YT_TOKEN")
	conf.TaskDiscoveryToken = os.Getenv("TASK_DISCOVERY_TOKEN")
	conf.StartrekConfig.OAuthToken = os.Getenv("STARTREK_OAUTH_TOKEN")

	if err := conf.Init(logger); err != nil {
		logger.Fatal("failed to init config fields", log.Error(err))
	}

	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return waitSignal(ctx, logger)
	})

	g.Go(ensureError(func() error {
		err := app.NewApp(conf, logger).Run(ctx)
		logger.Info("app stopped", log.Error(err))
		return err
	}))

	if err := g.Wait(); err != nil {
		logger.Info("main stopped", log.Error(err))
	}
}

// readConfig loads yaml config from file.
func readConfig(path string) (*app.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var conf app.Config
	err = yaml.Unmarshal(data, &conf)
	return &conf, err
}

// newLogger initializes debugging logger
// that either writes to stderr or to cms.log in given directory.
func newLogger(logToStderr bool, logDir string) (*logzap.Logger, func()) {
	if logToStderr {
		return ytlog.Must(), func() {}
	}

	l, stop, err := ytlog.NewSelfrotate(filepath.Join(logDir, "cms.log"))
	if err != nil {
		panic(err)
	}

	return l, stop
}

// waitSignal blocks until SIGINT or SIGTERM is received.
//
// Can be canceled via ctx.
func waitSignal(ctx context.Context, l *logzap.Logger) error {
	exitSignal := make(chan os.Signal, 1)
	signal.Notify(exitSignal, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-exitSignal:
		l.Info("received signal", log.String("signal", s.String()))
		return xerrors.New("interrupted")
	case <-ctx.Done():
		return nil
	}
}

// ensureError wraps f into function that always returns an error.
//
// Which is either f's error if non-nil or synthetic "unexpected nil" otherwise.
func ensureError(f func() error) func() error {
	return func() error {
		if err := f(); err != nil {
			return err
		}
		return xerrors.Errorf("unexpected nil")
	}
}
