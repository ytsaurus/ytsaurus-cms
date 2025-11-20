package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.ytsaurus.tech/library/go/core/log"
	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/admin/cms/internal/app"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"gopkg.in/yaml.v2"
)

func main() {
	Adjust()

	logToStderr := flag.Bool("log-to-stderr", false, "write logs to stderr")
	logDir := flag.String("log-dir", "/logs", "log output directory")
	uaLogsURL := flag.String("ua-url", "", "url (host:port) for unified agent client")
	configPath := flag.String("config", "", "path to the yaml config")
	flag.Parse()

	logger, stop := newLogger(*logToStderr, *logDir, WithUALogsURL(*uaLogsURL))
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
		logger.Debug("app stopped", log.Error(err))
		return err
	}))

	if err := g.Wait(); err != nil {
		logger.Debug("main stopped", log.Error(err))
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

// waitSignal blocks until SIGINT or SIGTERM is received.
//
// Can be canceled via ctx.
func waitSignal(ctx context.Context, l *logzap.Logger) error {
	exitSignal := make(chan os.Signal, 1)
	signal.Notify(exitSignal, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-exitSignal:
		l.Warn("received signal", log.String("signal", s.String()))
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
