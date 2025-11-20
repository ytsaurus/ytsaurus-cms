package main

import (
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/ytlog"
)

type (
	Option interface {
		isOption()
	}

	uaLogsURLOption string
)

func (uaLogsURLOption) isOption() {}

func WithUALogsURL(url string) Option {
	return uaLogsURLOption(url)
}

// newLogger initializes debugging logger
// that either writes to stderr or to cms.log in given directory.
func newLogger(logToStderr bool, logDir string, options ...Option) (*logzap.Logger, func()) {
	var (
		core zapcore.Core
		stop func()
		err  error
	)

	if logToStderr {
		core = zapcore.NewCore(
			zapcore.NewJSONEncoder(ytlog.NewConfig().EncoderConfig),
			zapcore.Lock(os.Stderr),
			zapcore.DebugLevel,
		)
	} else {
		core, stop, err = ytlog.NewSelfrotateCore(filepath.Join(logDir, "cms.log"), ytlog.WithMinFreeSpace(0.1))
		if err != nil {
			panic(err)
		}
	}

	return logzap.NewWithCore(core, zap.AddCallerSkip(1), zap.AddCaller()), stop
}
