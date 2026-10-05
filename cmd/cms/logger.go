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
// that writes to stderr and/or to cms.log in given directory.
func newLogger(logToStderr, logToFile bool, logDir string, options ...Option) (*logzap.Logger, func()) {
	var (
		cores []zapcore.Core
		stop  = func() {}
	)

	if logToStderr {
		cores = append(cores, zapcore.NewCore(
			zapcore.NewJSONEncoder(ytlog.NewConfig().EncoderConfig),
			zapcore.Lock(os.Stderr),
			zapcore.DebugLevel,
		))
	}
	if logToFile {
		core, closeLogger, err := ytlog.NewSelfrotateCore(filepath.Join(logDir, "cms.log"), ytlog.WithMinFreeSpace(0.1))
		if err != nil {
			panic(err)
		}
		cores = append(cores, core)
		stop = closeLogger
	}

	return logzap.NewWithCore(zapcore.NewTee(cores...), zap.AddCallerSkip(1), zap.AddCaller()), stop
}
