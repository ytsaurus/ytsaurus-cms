package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/ytlog"
)

func main() {
	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		_, _ = fmt.Fprint(flag.CommandLine.Output(), "migrate tries to alter cms tasks table schema\n")
		_, _ = fmt.Fprint(flag.CommandLine.Output(), "")
		flag.PrintDefaults()
	}

	proxy := flag.String("proxy", "socrates", "yt proxy")
	cypressPath := flag.String("cypress-path", "//sys/cms/tasks", "path to the table in cypress")
	dryRun := flag.Bool("dry-run", false, "print schema diff (if any) to stdout and exit")
	flag.Parse()

	l := ytlog.Must()

	l.Info("migrating table", log.String("proxy", *proxy), log.String("path", *cypressPath))
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:             *proxy,
		ReadTokenFromFile: true,
	})
	if err != nil {
		l.Fatalf("error creating yt client: %s", err)
	}

	tables := map[ypath.Path]migrate.Table{
		ypath.Path(*cypressPath): {Schema: schema.MustInfer(&models.Task{})},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	if err := migrate.EnsureTables(ctx, yc, tables, func(p ypath.Path, actual, expected schema.Schema) error {
		diff := cmp.Diff(expected, actual)
		_, _ = fmt.Fprintf(os.Stdout, "diff:\n%s\n", diff)
		if *dryRun {
			return migrate.ErrConflict
		}
		return migrate.OnConflictTryAlter(ctx, yc)(p, actual, expected)
	}); err != nil {
		l.Fatalf("error migrating table: %s", err)
	}

	l.Info("migrated table", log.String("proxy", *proxy), log.String("path", *cypressPath))
}
