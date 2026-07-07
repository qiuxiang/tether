package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/qiuxiang/tether/internal/config"
	"github.com/qiuxiang/tether/internal/hub"
	svc "github.com/qiuxiang/tether/internal/service"
)

func Serve(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", expandHome("~/.config/tether/config.yaml"), "Path to config")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	s := hub.NewServer(hub.Options{Token: cfg.Token})
	srv := &http.Server{Addr: cfg.Listen, Handler: s.Handler()}

	run := func(ctx context.Context) {
		fmt.Fprintf(stderr, "tether serve listening on %s\n", cfg.Listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintln(stderr, err)
		}
	}
	shutdown := func() { srv.Shutdown(context.Background()) }

	if err := svc.Run(run, shutdown); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
