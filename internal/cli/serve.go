package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/qiuxiang/tether/internal/config"
	"github.com/qiuxiang/tether/internal/hub"
)

func Serve(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "/etc/tether/config.yaml", "Path to config")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := config.LoadHub(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	s := hub.NewServer(hub.Options{Token: cfg.Token})
	srv := &http.Server{Addr: cfg.Listen, Handler: s.Handler()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	fmt.Fprintf(stderr, "tether serve listening on %s\n", cfg.Listen)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
