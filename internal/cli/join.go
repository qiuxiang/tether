package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/qiuxiang/tether/internal/config"
	"github.com/qiuxiang/tether/internal/node"
)

func Join(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	configPath := fs.String("config", expandHome("~/.config/tether/config.yaml"), "Path to config")
	once := fs.Bool("once", false, "Run a single connection attempt and exit")
	tail := fs.Bool("tail", false, "Print inbound/outbound frames to stderr")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	_ = once
	_ = tail // wired in by the engineer if desired
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if cfg.HubURL == "" {
		fmt.Fprintln(stderr, "config: hub_url is required")
		return 1
	}
	host := cfg.HostnameOverride
	if host == "" {
		host, _ = os.Hostname()
	}
	ph := node.NewHandler()
	ph.ForwardHandler().InitRules(cfg.Forwards)

	cli := node.New(node.Config{
		HubURL:   cfg.HubURL,
		Token:    cfg.Token,
		Hostname: host,
		OnConnected: func(send node.Sender) {
			ph.ForwardHandler().Start(context.Background(), send)
		},
	})
	cli.SetHandler(ph)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cli.Run(ctx)
	ph.Shutdown()
	return 0
}

func expandHome(p string) string {
	if len(p) > 1 && p[0] == '~' {
		if home, err := os.UserHomeDir(); err == nil {
			return home + p[1:]
		}
	}
	return p
}
