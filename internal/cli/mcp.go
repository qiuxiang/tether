package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/qiuxiang/tether/internal/client"
	"github.com/qiuxiang/tether/internal/config"
)

func MCP(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	configPath := fs.String("config", expandHome("~/.config/tether/config.yaml"), "Path to config")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if cfg.HubURL == "" {
		fmt.Fprintln(stderr, "config: hub_url is required")
		return 1
	}
	c := client.NewConn(client.Config{HubURL: cfg.HubURL, Token: cfg.Token})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go c.Run(ctx)
	if err := c.WaitReady(ctx); err != nil {
		fmt.Fprintln(stderr, "hub not reachable:", err)
		return 1
	}

	s := client.NewMCPServer(c)
	if err := s.Serve(ctx, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
