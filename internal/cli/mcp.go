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
)

func MCP(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	configPath := fs.String("config", expandHome("~/.config/tether/client.yaml"), "Path to client config")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := client.Load(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	c := client.NewConn(*cfg)

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
