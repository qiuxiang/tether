package main

import (
	"fmt"
	"io"
	"os"

	"github.com/qiuxiang/tether/internal/cli"
)

func main() { os.Exit(run(os.Args, os.Stderr)) }

func run(args []string, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprintln(stderr, "usage: tether <serve|join> [flags]")
		return 2
	}
	switch args[1] {
	case "serve":
		return cli.Serve(args[2:], stderr)
	case "join":
		return cli.Join(args[2:], stderr)
	default:
		fmt.Fprintf(stderr, "unknown subcommand: %s\n", args[1])
		return 2
	}
}
