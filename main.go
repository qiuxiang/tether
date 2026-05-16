package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args, os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprintln(stderr, "usage: tether <serve|join> [flags]")
		return 2
	}
	switch args[1] {
	case "serve":
		fmt.Fprintln(stderr, "serve: not implemented")
		return 1
	case "join":
		fmt.Fprintln(stderr, "join: not implemented")
		return 1
	default:
		fmt.Fprintf(stderr, "unknown subcommand: %s\n", args[1])
		return 2
	}
}
