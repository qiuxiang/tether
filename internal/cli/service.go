package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"os/user"
	"runtime"
	"strings"

	"github.com/kardianos/service"
	tsvc "github.com/qiuxiang/tether/internal/service"
)

func Service(args []string, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(stderr, "usage: tether service <install|start|stop|uninstall|status> --name <svc> [--args \"...\"] [--env K=V]... [--system|--user]")
		return 2
	}
	action := args[0]

	fs := flag.NewFlagSet("service "+action, flag.ContinueOnError)
	name := fs.String("name", "", "Service name")
	argsStr := fs.String("args", "", "Arguments passed to the binary (install only)")
	var envFlags envList
	fs.Var(&envFlags, "env", "KEY=VALUE, repeatable")
	systemFlag := fs.Bool("system", false, "Install/operate as a system service")
	userFlag := fs.Bool("user", false, "Install/operate as a user service")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *name == "" {
		fmt.Fprintln(stderr, "service: --name is required")
		return 2
	}
	if *systemFlag && *userFlag {
		fmt.Fprintln(stderr, "service: --system and --user are mutually exclusive")
		return 2
	}
	requested := tsvc.ScopeAuto
	if *systemFlag {
		requested = tsvc.ScopeSystem
	}
	if *userFlag {
		requested = tsvc.ScopeUser
	}
	scope, err := tsvc.Resolve(requested, runtime.GOOS)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	env, err := envFlags.parse()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	switch action {
	case "install":
		cfg, err := tsvc.BuildConfig(*name, *argsStr, env, scope, runtime.GOOS)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		s, err := service.New(noopProgram(), cfg)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := s.Install(); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if runtime.GOOS == "linux" && scope == tsvc.ScopeUser {
			enableLinger(stderr)
		}
		fmt.Fprintf(stderr, "service %s installed\n", *name)
		return 0

	case "start", "stop", "uninstall":
		s, err := service.New(noopProgram(), tsvc.ControlConfig(*name, scope))
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := service.Control(s, action); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintf(stderr, "service %s: %s ok\n", *name, action)
		return 0

	case "status":
		s, err := service.New(noopProgram(), tsvc.ControlConfig(*name, scope))
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		st, err := s.Status()
		if errors.Is(err, service.ErrNotInstalled) {
			fmt.Fprintln(stderr, "not-installed")
			return 0
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		switch st {
		case service.StatusRunning:
			fmt.Fprintln(stderr, "running")
		case service.StatusStopped:
			fmt.Fprintln(stderr, "stopped")
		default:
			fmt.Fprintln(stderr, "unknown")
		}
		return 0

	default:
		fmt.Fprintf(stderr, "unknown service action: %s\n", action)
		return 2
	}
}

func noopProgram() *tsvc.Program {
	return tsvc.New(func(context.Context) {}, func() {})
}

func enableLinger(stderr io.Writer) {
	u, err := user.Current()
	if err != nil {
		fmt.Fprintf(stderr, "warning: loginctl enable-linger skipped: %v\n", err)
		return
	}
	if err := exec.Command("loginctl", "enable-linger", u.Username).Run(); err != nil {
		fmt.Fprintf(stderr, "warning: loginctl enable-linger failed: %v\n", err)
	}
}

type envList []string

func (e *envList) String() string { return strings.Join(*e, ",") }
func (e *envList) Set(v string) error {
	*e = append(*e, v)
	return nil
}

func (e envList) parse() ([]tsvc.KV, error) {
	var out []tsvc.KV
	for _, v := range e {
		k, val, ok := strings.Cut(v, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("service: invalid --env value %q, want KEY=VALUE", v)
		}
		out = append(out, tsvc.KV{Key: k, Value: val})
	}
	return out, nil
}
