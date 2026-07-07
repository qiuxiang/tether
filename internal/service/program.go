package service

import (
	"context"
	"os"

	"github.com/kardianos/service"
)

// Program wraps a role's run loop (serve/join) as a kardianos service.Interface.
type Program struct {
	run      func(context.Context)
	shutdown func()
	cancel   context.CancelFunc
}

// New builds a Program from a run-func, launched in a goroutine on Start, and
// a shutdown-func, invoked on Stop after the run-func's context is canceled.
func New(run func(context.Context), shutdown func()) *Program {
	return &Program{run: run, shutdown: shutdown}
}

// Start must not block; kardianos and the SCM expect it to return quickly.
func (p *Program) Start(s service.Service) error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go p.run(ctx)
	return nil
}

func (p *Program) Stop(s service.Service) error {
	p.cancel()
	p.shutdown()
	return nil
}

// Run is the entry point used by serve/join: it resolves the service name
// this process was started under (set by `service install`, empty when run
// interactively) and hands control to kardianos.
func Run(run func(context.Context), shutdown func()) error {
	name := os.Getenv("TETHER_SERVICE_NAME")
	if name == "" {
		name = "tether"
	}
	s, err := service.New(New(run, shutdown), &service.Config{Name: name})
	if err != nil {
		return err
	}
	return s.Run()
}
