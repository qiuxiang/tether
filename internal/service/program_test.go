package service

import (
	"context"
	"testing"
	"time"
)

func TestProgramStartStop(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	stopped := make(chan struct{})

	run := func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(canceled)
	}
	shutdown := func() { close(stopped) }

	p := New(run, shutdown)
	if err := p.Start(nil); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("run-func was not launched")
	}

	if err := p.Stop(nil); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the run-func's context")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not invoke shutdown")
	}
}
