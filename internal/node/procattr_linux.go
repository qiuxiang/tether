//go:build linux

package node

import (
	"context"
	"os/exec"
	"syscall"
)

// newProcessCmd spawns args directly (no shell) in a new process group so
// killGroup can signal the whole group; Pdeathsig kills the child if the agent
// dies.
func newProcessCmd(ctx context.Context, args []string) *exec.Cmd {
	c := exec.CommandContext(ctx, args[0], args[1:]...)
	c.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
	}
	return c
}

func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
}
