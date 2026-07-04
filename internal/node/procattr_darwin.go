//go:build darwin || freebsd || openbsd || netbsd || dragonfly || solaris

package node

import (
	"context"
	"os/exec"
	"syscall"
)

// newProcessCmd spawns args directly (no shell) in a new process group so
// killGroup can signal the whole group.
func newProcessCmd(ctx context.Context, args []string) *exec.Cmd {
	c := exec.CommandContext(ctx, args[0], args[1:]...)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return c
}

func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
}
