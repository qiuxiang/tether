//go:build darwin || freebsd || openbsd || netbsd || dragonfly || solaris

package node

import (
	"context"
	"os/exec"
	"syscall"
)

// newShellCmd runs the command through `sh -c` in a new process group so
// killGroup can signal the whole group.
func newShellCmd(ctx context.Context, command string) *exec.Cmd {
	c := exec.CommandContext(ctx, "sh", "-c", command)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return c
}

func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
}
