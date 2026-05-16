//go:build linux

package node

import "syscall"

// childAttr returns SysProcAttr for non-TTY (pipe) children.
// Setpgid puts the child in its own process group (pid == pgid),
// enabling killGroup(-pid, sig). Pdeathsig kills the child if the
// parent dies before graceful Shutdown.
func childAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
		Setpgid:   true,
	}
}

// childAttrPTY returns SysProcAttr for TTY children. go-pty sets
// Setsid+Setctty internally; we only add Pdeathsig so we don't
// interfere with session/tty setup. PTY children are already
// isolated: closing the PTY sends SIGHUP anyway.
func childAttrPTY() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
}

func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
}
