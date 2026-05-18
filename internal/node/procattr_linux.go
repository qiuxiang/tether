//go:build linux

package node

import "syscall"

// childAttrPTY returns SysProcAttr for PTY children. go-pty sets
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
