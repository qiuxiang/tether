//go:build darwin || freebsd || openbsd || netbsd || dragonfly || solaris

package node

import "syscall"

// childAttrPTY returns SysProcAttr for PTY children. go-pty sets
// Setsid+Setctty internally; we pass nil to avoid interfering with
// session/tty setup.
func childAttrPTY() *syscall.SysProcAttr {
	return nil
}

func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
}
