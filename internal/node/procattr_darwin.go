//go:build darwin || freebsd || openbsd || netbsd || dragonfly || solaris

package node

import "syscall"

// childAttr returns SysProcAttr for non-TTY (pipe) children.
func childAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// childAttrPTY returns SysProcAttr for TTY children. go-pty sets
// Setsid+Setctty internally; we pass nil to avoid interfering with
// session/tty setup.
func childAttrPTY() *syscall.SysProcAttr {
	return nil
}

func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
}
