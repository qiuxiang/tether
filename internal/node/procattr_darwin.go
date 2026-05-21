//go:build darwin || freebsd || openbsd || netbsd || dragonfly || solaris

package node

import "syscall"

func killGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
}

// childAttrExec returns SysProcAttr for plain exec children: a new process
// group so killGroup can signal the whole group.
func childAttrExec() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
