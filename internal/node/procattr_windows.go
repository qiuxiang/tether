//go:build windows

package node

import "syscall"

func killGroup(_ int) {}

// childAttrExec is a no-op on Windows; killGroup is also a no-op there and
// exec.CommandContext's default kill applies.
func childAttrExec() *syscall.SysProcAttr { return nil }
