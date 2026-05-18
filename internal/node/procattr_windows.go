//go:build windows

package node

import "syscall"

func childAttrPTY() *syscall.SysProcAttr {
	return nil
}

func killGroup(_ int) {}
