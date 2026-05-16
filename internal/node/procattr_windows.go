//go:build windows

package node

import "syscall"

func childAttr() *syscall.SysProcAttr {
	return nil
}

func childAttrPTY() *syscall.SysProcAttr {
	return nil
}

func killGroup(_ int) {}
