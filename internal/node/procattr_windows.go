//go:build windows

package node

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// newShellCmd runs the command through `cmd /c`. We set CmdLine explicitly so
// the command reaches cmd verbatim: Go's default argument escaping mangles
// metacharacters (| & < > ()) that live inside a quoted sub-argument such as a
// PowerShell pipeline. Wrapping the whole command in one pair of quotes lets
// cmd's /c quote-stripping rule hand the inner string to the program intact.
func newShellCmd(ctx context.Context, command string) *exec.Cmd {
	shell := os.Getenv("ComSpec")
	if shell == "" {
		shell = "cmd"
	}
	c := exec.CommandContext(ctx, shell)
	c.SysProcAttr = &syscall.SysProcAttr{
		CmdLine: shell + ` /c "` + command + `"`,
	}
	return c
}

// killGroup terminates the whole process tree rooted at pid. Windows has no
// killable process group like Unix, and a no-op kill lets timed-out commands
// (and their children) linger and pile up, which wedges the node. taskkill /T
// walks and force-kills the entire tree.
func killGroup(pid int) {
	_ = exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).Run()
}
