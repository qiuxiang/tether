//go:build windows

package node

import (
	"context"
	"os/exec"
	"strconv"
	"syscall"
)

// createNoWindow keeps a console child from flashing a window on the desktop
// when the node runs interactively. (The "exec never replies" hang is fixed by
// capturing output to files in runExec, not here.)
const createNoWindow = 0x08000000

// newProcessCmd spawns args directly (no shell). Go's standard argument
// escaping is correct here because each argument reaches the program as its
// own argv entry — no cmd metacharacter or quote-stripping rules apply.
func newProcessCmd(ctx context.Context, args []string) *exec.Cmd {
	c := exec.CommandContext(ctx, args[0], args[1:]...)
	c.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: createNoWindow,
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
