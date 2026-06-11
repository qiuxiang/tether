package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

const (
	// execOutputCap bounds each of stdout/stderr. 4 MiB each stays well under
	// the 16 MiB WSReadLimit once both are packed into one Reply.
	execOutputCap      = 4 << 20
	defaultExecTimeout = 30 * time.Second
)

// execResult is the outcome of running a command via runExec.
type execResult struct {
	Stdout    string
	Stderr    string
	ExitCode  int
	TimedOut  bool
	Truncated bool
}

func mergeEnv(extra map[string]string) []string {
	base := os.Environ()
	for k, v := range extra {
		base = append(base, fmt.Sprintf("%s=%s", k, v))
	}
	return base
}

// readCapped reads at most capN bytes from the start of f, reporting whether
// the file held more (i.e. output was truncated).
func readCapped(f *os.File, capN int) (string, bool) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", false
	}
	buf := make([]byte, capN+1)
	n := 0
	for n < len(buf) {
		r, err := f.Read(buf[n:])
		n += r
		if err != nil {
			break
		}
	}
	if n > capN {
		return string(buf[:capN]), true
	}
	return string(buf[:n]), false
}

// runExec runs m.Cmd through the native shell to completion or until the
// timeout, whichever comes first. On timeout the whole process group is
// killed and TimedOut is set. The returned error is non-nil only when the
// process failed to start at the OS level (e.g. a bad working directory).
//
// stdout/stderr are captured to temp files rather than pipes. On Windows a
// grandchild (or conhost.exe) can inherit the write end of an os/exec pipe and
// hold it open after the command itself exits, so the pipe never reaches EOF
// and c.Wait() blocks forever — and closing the read end from another
// goroutine does not unblock a pending ReadFile on Windows, so WaitDelay can't
// rescue it. A real file handed straight to the child has no such drain step:
// Wait returns as soon as the process exits, and we read the file afterward.
func runExec(ctx context.Context, m *protocol.Exec) (execResult, error) {
	timeout := defaultExecTimeout
	if m.Timeout > 0 {
		timeout = time.Duration(m.Timeout) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	outFile, err := os.CreateTemp("", "tether-exec-out-*")
	if err != nil {
		return execResult{}, err
	}
	defer os.Remove(outFile.Name())
	defer outFile.Close()
	errFile, err := os.CreateTemp("", "tether-exec-err-*")
	if err != nil {
		return execResult{}, err
	}
	defer os.Remove(errFile.Name())
	defer errFile.Close()

	// newShellCmd builds the per-OS shell invocation (sh -c / cmd /c) with the
	// right process-group attributes and command-line quoting.
	c := newShellCmd(ctx, m.Cmd)
	c.Dir = m.Cwd
	c.Env = mergeEnv(m.Env)
	c.Stdout = outFile
	c.Stderr = errFile

	// On timeout, kill the whole process group so children don't outlive us.
	// Set Cancel/WaitDelay before Start: Start spawns a goroutine that reads
	// these fields, so assigning them afterward would be a data race.
	c.Cancel = func() error {
		killGroup(c.Process.Pid)
		return nil
	}
	// Backstop in case the killed process is slow to reap.
	c.WaitDelay = 5 * time.Second

	if err := c.Start(); err != nil {
		return execResult{}, err
	}

	waitErr := c.Wait()

	stdout, outTrunc := readCapped(outFile, execOutputCap)
	stderr, errTrunc := readCapped(errFile, execOutputCap)
	res := execResult{
		Stdout:    stdout,
		Stderr:    stderr,
		Truncated: outTrunc || errTrunc,
	}
	if ctx.Err() == context.DeadlineExceeded {
		res.TimedOut = true
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		res.ExitCode = exitErr.ExitCode()
	} else if waitErr != nil {
		res.ExitCode = -1
	}
	return res, nil
}
