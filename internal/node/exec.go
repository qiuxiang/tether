package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
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

// cappedBuffer is an io.Writer that retains at most cap bytes and records
// whether it had to drop any. Write always reports the full input length so
// the process's write never sees a short write.
type cappedBuffer struct {
	buf       bytes.Buffer
	cap       int
	truncated bool
}

func (w *cappedBuffer) Write(p []byte) (int, error) {
	room := w.cap - w.buf.Len()
	switch {
	case room <= 0:
		if len(p) > 0 {
			w.truncated = true
		}
	case len(p) <= room:
		w.buf.Write(p)
	default:
		w.buf.Write(p[:room])
		w.truncated = true
	}
	return len(p), nil
}

// shellArgv wraps a shell command string in the node's native shell.
func shellArgv(cmd string) []string {
	if runtime.GOOS == "windows" {
		return []string{"cmd", "/c", cmd}
	}
	return []string{"sh", "-c", cmd}
}

func mergeEnv(extra map[string]string) []string {
	base := os.Environ()
	for k, v := range extra {
		base = append(base, fmt.Sprintf("%s=%s", k, v))
	}
	return base
}

// runExec runs m.Cmd through the native shell to completion or until the
// timeout, whichever comes first. On timeout the whole process group is
// killed and TimedOut is set. The returned error is non-nil only when the
// process failed to start at the OS level (e.g. a bad working directory).
func runExec(ctx context.Context, m *protocol.Exec) (execResult, error) {
	timeout := defaultExecTimeout
	if m.Timeout > 0 {
		timeout = time.Duration(m.Timeout) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	argv := shellArgv(m.Cmd)
	c := exec.CommandContext(ctx, argv[0], argv[1:]...)
	c.Dir = m.Cwd
	c.Env = mergeEnv(m.Env)
	c.SysProcAttr = childAttrExec()

	stdout := &cappedBuffer{cap: execOutputCap}
	stderr := &cappedBuffer{cap: execOutputCap}
	c.Stdout = stdout
	c.Stderr = stderr

	// On timeout, kill the whole process group so children don't outlive us.
	// Set Cancel/WaitDelay before Start: Start spawns a goroutine that reads
	// these fields, so assigning them afterward would be a data race.
	c.Cancel = func() error {
		killGroup(c.Process.Pid)
		return nil
	}
	// If a grandchild keeps the output pipe open after the group is killed,
	// don't let Wait hang forever.
	c.WaitDelay = 5 * time.Second

	if err := c.Start(); err != nil {
		return execResult{}, err
	}

	err := c.Wait()

	res := execResult{
		Stdout:    stdout.buf.String(),
		Stderr:    stderr.buf.String(),
		Truncated: stdout.truncated || stderr.truncated,
	}
	if ctx.Err() == context.DeadlineExceeded {
		res.TimedOut = true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		res.ExitCode = exitErr.ExitCode()
	} else if err != nil {
		res.ExitCode = -1
	}
	return res, nil
}
