package node

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/aymanbagabas/go-pty"
	"github.com/hinshun/vt10x"
	"github.com/qiuxiang/tether/internal/protocol"
)

// closeSlave closes only the slave end of the PTY when possible. On Unix
// this lets the master's pending buffer drain through Read→EOF instead of
// being discarded by an early master Close. On Windows (ConPty) the slave
// concept differs and there's nothing to close separately, so we no-op.
func closeSlave(p pty.Pty) {
	if u, ok := p.(pty.UnixPty); ok {
		_ = u.Slave().Close()
	}
}

// runExecStreamPTY runs an exec command attached to a PTY.
func runExecStreamPTY(ctx context.Context, m *protocol.Exec, send Sender) (int, error) {
	p, err := pty.New()
	if err != nil {
		return -1, err
	}

	c := p.CommandContext(ctx, m.Cmd[0], m.Cmd[1:]...)
	c.Dir = m.Cwd
	c.Env = mergeEnv(m.Env)
	c.SysProcAttr = childAttrPTY()

	if err := c.Start(); err != nil {
		p.Close()
		return -1, err
	}

	if len(m.Stdin) > 0 {
		p.Write(m.Stdin)
	}

	// Drain PTY output in a goroutine. The goroutine exits when the PTY is
	// closed (which happens after c.Wait returns below).
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, readErr := p.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				send.Send(&protocol.ExecOutput{MsgID: m.MsgID, Stream: "stdout", Data: chunk})
			}
			if readErr != nil {
				return
			}
		}
	}()

	// Wait for the process to finish. Then close the slave end so the master's
	// Read returns EOF naturally — only after the reader has drained whatever
	// bytes the child wrote just before exiting. Closing the master directly
	// here would discard buffered slave→master data that the reader hasn't
	// picked up yet, which surfaced as a flaky "TTY" detection test under load.
	werr := c.Wait()
	closeSlave(p)
	wg.Wait()
	p.Close()

	code := 0
	if exitErr, ok := werr.(*exec.ExitError); ok {
		code = exitErr.ExitCode()
	} else if werr != nil {
		code = -1
	}
	return code, nil
}

// startPTY launches a long-running process in a PTY, mirroring startPipe's lifecycle.
func (proc *Process) startPTY(ctx context.Context, logDir string, env map[string]string, cwd string, onExit func(code int)) error {
	if err := os.MkdirAll(logDir, 0700); err != nil {
		return err
	}
	proc.LogPath = filepath.Join(logDir, proc.ID+".log")
	logFile, err := os.OpenFile(proc.LogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	p, err := pty.New()
	if err != nil {
		logFile.Close()
		return err
	}
	if err := p.Resize(vtCols, ptyVisibleRows); err != nil {
		p.Close()
		logFile.Close()
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	c := p.CommandContext(ctx, proc.Cmd[0], proc.Cmd[1:]...)
	c.Dir = cwd
	c.Env = mergeEnv(env)
	c.SysProcAttr = childAttrPTY()

	proc.vt = vt10x.New(vt10x.WithSize(vtCols, vtRows))

	if err := c.Start(); err != nil {
		p.Close()
		logFile.Close()
		cancel()
		return err
	}

	proc.mu.Lock()
	proc.Status = "running"
	proc.StartedAt = time.Now()
	proc.LastActiveAt = proc.StartedAt
	proc.cancel = cancel
	proc.Pid = c.Process.Pid
	stdinCh := make(chan []byte, 16)
	proc.stdin = stdinCh
	proc.mu.Unlock()

	// Pump stdin → PTY.
	go func() {
		for data := range stdinCh {
			p.Write(data)
		}
	}()

	// Pump PTY → log + VT. Done signal used to sequence Close calls.
	var copyDone sync.WaitGroup
	copyDone.Add(1)
	go func() {
		defer copyDone.Done()
		io.Copy(io.MultiWriter(logFile, &vtSink{p: proc}), p)
	}()

	go func() {
		err := c.Wait()
		code := 0
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else if err != nil {
			code = -1
		}
		proc.mu.Lock()
		proc.Status = "exited"
		proc.ExitCode = &code
		proc.LastActiveAt = time.Now()
		close(stdinCh)
		proc.stdin = nil
		proc.mu.Unlock()
		// Close the slave end first so the master's io.Copy reads remaining
		// buffered output and then sees EOF cleanly; closing the master while
		// data is still in the slave→master buffer discards those bytes.
		closeSlave(p)
		copyDone.Wait()
		p.Close()
		logFile.Close()
		cancel()
		onExit(code)
	}()
	return nil
}
