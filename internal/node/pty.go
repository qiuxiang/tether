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
	proc.bus = newByteBus()

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
		io.Copy(io.MultiWriter(logFile, &vtSink{p: proc}, proc.bus), p)
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
		proc.bus.Close()
		cancel()
		onExit(code)
	}()
	return nil
}
