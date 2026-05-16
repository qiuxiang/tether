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
	"github.com/qiuxiang/tether/internal/protocol"
)

// runExecStreamPTY runs an exec command attached to a PTY.
func runExecStreamPTY(ctx context.Context, m *protocol.Exec, send Sender) (int, error) {
	p, err := pty.New()
	if err != nil {
		return -1, err
	}

	c := p.CommandContext(ctx, m.Cmd[0], m.Cmd[1:]...)
	c.Dir = m.Cwd
	c.Env = mergeEnv(m.Env)

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

	// Wait for the process to finish, then close the PTY to unblock the reader.
	werr := c.Wait()
	p.Close()
	wg.Wait()

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

	ctx, cancel := context.WithCancel(ctx)
	c := p.CommandContext(ctx, proc.Cmd[0], proc.Cmd[1:]...)
	c.Dir = cwd
	c.Env = mergeEnv(env)

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
	stdinCh := make(chan []byte, 16)
	proc.stdin = stdinCh
	proc.mu.Unlock()

	// Pump stdin → PTY.
	go func() {
		for data := range stdinCh {
			p.Write(data)
		}
	}()

	// Pump PTY → log. Done signal used to sequence Close calls.
	var copyDone sync.WaitGroup
	copyDone.Add(1)
	go func() {
		defer copyDone.Done()
		io.Copy(logFile, p)
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
		// Close PTY to unblock io.Copy, then wait for it to finish before
		// closing the log file.
		p.Close()
		copyDone.Wait()
		logFile.Close()
		cancel()
		onExit(code)
	}()
	return nil
}
