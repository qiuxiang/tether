package node

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

// removeFile is a var so tests can stub it. Default: os.Remove.
var removeFile = os.Remove

// Process represents a managed OS process on the node agent.
type Process struct {
	ID           string
	Name         string
	Cmd          []string
	Status       string // "running" | "exited"
	StartedAt    time.Time
	LastActiveAt time.Time
	ExitCode     *int
	LogPath      string

	// Runtime handles; nil after exit.
	mu     sync.Mutex
	stdin  chan<- []byte // nil if not started or already closed
	cancel func()
}

// Start launches a process under the agent, writes output to logPath, and
// returns once the OS-level start has succeeded. Exit is reported via onExit.
func (p *Process) Start(ctx context.Context, logDir string, env map[string]string, cwd string, tty bool, onExit func(code int)) error {
	if tty {
		return p.startPTY(ctx, logDir, env, cwd, onExit)
	}
	return p.startPipe(ctx, logDir, env, cwd, onExit)
}

func (p *Process) startPipe(ctx context.Context, logDir string, env map[string]string, cwd string, onExit func(code int)) error {
	if err := os.MkdirAll(logDir, 0700); err != nil {
		return err
	}
	p.LogPath = filepath.Join(logDir, p.ID+".log")
	logFile, err := os.OpenFile(p.LogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, p.Cmd[0], p.Cmd[1:]...)
	cmd.Dir = cwd
	cmd.Env = mergeEnv(env)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	stdin, err := cmd.StdinPipe()
	if err != nil {
		logFile.Close()
		cancel()
		return err
	}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		cancel()
		return err
	}

	p.mu.Lock()
	p.Status = "running"
	p.StartedAt = time.Now()
	p.LastActiveAt = p.StartedAt
	p.cancel = cancel
	stdinCh := make(chan []byte, 16)
	p.stdin = stdinCh
	p.mu.Unlock()

	go pumpStdin(stdin, stdinCh)

	go func() {
		err := cmd.Wait()
		code := 0
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else if err != nil {
			code = -1
		}
		p.mu.Lock()
		p.Status = "exited"
		p.ExitCode = &code
		p.LastActiveAt = time.Now()
		close(stdinCh)
		p.stdin = nil
		p.mu.Unlock()
		logFile.Close()
		cancel()
		onExit(code)
	}()
	return nil
}

func pumpStdin(w io.WriteCloser, ch <-chan []byte) {
	defer w.Close()
	for data := range ch {
		if _, err := w.Write(data); err != nil {
			return
		}
	}
}

func mergeEnv(extra map[string]string) []string {
	base := os.Environ()
	for k, v := range extra {
		base = append(base, fmt.Sprintf("%s=%s", k, v))
	}
	return base
}

// WriteStdin sends data to the process's stdin. Returns error if exited.
func (p *Process) WriteStdin(data []byte) error {
	p.mu.Lock()
	ch := p.stdin
	p.mu.Unlock()
	if ch == nil {
		return fmt.Errorf("process not accepting stdin")
	}
	select {
	case ch <- data:
		return nil
	case <-time.After(time.Second):
		return fmt.Errorf("stdin send timed out")
	}
}

func (p *Process) Kill(signal string) error {
	p.mu.Lock()
	cancel := p.cancel
	p.mu.Unlock()
	if cancel == nil {
		return fmt.Errorf("process not running")
	}
	// Signal is a hint; for simplicity, KILL → cancel ctx (SIGKILL via CommandContext);
	// TERM → graceful via cancel as well. Refine later if needed.
	_ = signal
	_ = syscall.SIGTERM // reserve for future graceful signal path
	cancel()
	return nil
}

func runExecStream(ctx context.Context, m *protocol.Exec, send Sender) (int, error) {
	if m.TTY {
		return runExecStreamPTY(ctx, m, send)
	}
	cmd := exec.CommandContext(ctx, m.Cmd[0], m.Cmd[1:]...)
	cmd.Dir = m.Cwd
	cmd.Env = mergeEnv(m.Env)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return -1, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return -1, err
	}
	var stdin io.WriteCloser
	if len(m.Stdin) > 0 {
		stdin, err = cmd.StdinPipe()
		if err != nil {
			return -1, err
		}
	}

	if err := cmd.Start(); err != nil {
		return -1, err
	}
	if stdin != nil {
		stdin.Write(m.Stdin)
		stdin.Close()
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go streamReader(stdout, "stdout", m.MsgID, send, &wg)
	go streamReader(stderr, "stderr", m.MsgID, send, &wg)
	wg.Wait()

	werr := cmd.Wait()
	code := 0
	if exitErr, ok := werr.(*exec.ExitError); ok {
		code = exitErr.ExitCode()
	} else if werr != nil {
		code = -1
	}
	return code, nil
}

func streamReader(r io.Reader, stream, msgID string, send Sender, wg *sync.WaitGroup) {
	defer wg.Done()
	br := bufio.NewReader(r)
	buf := make([]byte, 4096)
	for {
		n, err := br.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			send.Send(&protocol.ExecOutput{MsgID: msgID, Stream: stream, Data: chunk})
		}
		if err != nil {
			return
		}
	}
}

// ReadOutput returns log bytes starting at offset, up to length. eof=true when
// the process has exited AND we've returned everything up to current file size.
func (p *Process) ReadOutput(offset int64, length int) ([]byte, int64, bool, error) {
	if p.LogPath == "" {
		return nil, 0, false, fmt.Errorf("no log")
	}
	f, err := os.Open(p.LogPath)
	if err != nil {
		return nil, 0, false, err
	}
	defer f.Close()
	stat, _ := f.Stat()
	size := stat.Size()
	if offset >= size {
		p.mu.Lock()
		exited := p.Status == "exited"
		p.mu.Unlock()
		return nil, offset, exited, nil
	}
	if length <= 0 || length > 1<<20 {
		length = 65536
	}
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, offset, false, err
	}
	next := offset + int64(n)
	p.mu.Lock()
	exited := p.Status == "exited" && next >= size
	p.mu.Unlock()
	return buf[:n], next, exited, nil
}
