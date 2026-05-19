package node

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hinshun/vt10x"
)

// removeFile is a var so tests can stub it. Default: os.Remove.
var removeFile = os.Remove

// Process represents a managed OS process on the node agent.
type Process struct {
	ID           string
	Description  string
	Cmd          []string
	Status       string // "running" | "exited"
	StartedAt    time.Time
	LastActiveAt time.Time
	ExitCode     *int
	LogPath      string
	Pid          int

	// Runtime handles; nil after exit.
	mu     sync.Mutex
	stdin  chan<- []byte // nil if not started or already closed
	cancel func()

	vt   vt10x.Terminal
	vtMu sync.Mutex

	// bus is the live raw-byte stream used by Attach subscribers. Always
	// non-nil for a running process; Close()d after exit so subscribers
	// see EOF naturally.
	bus *byteBus
}

// Start launches a process under the agent inside a PTY, writes output to
// logPath, and returns once the OS-level start has succeeded. Exit is
// reported via onExit.
func (p *Process) Start(ctx context.Context, logDir string, env map[string]string, cwd string, onExit func(code int)) error {
	return p.startPTY(ctx, logDir, env, cwd, onExit)
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

