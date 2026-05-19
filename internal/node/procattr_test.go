//go:build !windows

package node

import (
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// TestStartProcessGroupCleanupOnShutdown verifies that Shutdown() sends SIGTERM
// to all running process groups, causing the child to exit.
func TestStartProcessGroupCleanupOnShutdown(t *testing.T) {
	h := NewProcessHandler(t.TempDir(), 50)

	started := make(chan int, 1) // receives pid
	done := make(chan int, 1)    // receives exit code

	p := &Process{ID: "test-shutdown-1", Description: "sleep", Cmd: []string{"sleep", "30"}}
	err := p.Start(t.Context(), h.logDir, nil, "", func(code int) {
		done <- code
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	h.registry.Add(p)

	p.mu.Lock()
	pid := p.Pid
	p.mu.Unlock()
	started <- pid

	childPid := <-started
	if childPid <= 0 {
		t.Fatal("expected non-zero pid")
	}

	h.Shutdown()

	// Wait for process to exit (via onExit callback or direct signal check).
	select {
	case <-done:
		// exited via onExit — good
	case <-time.After(3 * time.Second):
		// onExit may not fire if context is still alive; check OS-level.
	}

	// Poll until the process is gone (ESRCH) or timeout.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(childPid, 0)
		if err == syscall.ESRCH {
			return // success: process is gone
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("process %d still alive after Shutdown + 3s", childPid)
}

// TestStartProcessGroupIncludesGrandchild verifies that killing by pgroup
// (via Shutdown) also terminates grandchildren in the same pgroup.
func TestStartProcessGroupIncludesGrandchild(t *testing.T) {
	// Only run if pgrep is available.
	if _, err := exec.LookPath("pgrep"); err != nil {
		t.Skip("pgrep not available")
	}

	h := NewProcessHandler(t.TempDir(), 50)
	done := make(chan struct{})

	// Start a shell that spawns an inner sleep and waits for it.
	// The inner sleep shares the same pgroup as the shell (since we use Setpgid
	// on the shell, and the shell forks without changing pgid).
	p := &Process{
		ID:          "test-shutdown-grandchild",
		Description: "shell-with-child",
		Cmd:         []string{"sh", "-c", "sleep 30 & wait"},
	}
	err := p.Start(t.Context(), h.logDir, nil, "", func(_ int) {
		close(done)
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	h.registry.Add(p)

	p.mu.Lock()
	pid := p.Pid
	p.mu.Unlock()

	// Give child time to spawn grandchild.
	time.Sleep(200 * time.Millisecond)

	// Find grandchild (child of pid).
	out, err := exec.Command("pgrep", "-P", itoa(pid)).Output()
	if err != nil || len(out) == 0 {
		t.Skip("could not find grandchild via pgrep; skipping grandchild assertion")
	}
	grandchildPid := parseFirstPid(string(out))

	h.Shutdown()

	// Wait for shell to exit.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}

	// Check both pid and grandchild are gone.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		parentGone := syscall.Kill(pid, 0) == syscall.ESRCH
		grandchildGone := grandchildPid <= 0 || syscall.Kill(grandchildPid, 0) == syscall.ESRCH
		if parentGone && grandchildGone {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("process(es) still alive after Shutdown: parent=%d grandchild=%d",
		pid, grandchildPid)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 10)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}

func parseFirstPid(s string) int {
	n := 0
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		} else if n > 0 {
			break
		}
	}
	return n
}
