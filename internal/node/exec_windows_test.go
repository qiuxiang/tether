//go:build windows

package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These run on a real Windows node. Build with:
//   GOOS=windows GOARCH=amd64 go test -c ./internal/node -o node_windows_test.exe
// then run on the device:  node_windows_test.exe -test.run TestWin -test.v

func TestWinExecEchoAndExit(t *testing.T) {
	res, err := runExec(context.Background(), &protocol.Exec{
		Args: []string{"cmd", "/c", "echo hi & exit 7"},
	})
	require.NoError(t, err)
	assert.Contains(t, res.Stdout, "hi")
	assert.Equal(t, 7, res.ExitCode)
	assert.False(t, res.TimedOut)
}

func TestWinExecStderr(t *testing.T) {
	res, err := runExec(context.Background(), &protocol.Exec{
		Args: []string{"cmd", "/c", "echo oops 1>&2"},
	})
	require.NoError(t, err)
	assert.Contains(t, res.Stderr, "oops")
	assert.False(t, res.TimedOut)
}

// TestWinExecTimeoutKills: a long command must be killed at the deadline,
// reported timed_out, and runExec must return shortly after. With killGroup a
// no-op the process is not actually killed; this catches that regression.
func TestWinExecTimeoutKills(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Args:    []string{"ping", "-n", "30", "127.0.0.1"},
		Timeout: 1,
	})
	require.NoError(t, err)
	assert.True(t, res.TimedOut, "expected timed_out=true")
	assert.Less(t, time.Since(start), 12*time.Second, "must return shortly after the timeout")
}

// TestWinExecTimeoutActuallyKills verifies the timeout kills the whole tree,
// not just closes the pipe. The command pings for ~6s and only then writes a
// marker; with a real tree-kill at the 1s timeout the marker must never appear.
// On the old no-op killGroup the child survived and wrote it.
func TestWinExecTimeoutActuallyKills(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "marker.txt")
	cmd := `ping -n 7 127.0.0.1 >NUL & echo done > "` + marker + `"`
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Args:    []string{"cmd", "/c", cmd},
		Timeout: 1,
	})
	require.NoError(t, err)
	assert.True(t, res.TimedOut, "expected timed_out=true")
	assert.Less(t, time.Since(start), 10*time.Second)
	// Wait past when the marker would have been written had the child lived.
	time.Sleep(9 * time.Second)
	_, statErr := os.Stat(marker)
	assert.Truef(t, os.IsNotExist(statErr),
		"child survived the timeout; tree-kill failed (marker=%s statErr=%v)", marker, statErr)
}

// TestWinExecLingeringChild reproduces the field hang (wmic/systeminfo/CIM): a
// command that finishes immediately but leaves a detached child holding the
// output handle must NOT hang runExec nor be falsely reported timed_out.
func TestWinExecLingeringChild(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Args:    []string{"cmd", "/c", "start /b ping -n 30 127.0.0.1"},
		Timeout: 20,
	})
	require.NoError(t, err)
	assert.False(t, res.TimedOut, "command finished at once; must not be timed_out")
	assert.Less(t, time.Since(start), 12*time.Second, "child must be reaped so runExec returns promptly")
}

// TestWinExecPowershell mirrors the real failure: a powershell call that
// completes must return its output promptly, not hang.
func TestWinExecPowershell(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Args:    []string{"powershell", "-NoProfile", "-Command", "Write-Output marker123"},
		Timeout: 25,
	})
	require.NoError(t, err)
	assert.False(t, res.TimedOut, "powershell completed; must not be timed_out (hang)")
	assert.Contains(t, res.Stdout, "marker123")
	assert.Less(t, time.Since(start), 15*time.Second, "must return promptly")
}

// TestWinExecQuotedArg checks that an argument with spaces and embedded quotes
// reaches the program as a single argv entry under direct spawn.
func TestWinExecQuotedArg(t *testing.T) {
	res, err := runExec(context.Background(), &protocol.Exec{
		Args:    []string{"powershell", "-NoProfile", "-Command", `Write-Output "quoted ok"`},
		Timeout: 25,
	})
	require.NoError(t, err)
	assert.False(t, res.TimedOut)
	assert.Contains(t, res.Stdout, "quoted ok")
}

// --- The exact commands that hung in the field, run directly via runExec. ---
// If these hang here too, the bug is at the runExec/OS layer. If they pass,
// the field hangs were node sluggishness (accumulated un-killed children).

func TestWinFieldVer(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Args: []string{"cmd", "/c", "ver"}, Timeout: 8,
	})
	require.NoError(t, err)
	t.Logf("ver: elapsed=%v timedout=%v exit=%d stdout=%q stderr=%q",
		time.Since(start), res.TimedOut, res.ExitCode, res.Stdout, res.Stderr)
	assert.False(t, res.TimedOut, "ver should not time out")
	assert.Contains(t, res.Stdout, "indows")
}

func TestWinFieldWmic(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Args: []string{"wmic", "os", "get", "Caption", "/value"}, Timeout: 12,
	})
	require.NoError(t, err)
	t.Logf("wmic: elapsed=%v timedout=%v exit=%d stdout=%q stderr=%q",
		time.Since(start), res.TimedOut, res.ExitCode, res.Stdout, res.Stderr)
	assert.False(t, res.TimedOut, "wmic should not time out")
}

func TestWinFieldSysteminfo(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Args: []string{"systeminfo"}, Timeout: 40,
	})
	require.NoError(t, err)
	t.Logf("systeminfo: elapsed=%v timedout=%v exit=%d len(stdout)=%d",
		time.Since(start), res.TimedOut, res.ExitCode, len(res.Stdout))
}

// A PowerShell pipeline with metacharacters (| and spaces) passed as one argv
// entry — the config-dump case that came back garbled under the old cmd /c
// quoting hack. Direct spawn must hand it to powershell intact.
func TestWinFieldPowershellCim(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Args: []string{"powershell", "-NoProfile", "-Command",
			"Get-CimInstance Win32_OperatingSystem | Select-Object -ExpandProperty Caption"},
		Timeout: 25,
	})
	require.NoError(t, err)
	t.Logf("ps-cim: elapsed=%v timedout=%v exit=%d stdout=%q stderr=%q",
		time.Since(start), res.TimedOut, res.ExitCode, res.Stdout, res.Stderr)
	assert.False(t, res.TimedOut)
	assert.Contains(t, res.Stdout, "Windows", "quoted powershell pipeline must run, not echo back")
}
