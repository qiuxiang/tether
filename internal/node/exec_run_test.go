//go:build !windows

package node

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadCapped(t *testing.T) {
	f, err := os.CreateTemp("", "rc-*")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	defer f.Close()
	_, err = f.WriteString("abcdef")
	require.NoError(t, err)

	s, trunc := readCapped(f, 4)
	assert.Equal(t, "abcd", string(s))
	assert.True(t, trunc, "more than cap bytes must report truncated")

	s, trunc = readCapped(f, 100)
	assert.Equal(t, "abcdef", string(s))
	assert.False(t, trunc)
}

func TestRunExecCapturesOutputAndExit(t *testing.T) {
	res, err := runExec(context.Background(), &protocol.Exec{
		Args: []string{"sh", "-c", "echo out; echo err 1>&2; exit 7"},
	})
	require.NoError(t, err)
	assert.Equal(t, "out\n", res.Stdout)
	assert.Equal(t, "err\n", res.Stderr)
	assert.Equal(t, 7, res.ExitCode)
	assert.False(t, res.TimedOut)
	assert.False(t, res.Truncated)
}

// TestRunExecNoShell verifies args are spawned directly: a shell would treat
// `|` as a pipe, but as a plain argv entry it must come back verbatim.
func TestRunExecNoShell(t *testing.T) {
	res, err := runExec(context.Background(), &protocol.Exec{
		Args: []string{"echo", "a|b"},
	})
	require.NoError(t, err)
	assert.Equal(t, "a|b\n", res.Stdout)
	assert.Equal(t, 0, res.ExitCode)
}

func TestRunExecTimeoutKills(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Args:    []string{"sh", "-c", "echo started; sleep 30"},
		Timeout: 1,
	})
	require.NoError(t, err)
	assert.True(t, res.TimedOut, "expected timed_out")
	assert.Contains(t, res.Stdout, "started")
	assert.Less(t, time.Since(start), 10*time.Second, "runExec must return shortly after the timeout")
}

func TestRunExecStartError(t *testing.T) {
	_, err := runExec(context.Background(), &protocol.Exec{
		Args: []string{"true"},
		Cwd:  "/no/such/directory/exists",
	})
	require.Error(t, err, "a bad working directory must surface as a start error")
}

func TestRunExecEmptyArgs(t *testing.T) {
	_, err := runExec(context.Background(), &protocol.Exec{})
	require.Error(t, err, "empty args must be rejected")
}
