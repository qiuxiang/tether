package node

import (
	"context"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCappedBuffer(t *testing.T) {
	w := &cappedBuffer{cap: 4}
	n, err := w.Write([]byte("ab"))
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.False(t, w.truncated)

	n, err = w.Write([]byte("cdef"))
	require.NoError(t, err)
	assert.Equal(t, 4, n, "Write must report the full input length")
	assert.Equal(t, "abcd", w.buf.String())
	assert.True(t, w.truncated)
}

func TestRunExecCapturesOutputAndExit(t *testing.T) {
	res, err := runExec(context.Background(), &protocol.Exec{
		Cmd: []string{"sh", "-c", "echo out; echo err 1>&2; exit 7"},
	})
	require.NoError(t, err)
	assert.Contains(t, res.Stdout, "out")
	assert.Contains(t, res.Stderr, "err")
	assert.Equal(t, 7, res.ExitCode)
	assert.False(t, res.TimedOut)
	assert.False(t, res.Truncated)
}

func TestRunExecTimeoutKills(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Cmd:     []string{"sh", "-c", "echo started; sleep 30"},
		Timeout: 1,
	})
	require.NoError(t, err)
	assert.True(t, res.TimedOut, "expected timed_out")
	assert.Contains(t, res.Stdout, "started")
	assert.Less(t, time.Since(start), 10*time.Second, "runExec must return shortly after the timeout")
}

func TestRunExecStartError(t *testing.T) {
	_, err := runExec(context.Background(), &protocol.Exec{
		Cmd: []string{"sh", "-c", "true"},
		Cwd: "/no/such/directory/exists",
	})
	require.Error(t, err, "a bad working directory must surface as a start error")
}
