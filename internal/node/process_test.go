package node

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessStartEchoAndRead(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "p1", Cmd: []string{"sh", "-c", "echo hello; echo world"}}
	var wg sync.WaitGroup
	wg.Add(1)
	err := p.Start(context.Background(), dir, nil, "", false, func(code int) { wg.Done() })
	require.NoError(t, err)
	wg.Wait()

	data, _, eof, err := p.ReadOutput(0, 1024)
	require.NoError(t, err)
	assert.True(t, eof)
	assert.Contains(t, string(data), "hello")
	assert.Contains(t, string(data), "world")
}

func TestProcessKill(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "p2", Cmd: []string{"sh", "-c", "sleep 10"}}
	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, p.Start(context.Background(), dir, nil, "", false, func(int) { wg.Done() }))

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, p.Kill("KILL"))

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("kill did not terminate sleep")
	}
	assert.Equal(t, "exited", p.Status)
}

func TestProcessStdin(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "p3", Cmd: []string{"cat"}}
	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, p.Start(context.Background(), dir, nil, "", false, func(int) { wg.Done() }))

	require.NoError(t, p.WriteStdin([]byte("ping\n")))
	time.Sleep(50 * time.Millisecond)
	p.Kill("KILL")
	wg.Wait()

	data, _, _, _ := p.ReadOutput(0, 1024)
	assert.True(t, strings.Contains(string(data), "ping"))
}
