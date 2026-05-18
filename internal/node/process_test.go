package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
}

func TestStart_NonPTY_FeedsVT(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "vt-pipe", Cmd: []string{"sh", "-c", "printf 'hello\\nworld\\n'"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", false, func(code int) { close(done) }); err != nil {
		t.Fatal(err)
	}
	<-done

	lines, _, _, total := p.CaptureScreen(nil, nil)
	if total != 2 || len(lines) != 2 || lines[0] != "hello" || lines[1] != "world" {
		t.Fatalf("got lines=%q total=%d", lines, total)
	}
}
