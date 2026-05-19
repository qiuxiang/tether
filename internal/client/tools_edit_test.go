package client

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestReadFileHappyPath(t *testing.T) {
	c, _, cleanup := setupClusterWithClient(t)
	defer cleanup()

	// Write a temp file on the node (which runs locally in the test).
	dir := t.TempDir()
	p := filepath.Join(dir, "hello.txt")
	require.NoError(t, os.WriteFile(p, []byte("line1\nline2\nline3\n"), 0o644))

	id := NewMsgID()
	ch := c.rpc.Register(id)
	defer c.rpc.Unregister(id)
	require.NoError(t, c.Send(&protocol.ReadFileReq{
		MsgID: id, Target: "n1", Path: p, Offset: 0, Limit: 100,
	}))
	select {
	case r := <-ch:
		require.True(t, r.OK, "expected OK, got error: %s", r.Error)
		require.NotNil(t, r.Data["lines"])
	case <-time.After(3 * time.Second):
		t.Fatal("read_file timed out")
	}
}

func TestMustNodePathRejectsLocalPath(t *testing.T) {
	_, _, err := mustNodePath("/abs/local/path")
	require.Error(t, err)
	require.Contains(t, err.Error(), "node:/abs/path")

	// node: prefix should be accepted
	node, path, err := mustNodePath("mynode:/etc/hosts")
	require.NoError(t, err)
	require.Equal(t, "mynode", node)
	require.Equal(t, "/etc/hosts", path)
}

func TestWriteFileHappyPath(t *testing.T) {
	c, _, cleanup := setupClusterWithClient(t)
	defer cleanup()

	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")

	id := NewMsgID()
	ch := c.rpc.Register(id)
	defer c.rpc.Unregister(id)
	require.NoError(t, c.Send(&protocol.WriteFileReq{
		MsgID: id, Target: "n1", Path: p,
		Content: []byte("hello world\n"),
	}))
	select {
	case r := <-ch:
		require.True(t, r.OK, "expected OK, got error: %s", r.Error)
		got, err := os.ReadFile(p)
		require.NoError(t, err)
		require.Equal(t, "hello world\n", string(got))
	case <-time.After(3 * time.Second):
		t.Fatal("write_file timed out")
	}
}

func TestEditFileNotUnique(t *testing.T) {
	c, _, cleanup := setupClusterWithClient(t)
	defer cleanup()

	dir := t.TempDir()
	p := filepath.Join(dir, "dup.txt")
	// Write file with duplicate occurrences of "foo"
	require.NoError(t, os.WriteFile(p, []byte("foo\nfoo\n"), 0o644))

	id := NewMsgID()
	ch := c.rpc.Register(id)
	defer c.rpc.Unregister(id)
	require.NoError(t, c.Send(&protocol.EditFileReq{
		MsgID: id, Target: "n1", Path: p,
		OldString: []byte("foo"), NewString: []byte("bar"),
		ReplaceAll: false,
	}))
	select {
	case r := <-ch:
		require.False(t, r.OK)
		require.True(t, strings.Contains(r.Error, "not_unique") || strings.Contains(r.Error, "unique"),
			"expected not_unique error, got: %s", r.Error)
	case <-time.After(3 * time.Second):
		t.Fatal("edit_file timed out")
	}
}

func TestAwaitReplyContextCancel(t *testing.T) {
	// Verify awaitReply returns an error result when context is cancelled
	// without sending anything on the channel.
	ch := make(chan *protocol.Reply, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	res, err := awaitReply(ctx, ch)
	require.NoError(t, err)
	require.True(t, res.IsError)
}
