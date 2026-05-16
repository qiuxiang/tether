package client

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUploadAndDownload(t *testing.T) {
	c, _, cleanup := setupClusterWithClient(t)
	defer cleanup()

	dir := t.TempDir()
	local := filepath.Join(dir, "in.bin")
	payload := []byte("hello file transfer")
	require.NoError(t, os.WriteFile(local, payload, 0o644))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Upload local → node
	remote := filepath.Join(dir, "remote.bin")
	res, err := upload(ctx, c, local, "n1", remote, false)
	require.NoError(t, err)
	require.False(t, res.IsError)
	got, err := os.ReadFile(remote)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// Download node → another local path
	out := filepath.Join(dir, "out.bin")
	res, err = download(ctx, c, "n1", remote, out, false)
	require.NoError(t, err)
	require.False(t, res.IsError)
	got2, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, payload, got2)
}

func TestParsePath(t *testing.T) {
	cases := []struct {
		in        string
		wantNode  string
		wantPath  string
		wantError bool
	}{
		{"node1:/abs", "node1", "/abs", false},
		{"node1:~/x", "node1", "~/x", false},
		{"/abs/path", "", "/abs/path", false},
		{"~/local", "", "~/local", false},
	}
	for _, tc := range cases {
		p, err := parsePath(tc.in)
		if tc.wantError {
			require.Error(t, err, "input %q", tc.in)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tc.wantNode, p.Node, "input %q", tc.in)
		require.Equal(t, tc.wantPath, p.Path, "input %q", tc.in)
	}
}
