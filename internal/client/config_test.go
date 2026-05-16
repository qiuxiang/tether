package client

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("hub_url: wss://h\ntoken: tk\n"), 0o600))

	cfg, err := Load(p)
	require.NoError(t, err)
	require.Equal(t, "wss://h", cfg.HubURL)
	require.Equal(t, "tk", cfg.Token)
}

func TestLoadConfigMissingFields(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte("hub_url: wss://h\n"), 0o600))

	_, err := Load(p)
	require.Error(t, err)
}
