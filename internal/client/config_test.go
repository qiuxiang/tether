package client

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/qiuxiang/tether/internal/forward"
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

func TestLoadForwards(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "client.yaml")
	body := "hub_url: ws://x\ntoken: t\nforwards:\n  - \"L 9000:mac:5037\"\n  - \"R mac:8080:3000\"\n"
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Forwards) != 2 {
		t.Fatalf("got %d rules", len(c.Forwards))
	}
	if c.Forwards[0].Dir != forward.DirLocal || c.Forwards[1].Dir != forward.DirRemote {
		t.Fatalf("dirs wrong: %+v", c.Forwards)
	}
}

func TestLoadForwardsInvalid(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "client.yaml")
	body := "hub_url: ws://x\ntoken: t\nforwards: [\"L bogus\"]\n"
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err == nil {
		t.Fatal("expected parse error")
	}
}
