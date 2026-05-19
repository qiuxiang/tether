package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/qiuxiang/tether/internal/forward"
)

func TestLoadHub(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("listen: \":8080\"\ntoken: \"abc\"\n"), 0600)

	cfg, err := LoadHub(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":8080" || cfg.Token != "abc" {
		t.Fatalf("got %+v", cfg)
	}
}

func TestLoadHubDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("token: \"abc\"\n"), 0600)

	cfg, err := LoadHub(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":7000" {
		t.Fatalf("expected default listen :7000, got %q", cfg.Listen)
	}
}

func TestLoadHubMissingToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("listen: \":7000\"\n"), 0600)

	_, err := LoadHub(path)
	if err == nil {
		t.Fatal("expected error on missing token")
	}
}

func TestLoadNode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("hub_url: \"wss://x.example/device\"\ntoken: \"abc\"\n"), 0600)

	cfg, err := LoadNode(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HubURL == "" || cfg.Token != "abc" {
		t.Fatalf("got %+v", cfg)
	}
	if cfg.LogDir == "" {
		t.Fatal("expected default log_dir")
	}
}

func TestLoadNodeForwards(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := "hub_url: ws://x\ntoken: t\nforwards:\n  - \"L 9000:mac:5037\"\n  - \"R mac:8080:3000\"\n"
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	c, err := LoadNode(p)
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

func TestLoadNodeForwardsInvalid(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := "hub_url: ws://x\ntoken: t\nforwards: [\"L bogus\"]\n"
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadNode(p); err == nil {
		t.Fatal("expected parse error")
	}
}
