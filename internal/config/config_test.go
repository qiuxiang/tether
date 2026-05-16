package config

import (
	"os"
	"path/filepath"
	"testing"
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
