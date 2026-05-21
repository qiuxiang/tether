package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/qiuxiang/tether/internal/forward"
)

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "token: abc\nlisten: \":8080\"\nhub_url: wss://x.example/device\nhostname_override: host-a\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token != "abc" || cfg.Listen != ":8080" {
		t.Fatalf("got %+v", cfg)
	}
	if cfg.HubURL != "wss://x.example/device" || cfg.HostnameOverride != "host-a" {
		t.Fatalf("got %+v", cfg)
	}
}

func TestLoadDefaultsListen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("token: abc\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":7000" {
		t.Fatalf("expected default listen :7000, got %q", cfg.Listen)
	}
}

func TestLoadMissingToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("listen: \":7000\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected error on missing token")
	}
}

func TestLoadForwards(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "token: t\nhub_url: ws://x\nforwards:\n  - \"L 9000:mac:5037\"\n  - \"R mac:8080:3000\"\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Forwards) != 2 {
		t.Fatalf("got %d rules", len(cfg.Forwards))
	}
	if cfg.Forwards[0].Dir != forward.DirLocal || cfg.Forwards[1].Dir != forward.DirRemote {
		t.Fatalf("dirs wrong: %+v", cfg.Forwards)
	}
}

func TestLoadForwardsInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "token: t\nhub_url: ws://x\nforwards: [\"L bogus\"]\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}
