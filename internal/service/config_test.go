package service

import (
	"strings"
	"testing"
)

func TestBuildConfig(t *testing.T) {
	env := []KV{{Key: "FOO", Value: "bar"}, {Key: "TETHER_SERVICE_NAME", Value: "hijack"}}
	cfg, err := BuildConfig("tether-hub-b", "serve --config /etc/tether/hub-b.yaml", env, ScopeUser, "linux")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Name != "tether-hub-b" {
		t.Errorf("Name = %q, want tether-hub-b", cfg.Name)
	}
	wantArgs := []string{"serve", "--config", "/etc/tether/hub-b.yaml"}
	if len(cfg.Arguments) != len(wantArgs) {
		t.Fatalf("Arguments = %v, want %v", cfg.Arguments, wantArgs)
	}
	for i, a := range wantArgs {
		if cfg.Arguments[i] != a {
			t.Errorf("Arguments[%d] = %q, want %q", i, cfg.Arguments[i], a)
		}
	}
	if cfg.EnvVars["FOO"] != "bar" {
		t.Errorf("EnvVars[FOO] = %q, want bar", cfg.EnvVars["FOO"])
	}
	if _, ok := cfg.EnvVars["PATH"]; !ok {
		t.Error("EnvVars missing captured PATH")
	}
	if cfg.EnvVars["TETHER_SERVICE_NAME"] != "tether-hub-b" {
		t.Errorf("TETHER_SERVICE_NAME = %q, want tether-hub-b (must not be overridable via --env)", cfg.EnvVars["TETHER_SERVICE_NAME"])
	}
	if v, _ := cfg.Option["UserService"].(bool); !v {
		t.Error("Option[UserService] should be true for ScopeUser")
	}
}

func TestBuildConfigSystemScopeNoUserOption(t *testing.T) {
	cfg, err := BuildConfig("tether-hub", "serve", nil, ScopeSystem, "linux")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := cfg.Option["UserService"]; ok {
		t.Error("Option[UserService] should be unset for ScopeSystem")
	}
	if cfg.Option["Restart"] != "always" {
		t.Errorf("Option[Restart] = %v, want always", cfg.Option["Restart"])
	}
}

func TestBuildConfigLinuxSystemdScriptRestartSec(t *testing.T) {
	cfg, err := BuildConfig("tether-hub", "serve", nil, ScopeSystem, "linux")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	script, _ := cfg.Option["SystemdScript"].(string)
	if script == "" {
		t.Fatal("Option[SystemdScript] should be set for linux")
	}
	if !strings.Contains(script, "RestartSec=5") {
		t.Error("SystemdScript should contain RestartSec=5")
	}
	if strings.Contains(script, "RestartSec=120") {
		t.Error("SystemdScript should not contain RestartSec=120")
	}
	if !strings.Contains(script, "{{range $k, $v := .EnvVars") {
		t.Error("SystemdScript should still include the EnvVars range block")
	}
	if !strings.Contains(script, "{{if .Restart}}Restart={{.Restart}}{{end}}") {
		t.Error("SystemdScript should still include the Restart conditional")
	}
}

func TestBuildConfigDarwinNoSystemdScript(t *testing.T) {
	cfg, err := BuildConfig("tether-hub", "serve", nil, ScopeSystem, "darwin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := cfg.Option["SystemdScript"]; ok {
		t.Errorf("Option[SystemdScript] should be unset for darwin, got %v", v)
	}
}

func TestBuildConfigWindowsNoSystemdScript(t *testing.T) {
	cfg, err := BuildConfig("tether-node", "join", nil, ScopeSystem, "windows")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := cfg.Option["SystemdScript"]; ok {
		t.Errorf("Option[SystemdScript] should be unset for windows, got %v", v)
	}
}

func TestBuildConfigWindowsOnFailure(t *testing.T) {
	cfg, err := BuildConfig("tether-node", "join --config c.yaml", nil, ScopeSystem, "windows")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Option["OnFailure"] != "restart" {
		t.Errorf("Option[OnFailure] = %v, want restart", cfg.Option["OnFailure"])
	}
	if cfg.Option["OnFailureDelayDuration"] != "5s" {
		t.Errorf("Option[OnFailureDelayDuration] = %v, want 5s", cfg.Option["OnFailureDelayDuration"])
	}
	if cfg.Option["OnFailureResetPeriod"] != 60 {
		t.Errorf("Option[OnFailureResetPeriod] = %v, want 60", cfg.Option["OnFailureResetPeriod"])
	}
}

func TestControlConfig(t *testing.T) {
	cfg := ControlConfig("tether-hub", ScopeUser)
	if cfg.Name != "tether-hub" {
		t.Errorf("Name = %q, want tether-hub", cfg.Name)
	}
	if v, _ := cfg.Option["UserService"].(bool); !v {
		t.Error("Option[UserService] should be true for ScopeUser")
	}
}

func TestControlConfigSystemScopeNoUserOption(t *testing.T) {
	cfg := ControlConfig("tether-hub", ScopeSystem)
	if cfg.Name != "tether-hub" {
		t.Errorf("Name = %q, want tether-hub", cfg.Name)
	}
	if _, ok := cfg.Option["UserService"]; ok {
		t.Error("Option[UserService] should be unset for ScopeSystem")
	}
}

func TestBuildConfigNoArgs(t *testing.T) {
	cfg, err := BuildConfig("tether-hub", "", nil, ScopeUser, "linux")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Arguments) != 0 {
		t.Errorf("Arguments = %v, want empty", cfg.Arguments)
	}
}
