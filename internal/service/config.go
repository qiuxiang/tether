package service

import (
	"os"
	"strings"

	"github.com/kardianos/service"
)

// linuxSystemdScript is kardianos' default systemd unit template
// (service_systemd_linux.go) with RestartSec lowered from 120 to 5 so a
// crashed Linux service self-heals promptly instead of waiting two minutes.
const linuxSystemdScript = `[Unit]
Description={{.Description}}
ConditionFileIsExecutable={{.Path|cmdEscape}}
{{range $i, $dep := .Dependencies}} 
{{$dep}} {{end}}

[Service]
StartLimitInterval=5
StartLimitBurst=10
ExecStart={{.Path|cmdEscape}}{{range .Arguments}} {{.|cmd}}{{end}}
{{if .ChRoot}}RootDirectory={{.ChRoot|cmd}}{{end}}
{{if .WorkingDirectory}}WorkingDirectory={{.WorkingDirectory|cmdEscape}}{{end}}
{{if .UserName}}User={{.UserName}}{{end}}
{{if .ReloadSignal}}ExecReload=/bin/kill -{{.ReloadSignal}} "$MAINPID"{{end}}
{{if .PIDFile}}PIDFile={{.PIDFile|cmd}}{{end}}
{{if and .LogOutput .HasOutputFileSupport -}}
StandardOutput=file:{{.LogDirectory}}/{{.Name}}.out
StandardError=file:{{.LogDirectory}}/{{.Name}}.err
{{- end}}
{{if gt .LimitNOFILE -1 }}LimitNOFILE={{.LimitNOFILE}}{{end}}
{{if .Restart}}Restart={{.Restart}}{{end}}
{{if .SuccessExitStatus}}SuccessExitStatus={{.SuccessExitStatus}}{{end}}
RestartSec=5
EnvironmentFile=-/etc/sysconfig/{{.Name}}

{{range $k, $v := .EnvVars -}}
Environment={{$k}}={{$v}}
{{end -}}

[Install]
WantedBy=multi-user.target
`

// KV is a single --env K=V pair.
type KV struct {
	Key   string
	Value string
}

// BuildConfig assembles the kardianos service.Config for name/args/env under
// the resolved scope and goos. Quoting inside argsStr is not supported; it is
// split on whitespace.
func BuildConfig(name, argsStr string, env []KV, scope Scope, goos string) (*service.Config, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}

	envVars := map[string]string{"PATH": os.Getenv("PATH")}
	for _, kv := range env {
		envVars[kv.Key] = kv.Value
	}
	envVars["TETHER_SERVICE_NAME"] = name

	options := service.KeyValue{}
	if scope == ScopeUser {
		options["UserService"] = true
	}
	switch goos {
	case "linux":
		options["Restart"] = "always"
		options["SystemdScript"] = linuxSystemdScript
	case "windows":
		options["OnFailure"] = "restart"
		options["OnFailureDelayDuration"] = "5s"
		options["OnFailureResetPeriod"] = 60
	}

	var args []string
	if argsStr != "" {
		args = strings.Fields(argsStr)
	}

	return &service.Config{
		Name:       name,
		Arguments:  args,
		Executable: exe,
		EnvVars:    envVars,
		Option:     options,
	}, nil
}

// ControlConfig builds the minimal kardianos service.Config needed to locate
// an already-installed service for start/stop/uninstall/status. It must carry
// the same scope Option as the config used at install time, or kardianos
// resolves the wrong domain (system instead of user) on macOS/Linux.
func ControlConfig(name string, scope Scope) *service.Config {
	options := service.KeyValue{}
	if scope == ScopeUser {
		options["UserService"] = true
	}
	return &service.Config{
		Name:   name,
		Option: options,
	}
}
