package config

import (
	"errors"
	"os"

	"github.com/qiuxiang/tether/internal/forward"
	"gopkg.in/yaml.v3"
)

// Config is the unified configuration for every tether role. A single file
// can drive the hub, a node, the MCP client, or any combination — each
// subcommand reads the fields it needs and ignores the rest.
type Config struct {
	Token            string         // every role
	Listen           string         // hub
	HubURL           string         // node + client
	HostnameOverride string         // node
	Forwards         []forward.Rule // node
}

// raw mirrors the YAML file. forwards is decoded as []string and parsed into
// []forward.Rule, so a separate decode struct is needed.
type raw struct {
	Token            string   `yaml:"token"`
	Listen           string   `yaml:"listen"`
	HubURL           string   `yaml:"hub_url"`
	HostnameOverride string   `yaml:"hostname_override"`
	Forwards         []string `yaml:"forwards"`
}

// Load reads and parses a unified config file. It validates only token (the
// one field every role requires); role-specific requirements such as hub_url
// are checked by the subcommand that needs them.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r raw
	if err := yaml.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	if r.Token == "" {
		return nil, errors.New("config: token is required")
	}
	if r.Listen == "" {
		r.Listen = ":7000"
	}
	rules, err := forward.ParseAll(r.Forwards)
	if err != nil {
		return nil, err
	}
	return &Config{
		Token:            r.Token,
		Listen:           r.Listen,
		HubURL:           r.HubURL,
		HostnameOverride: r.HostnameOverride,
		Forwards:         rules,
	}, nil
}
