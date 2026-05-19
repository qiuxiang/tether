package config

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/qiuxiang/tether/internal/forward"
	"gopkg.in/yaml.v3"
)

type Hub struct {
	Listen string `yaml:"listen"`
	Token  string `yaml:"token"`
}

type Node struct {
	HubURL           string         `yaml:"hub_url"`
	Token            string         `yaml:"token"`
	HostnameOverride string         `yaml:"hostname_override"`
	LogDir           string         `yaml:"log_dir"`
	Forwards         []forward.Rule `yaml:"-"`
}

type rawNode struct {
	HubURL           string   `yaml:"hub_url"`
	Token            string   `yaml:"token"`
	HostnameOverride string   `yaml:"hostname_override"`
	LogDir           string   `yaml:"log_dir"`
	Forwards         []string `yaml:"forwards"`
}

func LoadHub(path string) (*Hub, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Hub
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.Listen == "" {
		c.Listen = ":7000"
	}
	if c.Token == "" {
		return nil, errors.New("config: token is required")
	}
	return &c, nil
}

func LoadNode(path string) (*Node, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw rawNode
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if raw.HubURL == "" {
		return nil, errors.New("config: hub_url is required")
	}
	if raw.Token == "" {
		return nil, errors.New("config: token is required")
	}
	rules, err := forward.ParseAll(raw.Forwards)
	if err != nil {
		return nil, err
	}
	logDir := raw.LogDir
	if logDir == "" {
		home, _ := os.UserHomeDir()
		logDir = filepath.Join(home, ".local", "share", "tether", "logs")
	}
	return &Node{
		HubURL:           raw.HubURL,
		Token:            raw.Token,
		HostnameOverride: raw.HostnameOverride,
		LogDir:           logDir,
		Forwards:         rules,
	}, nil
}
