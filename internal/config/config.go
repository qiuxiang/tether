package config

import (
	"errors"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Hub struct {
	Listen string `yaml:"listen"`
	Token  string `yaml:"token"`
}

type Node struct {
	HubURL           string `yaml:"hub_url"`
	Token            string `yaml:"token"`
	HostnameOverride string `yaml:"hostname_override"`
	LogDir           string `yaml:"log_dir"`
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
	var c Node
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.HubURL == "" {
		return nil, errors.New("config: hub_url is required")
	}
	if c.Token == "" {
		return nil, errors.New("config: token is required")
	}
	if c.LogDir == "" {
		home, _ := os.UserHomeDir()
		c.LogDir = filepath.Join(home, ".local", "share", "tether", "logs")
	}
	return &c, nil
}
