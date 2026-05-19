package client

import (
	"errors"
	"os"

	"github.com/qiuxiang/tether/internal/forward"
	"gopkg.in/yaml.v3"
)

type Config struct {
	HubURL   string         `yaml:"hub_url"`
	Token    string         `yaml:"token"`
	Forwards []forward.Rule `yaml:"-"`
}

type rawConfig struct {
	HubURL   string   `yaml:"hub_url"`
	Token    string   `yaml:"token"`
	Forwards []string `yaml:"forwards"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw rawConfig
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
	return &Config{HubURL: raw.HubURL, Token: raw.Token, Forwards: rules}, nil
}
