package client

import (
	"errors"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	HubURL string `yaml:"hub_url"`
	Token  string `yaml:"token"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.HubURL == "" {
		return nil, errors.New("config: hub_url is required")
	}
	if c.Token == "" {
		return nil, errors.New("config: token is required")
	}
	return &c, nil
}
