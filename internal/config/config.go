package config

import (
	"fmt"
	"os"

	_ "embed"

	"github.com/caarlos0/env/v11"
	"github.com/goccy/go-yaml"
)

//go:embed config.schema.json
var configSchema []byte

type PostgresDatasource struct {
	Url                string   `yaml:"url"`
	Publications       []string `yaml:"publications"`
	CreatePublications []string `yaml:"createPublications"`
}

type KafkaDatasource struct {
	Brokers []string `yaml:"brokers"`
}

type ConnectorMappingField struct {
	Name string `yaml:"name"`
}

type ConnectorMapping struct {
	From      string `yaml:"from"`
	To        string `yaml:"to"`
	Filter    string `yaml:"filter"`
	AllFields bool   `yaml:"allFields" envDefault:"true"`
}

type Connector struct {
	Id             string             `yaml:"id"`
	From           string             `yaml:"from"`
	To             string             `yaml:"to"`
	AllCollections bool               `yaml:"allCollections" envDefault:"true"`
	Mapping        []ConnectorMapping `yaml:"mapping"`
}

type Log struct {
	Level string `yaml:"level" envDefault:"info" env:"LOG_LEVEL"`
}

type Datasource struct {
	Id       string              `yaml:"id"`
	Postgres *PostgresDatasource `yaml:"postgres"`
	Kafka    *KafkaDatasource    `yaml:"kafka"`
}

type Executor struct {
	Datasources []Datasource `yaml:"datasources"`
	Connectors  []Connector  `yaml:"connectors"`
}

type Server struct {
	Port int `yaml:"port"`
}

type Config struct {
	Log      Log      `yaml:"log"`
	Executor Executor `yaml:"executor"`
	Server   Server   `yaml:"server"`
}

func LoadConfig(configFilePath string) (*Config, error) {
	cfg := Config{}

	// evaluate environment variables and default values
	err := env.Parse(&cfg)
	if err != nil {
		return nil, err
	}

	configFileBytes, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("could not read custom config file %s: %w", configFilePath, err)
	}

	configYamlData := os.ExpandEnv(string(configFileBytes))
	if err := yaml.Unmarshal([]byte(configYamlData), &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal router config: %w", err)
	}

	// Validate the config against the JSON schema
	configFileBytes = []byte(configYamlData)
	err = ValidateConfig(configFileBytes, configSchema)
	if err != nil {
		return nil, fmt.Errorf("router config validation error: %w", err)
	}

	return &cfg, nil
}
