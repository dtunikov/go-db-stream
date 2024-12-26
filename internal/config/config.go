package config

import (
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"
	"github.com/goccy/go-yaml"
)

type PostgresDatasource struct {
	Url string `yaml:"url"`
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
	Level       string `yaml:"level" envDefault:"info" env:"LOG_LEVEL"`
	Format      string `yaml:"format" envDefault:"json" env:"LOG_FORMAT"`
	PrintCaller bool   `yaml:"printCaller" envDefault:"false" env:"LOG_PRINT_CALLER"`
}

type Datasource struct {
	Id       string              `yaml:"name"`
	Postgres *PostgresDatasource `yaml:"postgres"`
	Kafka    *KafkaDatasource    `yaml:"kafka"`
}

type Executor struct {
	Datasources []Datasource `yaml:"datasources"`
	Connectors  []Connector  `yaml:"connectors"`
}

type Server struct {
	Port int `yaml:"port" envDefault:"8080"`
}

type Config struct {
	Log      Log      `yaml:"log"`
	Executor Executor `yaml:"executor"`
	Server   Server   `yaml:"server"`
}

func LoadConfig(configFilePath string) (*Config, error) {
	cfg := Config{}

	// Try to load the environment variables into the config
	err := env.Parse(&cfg)
	if err != nil {
		return nil, err
	}

	// only if the config file path is provided we will load the config from the file
	if configFilePath != "" {
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
		// TODO: Implement JSON schema validation
		// err = ValidateConfig(configFileBytes, "config.schema.json")
		// if err != nil {
		// 	return nil, fmt.Errorf("router config validation error: %w", err)
		// }

		// Unmarshal the final config
		if err := yaml.Unmarshal(configFileBytes, &cfg); err != nil {
			return nil, err
		}
	}

	return &cfg, nil
}
