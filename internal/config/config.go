package config

import (
	"fmt"
	"os"
	"time"

	_ "embed"

	"github.com/caarlos0/env/v11"
	"github.com/goccy/go-yaml"
)

type PostgresDatasource struct {
	Url                string        `yaml:"url"`
	ReplicationSlot    string        `yaml:"replicationSlot"`
	Publications       []string      `yaml:"publications"`
	CreatePublications []string      `yaml:"createPublications"`
	StandByTimeout     time.Duration `yaml:"standByTimeout" envDefault:"10s"`
}

type KafkaDatasource struct {
	// Brokers is a list of kafka brokers to connect to.
	Brokers []string `yaml:"brokers"`

	// TLS configuration for the kafka connection.
	TLS *struct {
		CertFile string `yaml:"certFile"`
		KeyFile  string `yaml:"keyFile"`
		CAFile   string `yaml:"caFile"`
	}

	// SASL auth configuration for the kafka connection.
	SASL *struct {
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
		Mechanism string `yaml:"mechanism" envDefault:"plain"`
	} `yaml:"sasl"`

	// Limit on how many attempts will be made to deliver a message.
	//
	// The default is to try at most 10 times.
	MaxAttempts int `yaml:"maxAttempts" envDefault:"10"`

	// Limit on how many messages will be buffered before being sent to a
	// partition.
	//
	// The default is to use a target batch size of 100 messages.
	BatchSize int `yaml:"batchSize" envDefault:"100"`

	// Limit the maximum size of a request in bytes before being sent to
	// a partition.
	//
	// The default is to use a kafka default value of 1048576.
	BatchBytes int `yaml:"batchBytes" envDefault:"1048576"`

	// Time limit on how often incomplete message batches will be flushed to
	// kafka.
	//
	// The default is to flush at least every second.
	BatchTimeout time.Duration `yaml:"batchTimeout" envDefault:"1s"`

	// Timeout for read operations performed by the Writer.
	//
	// Defaults to 10 seconds.
	ReadTimeout time.Duration `yaml:"readTimeout" envDefault:"10s"`

	// Timeout for write operation performed by the Writer.
	//
	// Defaults to 10 seconds.
	WriteTimeout time.Duration `yaml:"writeTimeout" envDefault:"10s"`

	// Number of acknowledges from partition replicas required before receiving
	// a response to a produce request. The default is -1, which means to wait for
	// all replicas, and a value above 0 is required to indicate how many replicas
	// should acknowledge a message to be considered successful.
	RequiredAcks int `yaml:"requiredAcks" envDefault:"-1"`

	// Setting this flag to true causes the WriteMessages method to never block.
	// It also means that errors are ignored since the caller will not receive
	// the returned value. Use this only if you don't care about guarantees of
	// whether the messages were written to kafka.
	Async bool `yaml:"async" envDefault:"false"`
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

	configFileBytes, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("could not read custom config file %s: %w", configFilePath, err)
	}

	configYamlData := os.ExpandEnv(string(configFileBytes))
	if err := yaml.Unmarshal([]byte(configYamlData), &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal router config: %w", err)
	}

	// evaluate environment variables and default values
	err = env.Parse(&cfg)
	if err != nil {
		return nil, err
	}

	// Validate the config against the JSON schema
	configFileBytes = []byte(configYamlData)
	err = ValidateConfig(configFileBytes)
	if err != nil {
		return nil, fmt.Errorf("router config validation error: %w", err)
	}

	return &cfg, nil
}
