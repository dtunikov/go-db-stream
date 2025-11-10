package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/dtunikov/go-db-stream/internal/config"
	"github.com/dtunikov/go-db-stream/internal/datasource"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

type KafkaDatasource struct {
	writer *kafka.Writer
	cfg    config.KafkaDatasource
	logger *zap.Logger
}

func NewKafkaDatasource(cfg config.KafkaDatasource, logger *zap.Logger) (*KafkaDatasource, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers provided")
	}

	dialer := &kafka.Dialer{}

	if cfg.SASL != nil {
		saslMechanism, err := saslMechanismFromConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		dialer.SASLMechanism = saslMechanism
	}

	if cfg.TLS != nil {
		tlsConfig, err := LoadTLSConfig(cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS config: %w", err)
		}
		dialer.TLS = tlsConfig
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Dialer:       dialer,
		Brokers:      cfg.Brokers,
		MaxAttempts:  cfg.MaxAttempts,
		BatchSize:    cfg.BatchSize,
		BatchBytes:   cfg.BatchBytes,
		BatchTimeout: cfg.BatchTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: cfg.RequiredAcks,
		Async:        cfg.Async,
	})

	return &KafkaDatasource{w, cfg, logger.With(zap.String("datasource-type", "kafka"))}, nil
}

func LoadTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate and key: %w", err)
	}

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA file: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("failed to append CA certificate")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}

func saslMechanismFromConfig(cfg config.KafkaDatasource) (sasl.Mechanism, error) {
	var err error
	var mechanism sasl.Mechanism
	switch cfg.SASL.Mechanism {
	case "plain":
		mechanism = plain.Mechanism{
			Username: cfg.SASL.Username,
			Password: cfg.SASL.Password,
		}
	case "scram-sha-256":
		mechanism, err = scram.Mechanism(scram.SHA512, cfg.SASL.Username, cfg.SASL.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM mechanism: %w", err)
		}
	case "scram-sha-512":
		mechanism, err = scram.Mechanism(scram.SHA512, cfg.SASL.Username, cfg.SASL.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM mechanism: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.SASL.Mechanism)
	}

	return mechanism, nil
}

func (k *KafkaDatasource) HealthCheck(ctx context.Context) error {
	for _, broker := range k.cfg.Brokers {
		k.logger.Info("checking kafka broker", zap.String("broker", broker))
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err != nil {
			return fmt.Errorf("failed to dial Kafka: %w", err)
		}
		defer conn.Close()

		_, err = conn.Brokers()
		if err != nil {
			return fmt.Errorf("failed to fetch Kafka metadata: %w", err)
		}
	}

	return nil
}

func (k *KafkaDatasource) Write(ctx context.Context, msg datasource.Message) error {
	err := k.writer.WriteMessages(ctx, kafka.Message{
		Headers: []kafka.Header{
			{
				Key:   "operation",
				Value: []byte(msg.Op),
			},
		},
		Value: msg.Data,
		Topic: msg.Collection,
	})
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	k.logger.Debug("message written successfully")
	return nil
}

func (k *KafkaDatasource) Close(ctx context.Context) error {
	err := k.writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close kafka writer: %w", err)
	}
	return nil
}
