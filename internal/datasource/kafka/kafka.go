package kafka

import (
	"context"
	"fmt"

	"github.com/dtunikov/go-kafka-connector/internal/config"
	"github.com/dtunikov/go-kafka-connector/internal/datasource"
	"github.com/segmentio/kafka-go"
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

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: cfg.Brokers,
	})

	return &KafkaDatasource{w, cfg, logger.With(zap.String("datasource-type", "kafka"))}, nil
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
	k.logger.Info("writing message", zap.String("collection", msg.Collection), zap.ByteString("data", msg.Data))
	err := k.writer.WriteMessages(ctx, kafka.Message{
		Value: msg.Data,
		Topic: msg.Collection,
	})
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}
