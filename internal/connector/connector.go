package connector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dtunikov/go-db-stream/internal/config"
	"github.com/dtunikov/go-db-stream/internal/datasource"
	"github.com/dtunikov/go-db-stream/internal/datasource/kafka"
	"github.com/dtunikov/go-db-stream/internal/datasource/postgres"
	"go.uber.org/zap"
)

// Connector connects two datasources and moves data from one to another
type Connector struct {
	Id     string
	from   datasource.ReadableDatasource
	to     datasource.WritableDatasource
	cfg    config.Connector
	logger *zap.Logger
	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

func NewConnector(ctx context.Context, fromDsCfg, toDsCfg config.Datasource, cfg config.Connector, logger *zap.Logger) (*Connector, error) {
	// Build ReadConfig for the source datasource
	readConfig := datasource.ReadConfig{
		AllCollections: cfg.AllCollections,
		Mapping:        make([]datasource.ReadConfigMapping, len(cfg.Mapping)),
	}
	for i, m := range cfg.Mapping {
		readConfig.Mapping[i] = datasource.ReadConfigMapping{
			Collection: m.From,
			AllFields:  m.AllFields,
			Filter:     m.Filter,
		}
	}

	// Create source datasource with ReadConfig
	var from datasource.Datasource
	var err error
	fromLogger := logger.With(zap.String("datasource", fromDsCfg.Id), zap.String("role", "source"))

	if fromDsCfg.Postgres != nil {
		from, err = postgres.NewPostgresDatasource(ctx, *fromDsCfg.Postgres, fromLogger, &readConfig)
		if err != nil {
			return nil, fmt.Errorf("could not create postgres source datasource: %w", err)
		}
	} else {
		return nil, fmt.Errorf("unsupported source datasource type")
	}

	fromReadable, ok := from.(datasource.ReadableDatasource)
	if !ok {
		return nil, fmt.Errorf("source datasource is not readable")
	}

	// Create destination datasource
	var to datasource.Datasource
	toLogger := logger.With(zap.String("datasource", toDsCfg.Id), zap.String("role", "destination"))

	if toDsCfg.Kafka != nil {
		to, err = kafka.NewKafkaDatasource(*toDsCfg.Kafka, toLogger)
		if err != nil {
			return nil, fmt.Errorf("could not create kafka destination datasource: %w", err)
		}
	} else {
		return nil, fmt.Errorf("unsupported destination datasource type")
	}

	toWritable, ok := to.(datasource.WritableDatasource)
	if !ok {
		return nil, fmt.Errorf("destination datasource is not writable")
	}

	connCtx, cancel := context.WithCancel(ctx)

	return &Connector{
		Id:     cfg.Id,
		from:   fromReadable,
		to:     toWritable,
		cfg:    cfg,
		done:   make(chan struct{}),
		ctx:    connCtx,
		cancel: cancel,
		logger: logger.With(zap.String("connector", cfg.Id)),
	}, nil
}

func (c *Connector) Run(wg *sync.WaitGroup) error {
	go func() {
		defer wg.Done()
		c.logger.Info("starting connector with pull-based batching",
			zap.Int("batch_size", c.cfg.BatchSize),
			zap.Duration("batch_timeout", c.cfg.BatchTimeout))

		batch := make([]datasource.Message, 0, c.cfg.BatchSize)
		var lastPosition any
		lastBatchTime := time.Now()

		for {
			// Check if we should flush the batch
			shouldFlush := len(batch) >= c.cfg.BatchSize ||
				(len(batch) > 0 && time.Since(lastBatchTime) >= c.cfg.BatchTimeout)

			if shouldFlush {
				if err := c.flushBatch(batch, lastPosition); err != nil {
					c.logger.Error("failed to flush batch", zap.Error(err), zap.Int("batch_size", len(batch)))
					// Continue processing - we'll retry on next flush
				} else {
					c.logger.Debug("flushed batch", zap.Int("batch_size", len(batch)))
					batch = batch[:0]
					lastBatchTime = time.Now()
				}
			}

			// Read next message with a timeout to allow periodic batch flushes
			readCtx, cancel := context.WithTimeout(c.ctx, c.cfg.BatchTimeout)
			msgWithPos, err := c.from.Next(readCtx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded || err == context.Canceled {
					// Timeout or cancellation - check if we should stop
					select {
					case <-c.ctx.Done():
						// Flush any remaining messages before stopping
						if len(batch) > 0 {
							if err := c.flushBatch(batch, lastPosition); err != nil {
								c.logger.Error("failed to flush final batch", zap.Error(err))
							}
						}
						c.logger.Info("connector stopped")
						return
					default:
						// Just a timeout, continue to check if we should flush
						continue
					}
				}
				c.logger.Error("failed to read message", zap.Error(err))
				continue
			}

			// Add message to batch
			batch = append(batch, msgWithPos.Message)
			lastPosition = msgWithPos.Position
		}
	}()

	return nil
}

func (c *Connector) flushBatch(batch []datasource.Message, position any) error {
	if len(batch) == 0 {
		return nil
	}

	// Write all messages in a single batch operation
	if err := c.to.WriteBatch(c.ctx, batch); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}

	// Commit the position after all messages are written
	if err := c.from.Commit(c.ctx, position); err != nil {
		return fmt.Errorf("commit position: %w", err)
	}

	return nil
}

func (c *Connector) Stop() {
	c.cancel()
}

func (c *Connector) Close(ctx context.Context) error {
	// Close both datasources
	if err := c.from.Close(ctx); err != nil {
		c.logger.Error("failed to close source datasource", zap.Error(err))
	}
	if err := c.to.Close(ctx); err != nil {
		c.logger.Error("failed to close destination datasource", zap.Error(err))
	}
	return nil
}
