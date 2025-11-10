package connector

import (
	"context"
	"fmt"
	"sync"

	"github.com/dtunikov/go-db-stream/internal/config"
	"github.com/dtunikov/go-db-stream/internal/datasource"
	"github.com/dtunikov/go-db-stream/internal/services/datasources"
	"github.com/google/uuid"
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
}

func NewConnector(cfg config.Connector, dsService *datasources.Service, logger *zap.Logger) (*Connector, error) {
	from, err := dsService.DatasourceById(cfg.From)
	if err != nil {
		return nil, fmt.Errorf("could not find source datasource: %w", err)
	}
	to, err := dsService.DatasourceById(cfg.To)
	if err != nil {
		return nil, fmt.Errorf("could not find destination datasource: %w", err)
	}

	fromReadable, ok := from.(datasource.ReadableDatasource)
	if !ok {
		return nil, fmt.Errorf("source datasource is not readable")
	}

	toWritable, ok := to.(datasource.WritableDatasource)
	if !ok {
		return nil, fmt.Errorf("destination datasource is not writable")
	}

	return &Connector{
		Id:     cfg.Id,
		from:   fromReadable,
		to:     toWritable,
		cfg:    cfg,
		done:   make(chan struct{}),
		logger: logger.With(zap.String("connector", cfg.Id), zap.String("from", cfg.From), zap.String("to", cfg.To)),
	}, nil
}

func (c *Connector) Run(wg *sync.WaitGroup) error {
	defer wg.Done()

	readConfig := datasource.ReadConfig{
		AllCollections: c.cfg.AllCollections,
		Mapping:        make([]datasource.ReadConfigMapping, len(c.cfg.Mapping)),
	}
	for i, m := range c.cfg.Mapping {
		readConfig.Mapping[i] = datasource.ReadConfigMapping{
			Collection: m.From,
			AllFields:  m.AllFields,
			Filter:     m.Filter,
		}
	}

	ctx := context.Background()
	sub := &datasource.Subscription{
		ID:         uuid.New(),
		Ch:         make(chan datasource.Message),
		ReadConfig: readConfig,
	}
	err := c.from.Read(ctx, sub)
	if err != nil {
		return fmt.Errorf("could not read from source datasource: %w", err)
	}

	c.logger.Info("starting connector")
Outer:
	for {
		select {
		case <-c.done:
			break Outer
		case msg := <-sub.Ch:
			// TODO: we could actually filter the message here instead of filtering it inside readable datasource code, BUT that would mean that we would have to read the message and unmarshal it to check if it should be filtered
			// sometimes it is better to filter the message as soon as possible (so on)
			// TODO: take timeout from config
			// ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.to.Write(context.Background(), msg)
			// cancel()
			if err != nil {
				c.logger.Error("could not write message", zap.Error(err))
			}
		}
	}

	c.logger.Info("connector stopped")
	return nil
}

func (c *Connector) Stop() {
	close(c.done)
}
