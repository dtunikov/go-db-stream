package executor

import (
	"context"
	"fmt"
	"sync"

	"github.com/dtunikov/go-db-stream/internal/config"
	"github.com/dtunikov/go-db-stream/internal/connector"
	"go.uber.org/zap"
)

type Executor struct {
	cfg          config.Executor
	connectors   []*connector.Connector
	connectorsWg sync.WaitGroup
	externalWg   *sync.WaitGroup
	logger       *zap.Logger
}

func NewExecutor(cfg config.Executor, wg *sync.WaitGroup, logger *zap.Logger) (*Executor, error) {
	executor := &Executor{
		cfg:        cfg,
		externalWg: wg,
		logger:     logger,
	}

	// Create a map of datasources by ID for lookup
	datasourceMap := make(map[string]config.Datasource)
	for _, ds := range cfg.Datasources {
		datasourceMap[ds.Id] = ds
	}

	// Init connectors - each connector creates its own datasource instances
	for _, connCfg := range cfg.Connectors {
		fromDs, ok := datasourceMap[connCfg.From]
		if !ok {
			return nil, fmt.Errorf("source datasource %q not found for connector %q", connCfg.From, connCfg.Id)
		}

		toDs, ok := datasourceMap[connCfg.To]
		if !ok {
			return nil, fmt.Errorf("destination datasource %q not found for connector %q", connCfg.To, connCfg.Id)
		}

		conn, err := connector.NewConnector(context.Background(), fromDs, toDs, connCfg, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create connector %q: %w", connCfg.Id, err)
		}
		executor.connectors = append(executor.connectors, conn)
	}

	return executor, nil
}

func (e *Executor) Run() error {
	e.connectorsWg.Add(len(e.connectors))
	for _, c := range e.connectors {
		err := c.Run(&e.connectorsWg)
		if err != nil {
			return fmt.Errorf("failed to run connector %q: %w", c.Id, err)
		}
	}

	return nil
}

func (e *Executor) Stop(ctx context.Context) {
	e.logger.Info("stopping executor")

	// Stop all connectors
	for _, c := range e.connectors {
		c.Stop()
	}

	// Wait for all connectors to stop
	e.connectorsWg.Wait()

	// Close all connector datasources
	for _, c := range e.connectors {
		if err := c.Close(ctx); err != nil {
			e.logger.Error("failed to close connector", zap.String("connector_id", c.Id), zap.Error(err))
		}
	}

	e.logger.Info("executor stopped")
	e.externalWg.Done()
}
