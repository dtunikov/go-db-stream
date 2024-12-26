package executor

import (
	"fmt"
	"sync"

	"github.com/dtunikov/go-kafka-connector/internal/config"
	"github.com/dtunikov/go-kafka-connector/internal/connector"
	"github.com/dtunikov/go-kafka-connector/internal/services/datasources"
	"go.uber.org/zap"
)

type Executor struct {
	cfg          config.Executor
	connectors   []connector.Connector
	connectorsWg sync.WaitGroup
}

func NewExecutor(cfg config.Executor, logger *zap.Logger) (*Executor, error) {
	executor := &Executor{
		cfg: cfg,
	}
	// init datasources service
	datasourcesSvc, err := datasources.NewService(cfg.Datasources, logger)
	if err != nil {
		return nil, err
	}

	// init connectors
	for _, c := range cfg.Connectors {
		connector, err := connector.NewConnector(c, datasourcesSvc, logger)
		if err != nil {
			return nil, err
		}
		executor.connectors = append(executor.connectors, *connector)
	}

	return executor, nil
}

func (e *Executor) Run(wg *sync.WaitGroup) error {
	defer wg.Done()

	e.connectorsWg.Add(len(e.connectors))
	for _, c := range e.connectors {
		err := c.Run(&e.connectorsWg)
		if err != nil {
			return fmt.Errorf("could not run connector %s: %w", c.Id, err)
		}
	}

	return nil
}

func (e *Executor) Stop() {
	for _, c := range e.connectors {
		c.Stop()
	}
	// wait for all connectors to stop
	e.connectorsWg.Wait()
}
