package executor

import (
	"context"
	"sync"

	"github.com/dtunikov/go-db-stream/internal/config"
	"github.com/dtunikov/go-db-stream/internal/connector"
	"github.com/dtunikov/go-db-stream/internal/services/datasources"
	"go.uber.org/zap"
)

type Executor struct {
	cfg            config.Executor
	datasourcesSvc *datasources.Service
	connectors     []connector.Connector
	connectorsWg   sync.WaitGroup
	externalWg     *sync.WaitGroup
}

func NewExecutor(cfg config.Executor, wg *sync.WaitGroup, logger *zap.Logger) (*Executor, error) {
	var err error
	executor := &Executor{
		cfg:        cfg,
		externalWg: wg,
	}
	// init datasources service
	executor.datasourcesSvc, err = datasources.NewService(cfg.Datasources, logger)
	if err != nil {
		return nil, err
	}

	// init connectors
	for _, c := range cfg.Connectors {
		connector, err := connector.NewConnector(c, executor.datasourcesSvc, logger)
		if err != nil {
			return nil, err
		}
		executor.connectors = append(executor.connectors, *connector)
	}

	return executor, nil
}

func (e *Executor) Run() error {
	e.connectorsWg.Add(len(e.connectors))
	for _, c := range e.connectors {
		go c.Run(&e.connectorsWg)
	}

	return nil
}

func (e *Executor) Stop(ctx context.Context) {
	e.datasourcesSvc.Close(ctx)
	for _, c := range e.connectors {
		c.Stop()
	}
	// wait for all connectors to stop
	e.connectorsWg.Wait()
	e.externalWg.Done()
}
