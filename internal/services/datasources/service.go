package datasources

import (
	"context"
	"fmt"

	"github.com/dtunikov/go-db-stream/internal/config"
	"github.com/dtunikov/go-db-stream/internal/datasource"
	"github.com/dtunikov/go-db-stream/internal/datasource/kafka"
	"github.com/dtunikov/go-db-stream/internal/datasource/postgres"
	"go.uber.org/zap"
)

type Service struct {
	datasources map[string]datasource.Datasource
	logger      *zap.Logger
}

func NewService(datasourcesConfig []config.Datasource, logger *zap.Logger) (*Service, error) {
	var err error
	datasources := make(map[string]datasource.Datasource)
	for _, ds := range datasourcesConfig {
		dsLogger := logger.With(zap.String("datasource", ds.Id))
		var created datasource.Datasource
		if ds.Kafka != nil {
			created, err = kafka.NewKafkaDatasource(*ds.Kafka, dsLogger)
			if err != nil {
				return nil, fmt.Errorf("could not create kafka datasource: %w", err)
			}
		} else if ds.Postgres != nil {
			created, err = postgres.NewPostgresDatasource(context.Background(), *ds.Postgres, dsLogger)
			if err != nil {
				return nil, fmt.Errorf("could not create postgres datasource: %w", err)
			}
		} else {
			return nil, fmt.Errorf("could not create datasource, unknown type")
		}

		datasources[ds.Id] = created
	}

	return &Service{
		datasources: datasources,
		logger:      logger,
	}, nil
}

func (s *Service) DatasourceById(id string) (datasource.Datasource, error) {
	ds, ok := s.datasources[id]
	if !ok {
		return nil, fmt.Errorf("datasource not found")
	}
	return ds, nil
}

func (s *Service) Close(ctx context.Context) {
	for _, ds := range s.datasources {
		err := ds.Close(ctx)
		if err != nil {
			s.logger.Error("failed to close datasource", zap.Error(err))
		}
	}
}
