package datasource

import (
	"context"

	"github.com/google/uuid"
)

type Datasource interface {
	// Check if the datasource is healthy
	HealthCheck(ctx context.Context) error
	Close(ctx context.Context) error
}

type ReadConfigMapping struct {
	// name of the collection to override
	Collection string
	// if all the fields should be read
	AllFields bool
	// filter is SQL like WHERE clause, e.g. "id = 1 and name = 'John' or age > 18"
	Filter string
	// field specific configuration
	Fields []ReadConfigFieldMapping
}

type ReadConfigFieldMapping struct {
	// name of the field to override
	Field string
	// TODO:  tranformation function
	// Transform func([]byte) []byte
}

type ReadConfig struct {
	// if all the collections should be read
	AllCollections bool
	// collection specific configuration
	Mapping []ReadConfigMapping
}

func NewReadConfigMapping() ReadConfigMapping {
	return ReadConfigMapping{
		AllFields: true,
	}
}

func NewReadConfig() ReadConfig {
	return ReadConfig{
		AllCollections: true,
		Mapping:        []ReadConfigMapping{},
	}
}

type ReadableDatasource interface {
	Datasource
	// Next returns the next message from the datasource, blocking until one is available
	Next(ctx context.Context) (*MessageWithPosition, error)
	// Commit acknowledges that messages up to the given position have been successfully processed
	Commit(ctx context.Context, position any) error
}

type WritableDatasource interface {
	Datasource
	Write(ctx context.Context, msg Message) error
	// WriteBatch writes multiple messages in a single operation for better performance
	WriteBatch(ctx context.Context, msgs []Message) error
}

type Operation string

const (
	Insert Operation = "insert"
	Update Operation = "update"
	Delete Operation = "delete"
)

type Message struct {
	ID         uuid.UUID
	Op         Operation
	Collection string
	Data       []byte
}

type MessageWithPosition struct {
	Message
	// Position is datasource-specific (e.g., LSN for Postgres, offset for Kafka)
	Position any
}
