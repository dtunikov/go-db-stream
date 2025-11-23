package postgres

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/dtunikov/go-db-stream/internal/config"
	"github.com/dtunikov/go-db-stream/internal/datasource"
	"github.com/dtunikov/go-db-stream/pkg/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

type PostgresDatasource struct {
	conn            *pgx.Conn
	replicationConn *pgconn.PgConn
	logger          *zap.Logger
	sysident        pglogrepl.IdentifySystemResult
	publications    []string
	cfg             config.PostgresDatasource

	// Pull-based reading state
	slotName              string
	clientXLogPos         pglogrepl.LSN
	lastConfirmedPosition pglogrepl.LSN
	relationsV2           map[uint32]*pglogrepl.RelationMessageV2
	inStream              bool
	readConfig            datasource.ReadConfig
	replicationStarted    bool
}

func NewPostgresDatasource(ctx context.Context, cfg config.PostgresDatasource, logger *zap.Logger, readConfig *datasource.ReadConfig) (*PostgresDatasource, error) {
	replicationUrl, err := url.Parse(cfg.Url)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}

	queryParams := replicationUrl.Query()
	queryParams.Set("replication", "database")
	replicationUrl.RawQuery = queryParams.Encode()
	replicationConn, err := pgconn.Connect(ctx, replicationUrl.String())
	if err != nil {
		return nil, fmt.Errorf("connect to replication url: %w", err)
	}

	pgConnection, err := pgx.Connect(ctx, cfg.Url)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	publications := make([]string, 0, len(cfg.CreatePublications))
	for _, createPublication := range cfg.CreatePublications {
		name, err := postgres.ExtractPulicationNameFromQuery(createPublication)
		if err != nil {
			return nil, fmt.Errorf("extract publication name from query: %w", err)
		}
		publications = append(publications, name)

		exists, err := postgres.PublicationExists(ctx, pgConnection, name)
		if err != nil {
			return nil, fmt.Errorf("check if publication exists: %w", err)
		}
		if exists {
			logger.Info("publication already exists", zap.String("name", name))
			continue
		}

		_, err = pgConnection.Exec(ctx, createPublication)
		if err != nil {
			return nil, fmt.Errorf("create publication: %w", err)
		}
	}
	for _, name := range cfg.Publications {
		exists, err := postgres.PublicationExists(ctx, pgConnection, name)
		if err != nil {
			return nil, fmt.Errorf("check if publication exists: %w", err)
		}
		if !exists {
			return nil, fmt.Errorf("publication %s does not exist", name)
		}
		publications = append(publications, name)
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, replicationConn)
	if err != nil {
		return nil, fmt.Errorf("identify system: %w", err)
	}
	logger.Info("created postgres datasource",
		zap.String("system_id", sysident.SystemID),
		zap.Int32("timeline", sysident.Timeline),
		zap.String("xlog_pos", sysident.XLogPos.String()),
		zap.String("db_name", sysident.DBName),
		zap.Strings("publications", publications),
	)

	// Default to reading all collections if no config provided
	finalReadConfig := datasource.NewReadConfig()
	if readConfig != nil {
		finalReadConfig = *readConfig
	}

	return &PostgresDatasource{
		conn:                  pgConnection,
		replicationConn:       replicationConn,
		logger:                logger.With(zap.String("datasource-type", "postgres")),
		sysident:              sysident,
		publications:          publications,
		cfg:                   cfg,
		relationsV2:           make(map[uint32]*pglogrepl.RelationMessageV2),
		replicationStarted:    false,
		clientXLogPos:         0,
		lastConfirmedPosition: 0,
		readConfig:            finalReadConfig,
	}, nil
}

func (p *PostgresDatasource) HealthCheck(ctx context.Context) error {
	var result int
	err := p.conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("health check query failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("unexpected result from health check query: %d", result)
	}

	return nil
}

func (p *PostgresDatasource) Close(ctx context.Context) error {
	// Drop temporary slot if using Next() method
	if p.replicationStarted && p.cfg.ReplicationSlot == "" && p.slotName != "" {
		err := pglogrepl.DropReplicationSlot(context.Background(), p.replicationConn, p.slotName, pglogrepl.DropReplicationSlotOptions{
			Wait: true,
		})
		if err != nil {
			p.logger.Error("failed to drop replication slot", zap.String("slot_name", p.slotName), zap.Error(err))
		}
	}

	err := p.conn.Close(ctx)
	if err != nil {
		return fmt.Errorf("failed to close pg connection: %w", err)
	}

	err = p.replicationConn.Close(ctx)
	if err != nil {
		return fmt.Errorf("failed to close replication connection: %w", err)
	}

	p.logger.Info("closed postgres datasource")
	return nil
}

var typeMap = pgtype.NewMap()

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

// Next returns the next message from PostgreSQL replication stream
func (p *PostgresDatasource) Next(ctx context.Context) (*datasource.MessageWithPosition, error) {
	// Initialize replication on first call
	if !p.replicationStarted {
		if err := p.startReplicationForNext(ctx); err != nil {
			return nil, fmt.Errorf("start replication: %w", err)
		}
		p.replicationStarted = true
	}

	standByTimeout := p.cfg.StandByTimeout
	lastStatusUpdate := time.Now()

	for {
		// Send periodic standby status updates
		if time.Since(lastStatusUpdate) >= standByTimeout {
			if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), p.replicationConn,
				pglogrepl.StandbyStatusUpdate{WALFlushPosition: p.lastConfirmedPosition}); err != nil {
				p.logger.Error("SendStandbyStatusUpdate failed", zap.Error(err))
			} else {
				p.logger.Debug("sent standby status message", zap.String("xlog_pos", p.lastConfirmedPosition.String()))
				lastStatusUpdate = time.Now()
			}
		}

		// Receive message with timeout
		receiveCtx, cancel := context.WithTimeout(ctx, standByTimeout)
		rawMsg, err := p.replicationConn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				// Check if context is cancelled
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				default:
					continue
				}
			}
			return nil, fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return nil, fmt.Errorf("postgres WAL error: %v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok || len(msg.Data) == 0 {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				p.logger.Error("failed to parse primary keepalive message", zap.Error(err))
				continue
			}
			p.logger.Debug("received primary keepalive message",
				zap.String("server_wal_end", pkm.ServerWALEnd.String()),
				zap.Bool("reply_requested", pkm.ReplyRequested))

			if pkm.ServerWALEnd > p.clientXLogPos {
				p.clientXLogPos = pkm.ServerWALEnd
			}

			if pkm.ReplyRequested {
				if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), p.replicationConn,
					pglogrepl.StandbyStatusUpdate{WALFlushPosition: p.lastConfirmedPosition}); err != nil {
					p.logger.Error("SendStandbyStatusUpdate failed", zap.Error(err))
				} else {
					lastStatusUpdate = time.Now()
				}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				p.logger.Error("failed to parse xlog data", zap.Error(err))
				continue
			}

			p.logger.Debug("received xlog data",
				zap.String("wal_start", xld.WALStart.String()),
				zap.String("server_wal_end", xld.ServerWALEnd.String()))

			dataMsg, err := p.processDataForNext(xld.WALData)
			if err != nil {
				p.logger.Error("failed to process data", zap.Error(err))
				continue
			}

			// Update position - but don't confirm until Commit() is called
			if xld.WALStart > p.clientXLogPos {
				p.clientXLogPos = xld.WALStart
			}

			// Skip if message was filtered out
			if dataMsg == nil {
				continue
			}

			return &datasource.MessageWithPosition{
				Message:  *dataMsg,
				Position: xld.WALStart,
			}, nil
		}
	}
}

// Commit confirms that messages up to the given position have been successfully processed
func (p *PostgresDatasource) Commit(ctx context.Context, position interface{}) error {
	lsn, ok := position.(pglogrepl.LSN)
	if !ok {
		return fmt.Errorf("invalid position type: expected pglogrepl.LSN, got %T", position)
	}

	p.lastConfirmedPosition = lsn
	p.logger.Debug("committed position", zap.String("lsn", lsn.String()))

	// Send standby status update to confirm the position
	if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), p.replicationConn,
		pglogrepl.StandbyStatusUpdate{WALFlushPosition: lsn}); err != nil {
		return fmt.Errorf("send standby status update: %w", err)
	}

	return nil
}

func (p *PostgresDatasource) startReplicationForNext(ctx context.Context) error {
	// Use configured slot or create temporary one
	if p.cfg.ReplicationSlot != "" {
		p.slotName = p.cfg.ReplicationSlot
	} else {
		randomBytes := make([]byte, 8)
		if _, err := rand.Read(randomBytes); err != nil {
			return fmt.Errorf("generate random slot name: %w", err)
		}
		p.slotName = fmt.Sprintf("replica_%x", randomBytes)
		_, err := pglogrepl.CreateReplicationSlot(ctx, p.replicationConn, p.slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Temporary: true})
		if err != nil {
			return fmt.Errorf("create replication slot: %w", err)
		}
		p.logger.Info("created temporary replication slot", zap.String("slot_name", p.slotName))
	}

	// Get confirmed_flush_lsn from the slot if it exists
	var confirmedLSN pglogrepl.LSN
	err := p.conn.QueryRow(ctx,
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
		p.slotName).Scan(&confirmedLSN)
	if err != nil {
		p.logger.Warn("could not get confirmed_flush_lsn, starting from current position",
			zap.String("slot_name", p.slotName), zap.Error(err))
	}

	startPos := p.sysident.XLogPos
	if confirmedLSN > 0 {
		startPos = confirmedLSN
		p.logger.Info("resuming from confirmed position", zap.String("lsn", confirmedLSN.String()))
	}

	p.clientXLogPos = startPos
	p.lastConfirmedPosition = startPos

	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", strings.Join(p.publications, ",")),
		"messages 'true'",
	}

	if err := pglogrepl.StartReplication(ctx, p.replicationConn, p.slotName, startPos,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	p.logger.Info("started replication", zap.String("slot_name", p.slotName), zap.String("start_pos", startPos.String()))
	return nil
}

func (p *PostgresDatasource) processDataForNext(walData []byte) (*datasource.Message, error) {
	logicalMsg, err := pglogrepl.ParseV2(walData, p.inStream)
	if err != nil {
		return nil, fmt.Errorf("failed to parse logical replication message: %w", err)
	}

	p.logger.Debug("received logical replication message", zap.String("type", logicalMsg.Type().String()))

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.InsertMessageV2:
		return p.processInsertForNext(logicalMsg)
	case *pglogrepl.UpdateMessageV2:
		return p.processUpdateForNext(logicalMsg)
	case *pglogrepl.DeleteMessageV2:
		return p.processDeleteForNext(logicalMsg)
	case *pglogrepl.RelationMessageV2:
		p.relationsV2[logicalMsg.RelationID] = logicalMsg
		p.logger.Debug("stored relation message", zap.Uint32("relation_id", logicalMsg.RelationID),
			zap.String("namespace", logicalMsg.Namespace), zap.String("relation_name", logicalMsg.RelationName))
		return nil, nil
	default:
		p.logger.Debug("skipping message", zap.String("type", logicalMsg.Type().String()))
		return nil, nil
	}
}

func (p *PostgresDatasource) processInsertForNext(logicalMsg *pglogrepl.InsertMessageV2) (*datasource.Message, error) {
	rel, ok := p.relationsV2[logicalMsg.RelationID]
	if !ok {
		return nil, fmt.Errorf("could not find relation for relation ID %d", logicalMsg.RelationID)
	}

	values, err := p.extractDataForNext(rel, logicalMsg.Tuple.Columns)
	if err != nil {
		return nil, fmt.Errorf("failed to extract data: %w", err)
	}

	// Filtered out
	if values == nil {
		return nil, nil
	}

	data, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data: %w", err)
	}

	return &datasource.Message{
		ID:         uuid.New(),
		Op:         datasource.Insert,
		Data:       data,
		Collection: rel.RelationName,
	}, nil
}

func (p *PostgresDatasource) processUpdateForNext(logicalMsg *pglogrepl.UpdateMessageV2) (*datasource.Message, error) {
	rel, ok := p.relationsV2[logicalMsg.RelationID]
	if !ok {
		return nil, fmt.Errorf("could not find relation for relation ID %d", logicalMsg.RelationID)
	}

	values, err := p.extractDataForNext(rel, logicalMsg.NewTuple.Columns)
	if err != nil {
		return nil, fmt.Errorf("failed to extract data: %w", err)
	}

	// Filtered out
	if values == nil {
		return nil, nil
	}

	data, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data: %w", err)
	}

	return &datasource.Message{
		ID:         uuid.New(),
		Op:         datasource.Update,
		Data:       data,
		Collection: rel.RelationName,
	}, nil
}

func (p *PostgresDatasource) processDeleteForNext(logicalMsg *pglogrepl.DeleteMessageV2) (*datasource.Message, error) {
	rel, ok := p.relationsV2[logicalMsg.RelationID]
	if !ok {
		return nil, fmt.Errorf("could not find relation for relation ID %d", logicalMsg.RelationID)
	}

	values, err := p.extractDataForNext(rel, logicalMsg.OldTuple.Columns)
	if err != nil {
		return nil, fmt.Errorf("failed to extract data: %w", err)
	}

	// Filtered out
	if values == nil {
		return nil, nil
	}

	data, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data: %w", err)
	}

	return &datasource.Message{
		ID:         uuid.New(),
		Op:         datasource.Delete,
		Data:       data,
		Collection: rel.RelationName,
	}, nil
}

func (p *PostgresDatasource) extractDataForNext(rel *pglogrepl.RelationMessageV2, dataColumns []*pglogrepl.TupleDataColumn) (map[string]interface{}, error) {
	var matchedMapping *datasource.ReadConfigMapping
	if !p.readConfig.AllCollections {
		for _, m := range p.readConfig.Mapping {
			if m.Collection == rel.RelationName {
				matchedMapping = &m
				break
			}
		}
		// If the collection is not in the mapping, skip
		if matchedMapping == nil {
			p.logger.Debug("skipping for collection", zap.String("namespace", rel.Namespace), zap.String("relation_name", rel.RelationName))
			return nil, nil
		}
	}

	values := map[string]interface{}{}
	for idx, col := range dataColumns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// Skip
		case 't': // text
			val, err := decodeTextColumnData(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("failed to decode text column data: %w", err)
			}
			values[colName] = val
		}
	}

	if matchedMapping != nil {
		for k := range values {
			var matchedFieldMapping *datasource.ReadConfigFieldMapping
			for _, m := range matchedMapping.Fields {
				if m.Field == k {
					matchedFieldMapping = &m
				}
			}
			// If the field is not in the mapping, skip the field
			if matchedFieldMapping == nil && !matchedMapping.AllFields {
				p.logger.Debug("skipping field", zap.String("field", k))
				delete(values, k)
			}
		}
	}

	return values, nil
}
