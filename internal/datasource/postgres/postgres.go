package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"strconv"
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
	subscriptions   []*subscription
	logger          *zap.Logger
	sysident        pglogrepl.IdentifySystemResult
	publications    []string
	cfg             config.PostgresDatasource
}

type subscription struct {
	*datasource.Subscription
	slotName string
	done     chan struct{}
}

func NewPostgresDatasource(ctx context.Context, cfg config.PostgresDatasource, logger *zap.Logger) (*PostgresDatasource, error) {
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

	return &PostgresDatasource{
		pgConnection,
		replicationConn,
		make([]*subscription, 0),
		logger.With(zap.String("datasource-type", "postgres")),
		sysident,
		publications,
		cfg,
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

func (p *PostgresDatasource) Read(ctx context.Context, cfg datasource.ReadConfig) (*datasource.Subscription, error) {
	subId := uuid.New()
	s := &subscription{
		Subscription: &datasource.Subscription{
			ID:         subId,
			Ch:         make(chan datasource.Message),
			ReadConfig: cfg,
		},
		slotName: p.cfg.ReplicationSlot,
		done:     make(chan struct{}),
	}

	// if the slot name is not provided, create a temporary one
	if p.cfg.ReplicationSlot == "" {
		s.slotName = "replica_" + strconv.Itoa(rand.Int())
		_, err := pglogrepl.CreateReplicationSlot(ctx, p.replicationConn, s.slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
			// meaning that the slot will be dropped when the connection is closed
			Temporary: true,
		})
		if err != nil {
			return nil, fmt.Errorf("create replication slot: %w", err)
		}
		p.logger.Info("created temporary replication slot", zap.String("slot_name", s.slotName))
	}

	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", strings.Join(p.publications, ",")),
		"messages 'true'",
		// TODO: figure out in which cases this could be useful
		// "streaming 'true'",
	}
	err := pglogrepl.StartReplication(context.Background(), p.replicationConn, s.slotName, p.sysident.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return nil, fmt.Errorf("start replication: %w", err)
	}
	p.logger.Info("started replication", zap.String("slot_name", s.slotName))

	go p.startPublishing(s)

	p.subscriptions = append(p.subscriptions, s)

	return s.Subscription, nil
}

func (p *PostgresDatasource) Close(ctx context.Context) error {
	for _, sub := range p.subscriptions {
		if p.cfg.ReplicationSlot == "" {
			// drop only if the slot was temporary
			err := pglogrepl.DropReplicationSlot(context.Background(), p.replicationConn, sub.slotName, pglogrepl.DropReplicationSlotOptions{})
			if err != nil {
				p.logger.Error("failed to drop replication slot", zap.String("slot_name", sub.slotName), zap.Error(err))
			}
		}
		// TODO: add check before writing to the channel
		close(sub.Ch)
		close(sub.done)
	}

	err := p.conn.Close(ctx)
	if err != nil {
		return fmt.Errorf("failed to close pg connection: %w", err)
	}

	p.logger.Info("closed postgres datasource")
	return nil
}

func (p *PostgresDatasource) startPublishing(sub *subscription) error {
	p.logger.Info("start publishing", zap.String("slot_name", sub.slotName))
	clientXLogPos := p.sysident.XLogPos
	relationsV2 := map[uint32]*pglogrepl.RelationMessageV2{}
	inStream := false

	// TODO: make it configurable
	standByTimeout := 10 * time.Second
	standbyMessageTicker := time.NewTicker(standByTimeout)
	defer standbyMessageTicker.Stop()
Loop:
	for {
		select {
		case <-sub.done:
			break Loop
		case <-standbyMessageTicker.C:
			err := pglogrepl.SendStandbyStatusUpdate(context.Background(), p.replicationConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				p.logger.Error("SendStandbyStatusUpdate failed", zap.Error(err))
				continue
			}
			p.logger.Debug("sent standby status message", zap.String("xlog_pos", clientXLogPos.String()))
		default:
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), standByTimeout)
			rawMsg, err := p.replicationConn.ReceiveMessage(ctx)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				p.logger.Error("ReceiveMessage failed", zap.Error(err))
				continue
			}
			p.logger.Info("time to receive message", zap.Duration("duration", time.Since(start)))

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				p.logger.Error("received Postgres WAL error", zap.Any("error", errMsg))
				continue
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				p.logger.Error("unexpected message type", zap.Any("msg", rawMsg))
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					p.logger.Error("failed to parse primary keepalive message", zap.Error(err))
					continue
				}
				p.logger.Debug("received primary keepalive message", zap.String("server_wal_end", pkm.ServerWALEnd.String()), zap.String("server_time", pkm.ServerTime.String()), zap.Bool("reply_requested", pkm.ReplyRequested))
				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					err := pglogrepl.SendStandbyStatusUpdate(context.Background(), p.replicationConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
					if err != nil {
						p.logger.Error("SendStandbyStatusUpdate failed", zap.Error(err))
						continue
					}
					p.logger.Info("sent standby status message", zap.String("xlog_pos", clientXLogPos.String()))
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					p.logger.Error("failed to parse xlog data", zap.Error(err))
					continue
				}

				p.logger.Info("received xlog data", zap.String("wal_start", xld.WALStart.String()), zap.String("server_wal_end", xld.ServerWALEnd.String()), zap.String("server_time", xld.ServerTime.String()))
				err = p.processData(xld.WALData, relationsV2, &inStream, sub)
				if err != nil {
					p.logger.Error("failed to process data", zap.Error(err))
					continue
				}

				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}
				p.logger.Info("time to process message", zap.Duration("duration", time.Since(start)))
			}
		}
	}

	p.logger.Info("stop publishing", zap.String("slot_name", sub.slotName))
	return nil
}

func (p *PostgresDatasource) extractData(rel *pglogrepl.RelationMessageV2, dataColumns []*pglogrepl.TupleDataColumn, sub *subscription) (map[string]interface{}, error) {
	var matchedMapping *datasource.ReadConfigMapping
	if !sub.ReadConfig.AllCollections {
		// check if the collection is in the mapping
		for _, m := range sub.ReadConfig.Mapping {
			m := m
			if m.Collection == rel.RelationName {
				matchedMapping = &m
				break
			}
		}
		// if the collection is not in the mapping, skip the insert
		if matchedMapping == nil {
			p.logger.Debug("skipping insert for xid", zap.String("namespace", rel.Namespace), zap.String("relation_name", rel.RelationName))
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
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
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
			// check if the field is in the mapping
			var matchedFieldMapping *datasource.ReadConfigFieldMapping
			for _, m := range matchedMapping.Fields {
				m := m
				if m.Field == k {
					matchedFieldMapping = &m
				}
			}
			// if the field is not in the mapping, skip the field
			if matchedFieldMapping == nil && !matchedMapping.AllFields {
				p.logger.Debug("skipping field", zap.String("field", k))
				delete(values, k)
			}
		}
	}

	return values, nil
}

func (p *PostgresDatasource) processInsert(logicalMsg *pglogrepl.InsertMessageV2, relations map[uint32]*pglogrepl.RelationMessageV2, sub *subscription) error {
	rel, ok := relations[logicalMsg.RelationID]
	if !ok {
		return fmt.Errorf("could not find relation for relation ID %d", logicalMsg.RelationID)
	}

	values, err := p.extractData(rel, logicalMsg.Tuple.Columns, sub)
	if err != nil {
		return fmt.Errorf("failed to extract data: %w", err)
	}

	p.logger.Info("insert for xid", zap.Uint32("xid", logicalMsg.Xid), zap.String("namespace", rel.Namespace), zap.String("relation_name", rel.RelationName), zap.Any("values", values))
	data, _ := json.Marshal(values)
	sub.Ch <- datasource.Message{Op: datasource.Insert, Data: data, Collection: rel.RelationName}

	return nil
}

func (p *PostgresDatasource) processUpdate(logicalMsg *pglogrepl.UpdateMessageV2, relations map[uint32]*pglogrepl.RelationMessageV2, sub *subscription) error {
	p.logger.Info("update message", zap.Uint32("xid", logicalMsg.Xid),
		zap.Any("old_tuple", logicalMsg.OldTuple), zap.Any("new_tuple", logicalMsg.NewTuple))
	rel, ok := relations[logicalMsg.RelationID]
	if !ok {
		return fmt.Errorf("could not find relation for relation ID %d", logicalMsg.RelationID)
	}

	values, err := p.extractData(rel, logicalMsg.NewTuple.Columns, sub)
	if err != nil {
		return fmt.Errorf("failed to extract data: %w", err)
	}

	data, _ := json.Marshal(values)
	sub.Ch <- datasource.Message{Op: datasource.Update, Data: data, Collection: rel.RelationName}

	return nil
}

func (p *PostgresDatasource) processDelete(logicalMsg *pglogrepl.DeleteMessageV2, relations map[uint32]*pglogrepl.RelationMessageV2, sub *subscription) error {
	rel, ok := relations[logicalMsg.RelationID]
	if !ok {
		return fmt.Errorf("could not find relation for relation ID %d", logicalMsg.RelationID)
	}

	p.logger.Debug("delete for xid", zap.Uint32("xid", logicalMsg.Xid),
		zap.String("namespace", rel.Namespace),
		zap.String("relation_name", rel.RelationName),
	)

	values, err := p.extractData(rel, logicalMsg.OldTuple.Columns, sub)
	if err != nil {
		return fmt.Errorf("failed to extract data: %w", err)
	}

	data, _ := json.Marshal(values)
	sub.Ch <- datasource.Message{Op: datasource.Delete, Data: data, Collection: rel.RelationName}

	return nil
}

func (p *PostgresDatasource) processData(walData []byte, relations map[uint32]*pglogrepl.RelationMessageV2, inStream *bool, sub *subscription) error {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		return fmt.Errorf("failed to parse logical replication message: %w", err)
	}

	p.logger.Info("received logical replication message", zap.String("type", logicalMsg.Type().String()))
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		relations[logicalMsg.RelationID] = logicalMsg
	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.
	case *pglogrepl.CommitMessage:
	case *pglogrepl.InsertMessageV2:
		return p.processInsert(logicalMsg, relations, sub)
	case *pglogrepl.UpdateMessageV2:
		return p.processUpdate(logicalMsg, relations, sub)
	case *pglogrepl.DeleteMessageV2:
		return p.processDelete(logicalMsg, relations, sub)
	case *pglogrepl.TruncateMessageV2:
		p.logger.Info("truncate for xid", zap.Uint32("xid", logicalMsg.Xid))
	case *pglogrepl.TypeMessageV2:
	case *pglogrepl.OriginMessage:
	case *pglogrepl.LogicalDecodingMessageV2:
		p.logger.Info("logical decoding message", zap.String("prefix", logicalMsg.Prefix), zap.String("content", string(logicalMsg.Content)), zap.Uint32("xid", logicalMsg.Xid))
	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		p.logger.Info("stream start message", zap.Uint32("xid", logicalMsg.Xid), zap.Bool("first_segment", logicalMsg.FirstSegment == 1))
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		p.logger.Info("stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		p.logger.Info("stream commit message", zap.Uint32("xid", logicalMsg.Xid))
	case *pglogrepl.StreamAbortMessageV2:
		p.logger.Info("stream abort message", zap.Uint32("xid", logicalMsg.Xid))
		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}

	return nil
}

var typeMap = pgtype.NewMap()

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
