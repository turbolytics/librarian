package postgres

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/turbolytics/librarian/internal/replicator"
	"go.uber.org/zap"
)

type Source struct {
	connURI     *url.URL
	replConn    *pgconn.PgConn
	regularConn *pgx.Conn

	logger *zap.Logger

	database        string
	table           string
	slotName        string
	publicationName string

	// WAL replication state
	currentLSN pglogrepl.LSN
	relations  map[uint32]*pglogrepl.RelationMessage

	// Buffer for pending events
	eventBuffer   []replicator.Event
	lastHeartbeat time.Time

	statsMu sync.RWMutex
	stats   replicator.SourceStats
}

func NewSource(uri *url.URL, logger *zap.Logger) (*Source, error) {
	query := uri.Query()
	database := uri.Path[1:]
	table := query.Get("table")

	slotName := query.Get("slot")
	if slotName == "" {
		slotName = fmt.Sprintf("librarian_%s_%s", database, table)
	}

	publicationName := query.Get("publication")
	if publicationName == "" {
		publicationName = fmt.Sprintf("librarian_pub_%s_%s", database, table)
	}

	// Remove custom parameters from the URI to create a clean connection string
	cleanQuery := url.Values{}
	for key, values := range query {
		// Only keep standard PostgreSQL connection parameters
		switch key {
		case "table", "slot", "publication":
			// Remove these custom parameters
			continue
		default:
			// Keep all other parameters (sslmode, connect_timeout, etc.)
			cleanQuery[key] = values
		}
	}

	// Create clean connection URI
	cleanURI := &url.URL{
		Scheme:   uri.Scheme,
		User:     uri.User,
		Host:     uri.Host,
		Path:     uri.Path,
		RawQuery: cleanQuery.Encode(),
		Fragment: uri.Fragment,
	}

	return &Source{
		connURI:         cleanURI,
		database:        database,
		table:           table,
		slotName:        slotName,
		publicationName: publicationName,

		logger:        logger,
		relations:     make(map[uint32]*pglogrepl.RelationMessage),
		eventBuffer:   make([]replicator.Event, 0),
		lastHeartbeat: time.Now(),
		stats: replicator.SourceStats{
			ConnectionHealthy: false,
			SourceSpecific: map[string]interface{}{
				"database":         database,
				"table":            table,
				"slot_name":        slotName,
				"publication_name": publicationName,
			},
		},
	}, nil
}

func (s *Source) Next(ctx context.Context) (replicator.Event, error) {
	// Return buffered events first
	if len(s.eventBuffer) > 0 {
		event := s.eventBuffer[0]
		s.eventBuffer = s.eventBuffer[1:]
		return event, nil
	}

	// Set receive timeout
	receiveCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	msg, err := s.replConn.ReceiveMessage(receiveCtx)
	if err != nil {
		if pgconn.Timeout(err) {
			s.logger.Debug("No new WAL messages")
			return replicator.Event{}, replicator.ErrNoEventsFound
		}

		s.statsMu.Lock()
		s.stats.EventErrorCount++
		s.stats.LastError = err.Error()
		s.statsMu.Unlock()

		s.logger.Error("Failed to receive WAL message", zap.Error(err))
		return replicator.Event{}, err
	}

	// Handle different message types
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				s.logger.Error("Failed to parse primary keepalive message", zap.Error(err))
				return replicator.Event{}, err
			}

			// TODO Metric on keep alive sent
			if keepalive.ReplyRequested {
				err := pglogrepl.SendStandbyStatusUpdate(ctx, s.replConn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: keepalive.ServerWALEnd,
					WALFlushPosition: keepalive.ServerWALEnd,
					WALApplyPosition: keepalive.ServerWALEnd,
					ClientTime:       time.Now(),
					ReplyRequested:   false,
				})
				if err != nil {
					s.logger.Error("Failed to send standby status update", zap.Error(err))
				}
			}
			return replicator.Event{}, replicator.ErrNoEventsFound
		case pglogrepl.XLogDataByteID: // 'w'
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				s.logger.Error("Failed to parse XLogData", zap.Error(err))
				return replicator.Event{}, err
			}

			// Now THIS is what you pass into pglogrepl.Parse
			/*
				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					s.logger.Error("Failed to parse logical WAL data", zap.Error(err))
					return replicator.Event{}, err
				}
			*/

			// handle logicalMsg (Begin/Insert/Update/Delete/Commit/etc)
			return s.processCopyData(xld.WALData)

		default:
			// ignore other message types
			s.logger.Debug("Received non-CopyData message", zap.String("type", fmt.Sprintf("%T", msg)))
			return replicator.Event{}, replicator.ErrNoEventsFound
		}
	case *pgproto3.ErrorResponse:
		err := fmt.Errorf("postgres error: %s", msg.Message)
		s.statsMu.Lock()
		s.stats.EventErrorCount++
		s.stats.LastError = err.Error()
		s.statsMu.Unlock()
		return replicator.Event{}, err
	default:
		s.logger.Debug("Received non-CopyData message", zap.String("type", fmt.Sprintf("%T", msg)))
		return replicator.Event{}, replicator.ErrNoEventsFound
	}
}

func (s *Source) processCopyData(data []byte) (replicator.Event, error) {
	if len(data) == 0 {
		return replicator.Event{}, replicator.ErrNoEventsFound
	}

	// Parse the logical replication message
	fmt.Println(string(data))
	msg, err := pglogrepl.Parse(data)
	if err != nil {
		return replicator.Event{}, fmt.Errorf("failed to parse logical replication message: %w", err)
	}

	switch msg := msg.(type) {
	case *pglogrepl.RelationMessage:
		// Store relation info for later use
		s.relations[msg.RelationID] = msg
		s.logger.Debug("Stored relation info",
			zap.String("relation", msg.RelationName),
			zap.Uint32("relation_id", msg.RelationID))
		return replicator.Event{}, replicator.ErrNoEventsFound

	case *pglogrepl.InsertMessage:
		return s.handleInsert(msg)

	case *pglogrepl.UpdateMessage:
		return s.handleUpdate(msg)

	case *pglogrepl.DeleteMessage:
		return s.handleDelete(msg)

	case *pglogrepl.CommitMessage:
		return s.handleCommit(msg)

	case *pglogrepl.BeginMessage:
		s.logger.Debug("Transaction begin", zap.Uint32("xid", msg.Xid))
		return replicator.Event{}, replicator.ErrNoEventsFound

	default:
		s.logger.Debug("Unhandled message type", zap.String("type", fmt.Sprintf("%T", msg)))
		return replicator.Event{}, replicator.ErrNoEventsFound
	}
}

func (s *Source) handleInsert(msg *pglogrepl.InsertMessage) (replicator.Event, error) {
	rel, exists := s.relations[msg.RelationID]
	if !exists {
		return replicator.Event{}, fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// Skip if not our target table
	if rel.RelationName != s.table {
		return replicator.Event{}, replicator.ErrNoEventsFound
	}

	values := s.tupleToMap(rel, msg.Tuple)

	// Update stats
	s.statsMu.Lock()
	s.stats.TotalEvents++
	s.stats.LastEventAt = time.Now()
	s.stats.SourceSpecific["last_operation"] = "INSERT"
	s.stats.SourceSpecific["current_lsn"] = s.currentLSN.String()
	s.statsMu.Unlock()

	event := replicator.Event{
		ID:       uuid.New().String(),
		Op:       "INSERT",
		Time:     time.Now().Unix(),
		Position: []byte(s.currentLSN.String()),
		Payload: map[string]interface{}{
			"operation": "INSERT",
			"table":     rel.RelationName,
			"schema":    rel.Namespace,
			"data":      values,
		},
	}

	s.logger.Info("PostgreSQL INSERT event",
		zap.String("table", rel.RelationName),
		zap.String("lsn", s.currentLSN.String()),
		zap.Any("data", values))

	return event, nil
}

func (s *Source) handleUpdate(msg *pglogrepl.UpdateMessage) (replicator.Event, error) {
	rel, exists := s.relations[msg.RelationID]
	if !exists {
		return replicator.Event{}, fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	if rel.RelationName != s.table {
		return replicator.Event{}, replicator.ErrNoEventsFound
	}

	var oldValues map[string]interface{}
	if msg.OldTuple != nil {
		oldValues = s.tupleToMap(rel, msg.OldTuple)
	}

	newValues := s.tupleToMap(rel, msg.NewTuple)

	s.statsMu.Lock()
	s.stats.TotalEvents++
	s.stats.LastEventAt = time.Now()
	s.stats.SourceSpecific["last_operation"] = "UPDATE"
	s.stats.SourceSpecific["current_lsn"] = s.currentLSN.String()
	s.statsMu.Unlock()

	event := replicator.Event{
		ID:       uuid.New().String(),
		Op:       "UPDATE",
		Time:     time.Now().Unix(),
		Position: []byte(s.currentLSN.String()),
		Payload: map[string]interface{}{
			"operation": "UPDATE",
			"table":     rel.RelationName,
			"schema":    rel.Namespace,
			"data":      newValues,
			"old_data":  oldValues,
		},
	}

	s.logger.Info("PostgreSQL UPDATE event",
		zap.String("table", rel.RelationName),
		zap.String("lsn", s.currentLSN.String()),
		zap.Any("new_data", newValues))

	return event, nil
}

func (s *Source) handleDelete(msg *pglogrepl.DeleteMessage) (replicator.Event, error) {
	rel, exists := s.relations[msg.RelationID]
	if !exists {
		return replicator.Event{}, fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	if rel.RelationName != s.table {
		return replicator.Event{}, replicator.ErrNoEventsFound
	}

	var oldValues map[string]interface{}
	if msg.OldTuple != nil {
		oldValues = s.tupleToMap(rel, msg.OldTuple)
	}

	s.statsMu.Lock()
	s.stats.TotalEvents++
	s.stats.LastEventAt = time.Now()
	s.stats.SourceSpecific["last_operation"] = "DELETE"
	s.stats.SourceSpecific["current_lsn"] = s.currentLSN.String()
	s.statsMu.Unlock()

	event := replicator.Event{
		ID:       uuid.New().String(),
		Op:       "DELETE",
		Time:     time.Now().Unix(),
		Position: []byte(s.currentLSN.String()),
		Payload: map[string]interface{}{
			"operation": "DELETE",
			"table":     rel.RelationName,
			"schema":    rel.Namespace,
			"old_data":  oldValues,
		},
	}

	s.logger.Info("PostgreSQL DELETE event",
		zap.String("table", rel.RelationName),
		zap.String("lsn", s.currentLSN.String()),
		zap.Any("old_data", oldValues))

	return event, nil
}

func (s *Source) handleCommit(msg *pglogrepl.CommitMessage) (replicator.Event, error) {
	// Update our current LSN
	s.currentLSN = msg.CommitLSN

	// Send heartbeat back to PostgreSQL periodically
	if time.Since(s.lastHeartbeat) > 30*time.Second {
		err := pglogrepl.SendStandbyStatusUpdate(context.Background(), s.replConn, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: msg.CommitLSN,
			WALFlushPosition: msg.CommitLSN,
			WALApplyPosition: msg.CommitLSN,
			ClientTime:       time.Now(),
			ReplyRequested:   false,
		})
		if err != nil {
			s.logger.Error("Failed to send standby status update", zap.Error(err))
		} else {
			s.lastHeartbeat = time.Now()
			s.logger.Debug("Sent heartbeat to PostgreSQL", zap.String("lsn", msg.CommitLSN.String()))
		}
	}

	return replicator.Event{}, replicator.ErrNoEventsFound
}

func (s *Source) tupleToMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]interface{} {
	values := make(map[string]interface{})

	for i, col := range rel.Columns {
		if i >= len(tuple.Columns) {
			break
		}

		tupleCol := tuple.Columns[i]
		var value interface{}

		switch tupleCol.DataType {
		case 'n': // null
			value = nil
		case 't': // text
			// Try to convert common types
			dataStr := string(tupleCol.Data)
			if col.DataType == 23 { // int4
				if intVal, err := strconv.Atoi(dataStr); err == nil {
					value = intVal
				} else {
					value = dataStr
				}
			} else if col.DataType == 20 { // int8 (bigint)
				if intVal, err := strconv.ParseInt(dataStr, 10, 64); err == nil {
					value = intVal
				} else {
					value = dataStr
				}
			} else {
				value = dataStr
			}
		case 'b': // binary (shouldn't happen with text protocol)
			value = tupleCol.Data
		default:
			value = string(tupleCol.Data)
		}

		values[col.Name] = value
	}

	return values
}

func (s *Source) Connect(ctx context.Context, checkpoint *replicator.Checkpoint) error {
	s.statsMu.Lock()
	s.stats.ConnectionRetries++
	s.statsMu.Unlock()

	// Create regular connection for setup
	regularConn, err := pgx.Connect(ctx, s.connURI.String())
	if err != nil {
		s.statsMu.Lock()
		s.stats.ConnectionHealthy = false
		s.stats.LastError = err.Error()
		s.statsMu.Unlock()
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	s.regularConn = regularConn

	// Create replication connection FIRST
	replConnConfig, err := pgconn.ParseConfig(s.connURI.String())
	if err != nil {
		return fmt.Errorf("failed to parse replication config: %w", err)
	}

	// Enable replication mode
	replConnConfig.RuntimeParams["replication"] = "database"
	replConn, err := pgconn.ConnectConfig(ctx, replConnConfig)
	if err != nil {
		s.statsMu.Lock()
		s.stats.ConnectionHealthy = false
		s.stats.LastError = err.Error()
		s.statsMu.Unlock()
		return fmt.Errorf("failed to create replication connection: %w", err)
	}
	s.replConn = replConn

	// Setup publication and slot (this needs the replication connection)
	if err := s.setupReplication(ctx); err != nil {
		s.statsMu.Lock()
		s.stats.ConnectionHealthy = false
		s.stats.LastError = err.Error()
		s.statsMu.Unlock()
		return fmt.Errorf("failed to setup replication: %w", err)
	}

	// Get starting LSN from checkpoint or current position
	startLSN, err := s.getStartingLSN(ctx, checkpoint)
	if err != nil {
		return fmt.Errorf("failed to get starting LSN: %w", err)
	}
	s.currentLSN = startLSN

	// Start replication
	err = pglogrepl.StartReplication(ctx, s.replConn, s.slotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", s.publicationName),
		},
	})
	if err != nil {
		s.statsMu.Lock()
		s.stats.ConnectionHealthy = false
		s.stats.LastError = err.Error()
		s.statsMu.Unlock()
		return fmt.Errorf("failed to start replication: %w", err)
	}

	// Update connection stats
	s.statsMu.Lock()
	s.stats.ConnectionHealthy = true
	s.stats.LastConnectAt = time.Now()
	s.stats.LastError = ""
	s.stats.SourceSpecific["current_lsn"] = startLSN.String()
	s.statsMu.Unlock()

	s.logger.Info("PostgreSQL replication started",
		zap.String("database", s.database),
		zap.String("table", s.table),
		zap.String("slot", s.slotName),
		zap.String("publication", s.publicationName),
		zap.String("start_lsn", startLSN.String()))

	return nil
}

func (s *Source) Close() error {
	return s.Disconnect(context.Background())
}

func (s *Source) Stats() replicator.SourceStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	stats := s.stats
	stats.SourceSpecific = make(map[string]interface{})
	for k, v := range s.stats.SourceSpecific {
		stats.SourceSpecific[k] = v
	}

	return stats
}

func (s *Source) Disconnect(ctx context.Context) error {
	if s.replConn != nil {
		s.replConn.Close(ctx)
	}
	if s.regularConn != nil {
		s.regularConn.Close(ctx)
	}

	s.statsMu.Lock()
	s.stats.ConnectionHealthy = false
	s.statsMu.Unlock()

	return nil
}

func (s *Source) setupReplication(ctx context.Context) error {
	// Check if publication exists first
	var exists bool
	err := s.regularConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		s.publicationName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check publication existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("publication '%s' does not exist. Please create it manually with: CREATE PUBLICATION %s FOR TABLE %s",
			s.publicationName, s.publicationName, s.table)
	}

	// Publication should be managed outside of librarian
	/*
		if !exists {
			// Create publication for the table
			createPubSQL := fmt.Sprintf(
				"CREATE PUBLICATION %s FOR TABLE %s",
				pgx.Identifier{s.publicationName}.Sanitize(),
				pgx.Identifier{s.table}.Sanitize(),
			)

			_, err = s.regularConn.Exec(ctx, createPubSQL)
			if err != nil {
				return fmt.Errorf("failed to create publication: %w", err)
			}
			s.logger.Info("Created publication", zap.String("publication", s.publicationName))
		}
	*/

	// Check if replication slot exists
	err = s.regularConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		s.slotName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check slot existence: %w", err)
	}

	if !exists {
		// Actually create the replication slot
		_, err = pglogrepl.CreateReplicationSlot(ctx, s.replConn, s.slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
		s.logger.Info("Created replication slot", zap.String("slot", s.slotName))
	}

	return nil
}

func (s *Source) getStartingLSN(ctx context.Context, checkpoint *replicator.Checkpoint) (pglogrepl.LSN, error) {
	if checkpoint != nil {
		if lsnStr := string(checkpoint.Position); lsnStr != "" {
			if lsn, err := pglogrepl.ParseLSN(lsnStr); err == nil {
				s.logger.Info("Resuming from checkpoint", zap.String("lsn", lsnStr))
				return lsn, nil
			}
		}
	}

	// Get current LSN
	var currentLSNStr string
	err := s.regularConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&currentLSNStr)
	if err != nil {
		return 0, fmt.Errorf("failed to get current LSN: %w", err)
	}

	lsn, err := pglogrepl.ParseLSN(currentLSNStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN: %w", err)
	}

	s.logger.Info("Starting from current LSN", zap.String("lsn", currentLSNStr))
	return lsn, nil
}
