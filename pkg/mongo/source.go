package mongo

import (
	"context"
	"encoding/base64"
	"net/url"
	"sync"
	"time"

	"github.com/turbolytics/librarian/pkg/replicator"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type Source struct {
	client     *mongo.Client
	connURI    *url.URL
	database   string
	collection string
	logger     *zap.Logger

	changeStream *mongo.ChangeStream
	statsMu      sync.RWMutex
	stats        replicator.SourceStats
}

func NewSource(ctx context.Context, uri *url.URL, logger *zap.Logger) (*Source, error) {
	// Extract database from URI if needed
	database := uri.Path[1:]
	collection := uri.Query().Get("collection")

	return &Source{
		connURI:    uri,
		database:   database,
		collection: collection,
		logger:     logger,
		stats: replicator.SourceStats{
			ConnectionHealthy: false,
			SourceSpecific: map[string]interface{}{
				"database":   database,
				"collection": collection,
			},
		},
	}, nil
}

func (s *Source) Connect(ctx context.Context, checkpoint *replicator.Checkpoint) error {
	s.statsMu.Lock()
	s.stats.ConnectionRetries++
	s.statsMu.Unlock()

	var err error
	s.client, err = mongo.Connect(ctx, options.Client().ApplyURI(s.connURI.String()))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.client.Ping(ctx, nil); err != nil {
		s.statsMu.Lock()
		s.stats.ConnectionHealthy = false
		s.stats.LastError = err.Error()
		s.statsMu.Unlock()
		return err
	}

	opts := options.ChangeStream().
		SetMaxAwaitTime(5 * time.Second)

	if checkpoint != nil {
		var resumeToken bson.Raw
		resumeTokenBytes, err := base64.StdEncoding.DecodeString(string(checkpoint.Position))
		if err != nil {
			s.logger.Error("Failed to decode resume token from checkpoint", zap.Error(err))
			return err
		}
		resumeToken = bson.Raw(resumeTokenBytes)
		opts.SetResumeAfter(resumeToken)
		s.logger.Info("Resuming from checkpoint",
			zap.String("database", s.database),
			zap.String("collection", s.collection),
			zap.Any("resume_token", checkpoint.Position))
	}

	// SetFullDocument(options.UpdateLookup) // Include full document for updates
	coll := s.client.Database(s.database).Collection(s.collection)

	// You can watch at different levels:
	// 1. Entire cluster: client.Watch()
	// 2. Database: database.Watch()
	// 3. Collection: collection.Watch()
	changeStream, err := coll.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		s.statsMu.Lock()
		s.stats.ConnectionHealthy = false
		s.stats.LastError = err.Error()
		s.statsMu.Unlock()
		return err
	}

	s.statsMu.Lock()
	s.stats.ConnectionHealthy = true
	s.stats.LastConnectAt = time.Now()
	s.stats.LastError = ""
	s.statsMu.Unlock()

	s.changeStream = changeStream
	s.logger.Info("MongoDB change stream started",
		zap.String("database", s.database),
		zap.String("collection", s.collection))

	return nil
}

func (s *Source) Disconnect(ctx context.Context) error {
	if s.changeStream != nil {
		if err := s.changeStream.Close(context.Background()); err != nil {
			s.statsMu.Lock()
			s.stats.LastError = err.Error()
			s.statsMu.Unlock()
			return err
		}
	}

	s.statsMu.Lock()
	s.stats.ConnectionHealthy = false
	s.statsMu.Unlock()

	return s.client.Disconnect(ctx)
}

func (s *Source) Close() error {
	return s.Disconnect(context.Background())
}

// Example of processing change events
func (s *Source) Next(ctx context.Context) (replicator.Event, error) {
	if ok := s.changeStream.Next(ctx); !ok {
		if err := s.changeStream.Err(); err != nil {

			s.statsMu.Lock()
			s.stats.EventErrorCount++
			s.stats.LastError = err.Error()
			s.statsMu.Unlock()

			s.logger.Error("Change stream error", zap.Error(err))
			return replicator.Event{}, err
		}

		s.logger.Debug("No more change events")
		return replicator.Event{}, replicator.ErrNoEventsFound
	}

	var changeEvent bson.M
	if err := s.changeStream.Decode(&changeEvent); err != nil {

		s.statsMu.Lock()
		s.stats.EventErrorCount++
		s.stats.LastError = err.Error()
		s.statsMu.Unlock()

		s.logger.Error("Failed to decode change event", zap.Error(err))
		return replicator.Event{}, err
	}

	s.statsMu.Lock()
	s.stats.TotalEvents++
	s.stats.LastEventAt = time.Now()
	s.stats.LastError = ""

	eventData, _ := bson.Marshal(changeEvent)
	s.stats.TotalBytes += int64(len(eventData))
	s.stats.SourceSpecific["last_operation_type"] = changeEvent["operationType"]
	s.statsMu.Unlock()

	token := base64.StdEncoding.EncodeToString(s.changeStream.ResumeToken())

	s.logger.Debug("Change event received",
		zap.String("operation", changeEvent["operationType"].(string)),
		zap.Any("document_key", changeEvent["documentKey"]),
		zap.Any("event", changeEvent),
	)

	// Convert MongoDB operation type to Debezium operation code
	opType := changeEvent["operationType"].(string)
	var op replicator.Operation
	switch opType {
	case "insert":
		op = replicator.OpCreate
	case "update", "replace":
		op = replicator.OpUpdate
	case "delete":
		op = replicator.OpDelete
	default:
		op = replicator.OpRead
	}

	// Extract before/after data based on operation type
	var before, after map[string]interface{}
	if fullDoc, ok := changeEvent["fullDocument"].(bson.M); ok {
		after = fullDoc
	}
	if fullDocBefore, ok := changeEvent["fullDocumentBeforeChange"].(bson.M); ok {
		before = fullDocBefore
	}

	now := time.Now()

	return replicator.Event{
		Position: []byte(token),
		Payload: replicator.Payload{
			Before: before,
			After:  after,
			Source: replicator.EventSource{
				Version:   "1.0.0",
				Connector: "mongodb",
				Name:      s.database,
				TsMs:      now.UnixMilli(),
				Snapshot:  "false",
				Db:        s.database,
				Schema:    s.collection, // MongoDB doesn't have schemas, use collection
				Table:     s.collection,
				Xmin:      nil,
			},
			Op:          op,
			TsMs:        now.UnixMilli(),
			Transaction: nil,
		},
	}, nil
}

func (s *Source) Stats() replicator.SourceStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	// Return a copy to prevent race conditions
	stats := s.stats
	stats.SourceSpecific = make(map[string]interface{})
	for k, v := range s.stats.SourceSpecific {
		stats.SourceSpecific[k] = v
	}
	return stats
}

func (s *Source) Checkpoint(ctx context.Context, checkpoint *replicator.Checkpoint) error {
	// For MongoDB, this is a no-op since the checkpoint (resume token) is already
	// persisted to storage by the Replicator after a successful flush.
	// MongoDB change streams will automatically resume from the saved resume token
	// on reconnection, so we don't need to send any acknowledgment back to MongoDB.
	if checkpoint != nil {
		s.logger.Debug("MongoDB checkpoint notification received",
			zap.String("replicator_id", checkpoint.ReplicatorID),
			zap.Time("timestamp", checkpoint.Timestamp))
	}
	return nil
}
