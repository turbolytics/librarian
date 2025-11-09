package mongo

import (
	"context"
	"encoding/base64"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/turbolytics/librarian/internal/replicator"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

type Source struct {
	client     *mongo.Client
	database   string
	collection string
	logger     *zap.Logger

	changeStream *mongo.ChangeStream
	statsMu      sync.RWMutex
	stats        replicator.SourceStats
}

func NewSource(ctx context.Context, uri *url.URL, logger *zap.Logger) (*Source, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri.String()))
	if err != nil {
		return nil, err
	}

	// Extract database from URI if needed
	database := uri.Path[1:]
	collection := uri.Query().Get("collection")

	return &Source{
		client:     client,
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

func (s *Source) Connect(checkpoint *replicator.Checkpoint) error {
	s.statsMu.Lock()
	s.stats.ConnectionRetries++
	s.statsMu.Unlock()

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

func (s *Source) Disconnect() error {
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

	return s.client.Disconnect(context.Background())
}

func (s *Source) Close() error {
	if s.changeStream != nil {
		s.changeStream.Close(context.Background())
	}
	return s.client.Disconnect(context.Background())
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

		s.logger.Info("No more change events")
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

	s.logger.Info("Change event received",
		zap.String("operation", changeEvent["operationType"].(string)),
		zap.Any("document_key", changeEvent["documentKey"]),
		zap.Any("event", changeEvent),
	)

	return replicator.Event{
		ID:       uuid.New().String(),
		Time:     time.Now().Unix(),
		Payload:  changeEvent,
		Position: []byte(token),
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
