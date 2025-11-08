package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/turbolytics/librarian/internal/replicator"
	"go.uber.org/zap"
)

type Repository struct {
	producer *kafka.Producer
	topic    string
	logger   *zap.Logger

	// Stats tracking
	statsMu sync.RWMutex
	stats   replicator.TargetStats

	// Batching
	eventBuffer []replicator.Event
	batchSize   int
}

func NewRepository(ctx context.Context, uri *url.URL, logger *zap.Logger) (*Repository, error) {
	// Parse topic from path
	topic := strings.TrimPrefix(uri.Path, "/")
	if topic == "" {
		return nil, fmt.Errorf("topic must be specified in URL path")
	}

	// Parse brokers from host
	brokers := uri.Host
	if uri.Port() != "" && !strings.Contains(brokers, ":") {
		brokers = fmt.Sprintf("%s:%s", uri.Hostname(), uri.Port())
	}

	// Parse query parameters for Kafka config
	config := kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         "librarian-replicator",
		"acks":              "all",
	}

	// Add query parameters to config
	for key, values := range uri.Query() {
		if len(values) > 0 {
			config[key] = values[0]
		}
	}

	batchSize := 1
	if batchSizeStr := uri.Query().Get("batch.size"); batchSizeStr != "" {
		if size, err := strconv.Atoi(batchSizeStr); err == nil {
			batchSize = size
		}
	}

	return &Repository{
		topic:     topic,
		logger:    logger,
		batchSize: batchSize,
		stats: replicator.TargetStats{
			ConnectionHealthy: false,
			TargetSpecific: map[string]interface{}{
				"topic":      topic,
				"brokers":    brokers,
				"batch_size": batchSize,
			},
		},
		eventBuffer: make([]replicator.Event, 0, batchSize),
	}, nil
}

func (r *Repository) Connect(ctx context.Context) error {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	// Create producer config from stored values
	config := kafka.ConfigMap{
		"bootstrap.servers": r.stats.TargetSpecific["brokers"].(string),
		"client.id":         "librarian-replicator",

		// Performance optimizations for local development
		"acks":                                  "1",      // Only wait for leader (faster than "all")
		"retries":                               "3",      // Reduce from default
		"batch.size":                            "16384",  // Larger batches
		"linger.ms":                             "5",      // Small delay to batch messages
		"compression.type":                      "snappy", // Compress messages
		"max.in.flight.requests.per.connection": "5",      // Pipeline requests

		// Reduce timeouts for local
		"request.timeout.ms":  "5000",  // 5s instead of 30s default
		"delivery.timeout.ms": "10000", // 10s instead of 120s default
	}

	producer, err := kafka.NewProducer(&config)
	if err != nil {
		r.stats.ConnectionHealthy = false
		r.stats.LastError = err.Error()
		return err
	}

	r.producer = producer
	r.stats.ConnectionHealthy = true
	r.stats.LastError = ""

	r.logger.Info("Kafka target connected",
		zap.String("topic", r.topic),
		zap.String("brokers", r.stats.TargetSpecific["brokers"].(string)))

	return nil
}

func (r *Repository) Disconnect(ctx context.Context) error {
	if r.producer != nil {
		// Flush any remaining messages
		r.producer.Flush(5000) // 5 second timeout
		r.producer.Close()
	}

	r.statsMu.Lock()
	r.stats.ConnectionHealthy = false
	r.statsMu.Unlock()

	return nil
}

func (r *Repository) Write(ctx context.Context, event replicator.Event) error {
	r.statsMu.Lock()
	r.eventBuffer = append(r.eventBuffer, event)
	bufferSize := len(r.eventBuffer)
	r.stats.PendingEvents = bufferSize
	r.statsMu.Unlock()

	// Auto-flush if buffer is full
	if bufferSize >= r.batchSize {
		return r.Flush(ctx)
	}

	return nil
}

func (r *Repository) Flush(ctx context.Context) error {
	r.statsMu.Lock()
	r.logger.Debug("Flushing events to Kafka", zap.Int("buffer_size", len(r.eventBuffer)))
	eventsToFlush := make([]replicator.Event, len(r.eventBuffer))
	copy(eventsToFlush, r.eventBuffer)
	r.eventBuffer = r.eventBuffer[:0] // Clear buffer
	r.stats.PendingEvents = 0
	r.statsMu.Unlock()

	if len(eventsToFlush) == 0 {
		return nil
	}

	// Send all events to Kafka
	for _, event := range eventsToFlush {
		// Serialize event to JSON
		eventData, err := json.Marshal(event)
		if err != nil {
			r.statsMu.Lock()
			r.stats.WriteErrorCount++
			r.stats.LastError = err.Error()
			r.statsMu.Unlock()
			return err
		}

		// Create Kafka message
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &r.topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(event.ID),
			Value: eventData,
		}

		// Send message (non-blocking)
		if err := r.producer.Produce(message, nil); err != nil {
			r.statsMu.Lock()
			r.stats.WriteErrorCount++
			r.stats.LastError = err.Error()
			r.statsMu.Unlock()
			return err
		}
	}

	// Wait for all messages to be delivered
	r.producer.Flush(5000) // 5 second timeout

	// Update stats
	r.statsMu.Lock()
	r.stats.TotalEvents += int64(len(eventsToFlush))
	r.stats.LastWriteAt = time.Now()
	r.stats.LastFlushAt = time.Now()
	r.stats.LastError = ""
	r.statsMu.Unlock()

	r.logger.Info("Flushed events to Kafka",
		zap.Int("event_count", len(eventsToFlush)),
		zap.String("topic", r.topic))

	return nil
}

func (r *Repository) Close(ctx context.Context) error {
	return r.Disconnect(ctx)
}

func (r *Repository) Stats() replicator.TargetStats {
	r.statsMu.RLock()
	defer r.statsMu.RUnlock()

	// Return a copy
	stats := r.stats
	stats.TargetSpecific = make(map[string]interface{})
	for k, v := range r.stats.TargetSpecific {
		stats.TargetSpecific[k] = v
	}

	return stats
}
