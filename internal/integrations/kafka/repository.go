package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/turbolytics/librarian/pkg/replicator"
	"go.uber.org/zap"
)

type Repository struct {
	config   kafka.ConfigMap
	producer *kafka.Producer
	topic    string
	logger   *zap.Logger

	// Stats tracking
	statsMu sync.RWMutex
	stats   replicator.TargetStats

	// Batching
	eventBuffer []replicator.Event
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

	// Add query parameters to config
	for key, values := range uri.Query() {
		if len(values) > 0 {
			config[key] = values[0]
		}
	}

	return &Repository{
		topic:  topic,
		config: config,
		logger: logger,
		stats: replicator.TargetStats{
			ConnectionHealthy: false,
			TargetSpecific: map[string]interface{}{
				"topic":   topic,
				"brokers": brokers,
			},
		},
	}, nil
}

func (r *Repository) Connect(ctx context.Context) error {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	// Create producer config from stored values

	producer, err := kafka.NewProducer(&r.config)
	if err != nil {
		r.stats.ConnectionHealthy = false
		r.stats.LastError = err.Error()
		return err
	}

	r.producer = producer
	r.stats.ConnectionHealthy = true
	r.stats.LastError = ""

	go func() {
		defer r.logger.Info("Producer event loop closed")

		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					r.logger.Error("Delivery failed", zap.Error(ev.TopicPartition.Error))
					// Add your retry logic, DLQ, or alerting here
				} else {
					r.logger.Debug("Message delivered",
						zap.String("topic", *ev.TopicPartition.Topic),
						zap.Int32("partition", ev.TopicPartition.Partition),
						zap.Int64("offset", int64(ev.TopicPartition.Offset)))
				}
			case kafka.Error:
				r.logger.Error("Producer error", zap.Error(ev))
			}
		}
	}()

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
	eventData, err := json.Marshal(event)
	if err != nil {
		r.statsMu.Lock()
		r.stats.WriteErrorCount++
		r.stats.LastError = err.Error()
		r.statsMu.Unlock()
		return err
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &r.topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(event.ID),
		Value: eventData,
	}

	if err := r.producer.Produce(message, nil); err != nil {
		r.statsMu.Lock()
		r.stats.WriteErrorCount++
		r.stats.LastError = err.Error()
		r.statsMu.Unlock()
		return err
	}

	r.statsMu.Lock()
	r.stats.TotalEvents += 1
	r.stats.LastWriteAt = time.Now()
	r.stats.LastError = ""
	r.statsMu.Unlock()

	return nil
}

// Flush is a noop since the kafka producer handles batching internally
func (r *Repository) Flush(ctx context.Context) error {
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
