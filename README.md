# Librarian

Librarian is a cloud-native database replicator designed as a modern replacement for Kafka Connect. It uses native database replication technologies like MongoDB Change Streams and PostgreSQL logical replication to efficiently stream data changes to message brokers and storage systems.

## Why Librarian?

- **Single Binary**: No JVM, no external dependencies, no connector management
- **Cloud Native**: Built-in metrics, tracing, and observability
- **Lightweight**: Runs on modest hardware with minimal resource overhead
- **Simple Configuration**: URL-based source and target configuration

## Supported Sources

- MongoDB (Change Streams)
- PostgreSQL (Logical Replication)

## Supported Targets

- Kafka
- S3 (Parquet)
- Local filesystem

## Quickstart

Stream MongoDB changes to Kafka in minutes.

### 1. Start Backing Services

Start MongoDB (with replica set) and Kafka:

```bash
make start-backing-services
```

### 2. Start the Replicator

Run the replicator to stream changes from MongoDB to Kafka:

```bash
LIBRARIAN_LOG_LEVEL=DEBUG go run cmd/librarian/main.go archiver replicate \
  --source "mongodb://localhost:27017/test?authSource=admin&collection=users" \
  --id=mongodb.test.users \
  -t "kafka://localhost:9092/order-events"
```

You should see output indicating the replicator has started:

```
2025-11-19T08:22:38.855-0500    INFO    librarian.replicator    archiver/replicate.go:69        starting replicator!
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator    archiver/replicate.go:71        replicating data...
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator    archiver/replicate.go:86        initializing MongoDB source     {"url": "mongodb://localhost:27017/test?authSource=admin&collection=users"}
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator    archiver/replicate.go:112       initializing kafka target       {"url": "kafka://localhost:9092/order-events"}
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator    replicator/replicator.go:142    Replicator created      {"state": "created"}
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator    replicator/server.go:40 replicator registered   {"replicator_id": "mongodb.test.users", "state": "created"}
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator    replicator/server.go:156        starting replicator server      {"addr": ":8080"}
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator.fsm        replicator/fsm.go:124   State transitioned      {"state": "connecting", "from": "created"}
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator    replicator/replicator.go:159    Starting replicator     {"state": "connecting", "source-options": "{CheckpointBatchSize:0 EmptyPollInterval:500ms}", "target-options": "{FlushTimeout:0s}"}
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator    replicator/checkpoint.go:65     No checkpoint found     {"replicator_id": "mongodb.test.users"}
2025-11-19T08:22:38.856-0500    INFO    librarian.replicator    replicator/replicator.go:184    No checkpoint found, starting fresh      {"replicator_id": "mongodb.test.users"}
```

### 3. Insert a Test Record

In another terminal, connect to MongoDB and insert a record:

```bash
docker exec -it dev_mongodb_1 mongosh
```

```javascript
rs0 [direct: primary] test> db.users.insert({"name":"test"});
{
  acknowledged: true,
  insertedIds: { '0': ObjectId('691dc55aa7af9969a1b1ddf4') }
}
```

### 4. Verify Replication

Back in the replicator terminal, you'll see the change event processed:

```
2025-11-19T08:22:39.356-0500    DEBUG   librarian.replicator    replicator/replicator.go:220    Change event received   {"operation": "insert", "collection": "users"}
2025-11-19T08:22:39.357-0500    DEBUG   librarian.replicator    replicator/replicator.go:245    Message sent to Kafka   {"topic": "order-events", "partition": 0}
```

The document is now available in your Kafka topic.

## PostgreSQL to Kafka Example

Stream PostgreSQL changes to Kafka using logical replication.

### 1. Start the Replicator

Run the replicator to stream changes from PostgreSQL to Kafka:

```bash
LIBRARIAN_LOG_LEVEL=DEBUG go run cmd/librarian/main.go archiver replicate \
  --source "postgres://test:test@localhost:5432/test?slot=librarian_users&sslmode=disable" \
  --target "kafka://localhost:9092/postgres-changes" \
  --id=postgres-to-kafka
```

You should see output indicating the replicator has started:

```
2025-11-19T15:07:17.718-0500    INFO    librarian.replicator    archiver/replicate.go:69        starting replicator!
2025-11-19T15:07:17.718-0500    INFO    librarian.replicator    archiver/replicate.go:71        replicating data...
2025-11-19T15:07:17.718-0500    INFO    librarian.replicator    archiver/replicate.go:97        initializing Postgres source    {"url": "postgres://test:test@localhost:5432/test?slot=librarian_users&sslmode=disable"}
2025-11-19T15:07:17.718-0500    INFO    librarian.replicator    archiver/replicate.go:112       initializing kafka target       {"url": "kafka://localhost:9092/postgres-changes"}
2025-11-19T15:07:17.718-0500    INFO    librarian.replicator    replicator/replicator.go:142    Replicator created      {"state": "created"}
2025-11-19T15:07:17.719-0500    INFO    librarian.replicator    replicator/server.go:40 replicator registered   {"replicator_id": "postgres-to-kafka", "state": "created"}
2025-11-19T15:07:17.719-0500    INFO    librarian.replicator    replicator/server.go:156        starting replicator server      {"addr": ":8080"}
2025-11-19T15:07:17.719-0500    INFO    librarian.replicator.fsm        replicator/fsm.go:124   State transitioned      {"state": "connecting", "from": "created"}
2025-11-19T15:07:17.719-0500    INFO    librarian.replicator    replicator/replicator.go:159    Starting replicator     {"state": "connecting", "source-options": "{CheckpointBatchSize:0 EmptyPollInterval:500ms}", "target-options": "{FlushTimeout:0s}"}
2025-11-19T15:07:17.719-0500    INFO    librarian.replicator    replicator/checkpoint.go:65     No checkpoint found     {"replicator_id": "postgres-to-kafka"}
2025-11-19T15:07:17.719-0500    INFO    librarian.replicator    replicator/replicator.go:184    No checkpoint found, starting fresh     {"replicator_id": "postgres-to-kafka"}
2025-11-19T15:07:17.734-0500    INFO    librarian.replicator    kafka/repository.go:121 Kafka target connected  {"topic": "postgres-changes", "brokers": "localhost:9092"}
2025-11-19T15:07:17.776-0500    INFO    librarian.replicator    postgres/source.go:582  Starting from current LSN       {"lsn": "0/1995338"}
2025-11-19T15:07:17.777-0500    INFO    librarian.replicator    postgres/source.go:483  PostgreSQL replication started  {"database": "test", "slot": "librarian_users", "publication": "librarian_pub_test", "start_lsn": "0/1995338"}
2025-11-19T15:07:17.777-0500    INFO    librarian.replicator.fsm        replicator/fsm.go:124   State transitioned      {"state": "streaming", "from": "connecting"}
2025-11-19T15:07:17.777-0500    INFO    librarian.replicator    replicator/replicator.go:204    Replicator started      {"state": "streaming"}
```

### 2. Insert a Test Record

In another terminal, connect to PostgreSQL and insert a record:

```bash
docker exec -it dev_postgres_1 psql -U test
```

```sql
test=# INSERT INTO users (account, signup_time) VALUES ('alice19example.com', NOW());
INSERT 0 1
```

### 3. Verify Replication

Back in the replicator terminal, you'll see the change event processed:

```
2025-11-19T15:07:21.786-0500    DEBUG   librarian.replicator    postgres/source.go:215  Transaction begin       {"xid": 830}
2025-11-19T15:07:22.288-0500    DEBUG   librarian.replicator    postgres/source.go:197  Stored relation info    {"relation": "users", "relation_id": 16400}
2025-11-19T15:07:22.790-0500    DEBUG   librarian.replicator    postgres/source.go:253  PostgreSQL INSERT event {"table": "users", "lsn": "0/1995338", "data": {"account":"alice19example.com","signup_time":"2025-11-19 20:07:20.830027"}}
2025-11-19T15:07:22.824-0500    DEBUG   librarian.replicator    kafka/repository.go:110 Message delivered       {"topic": "postgres-changes", "partition": 0, "offset": 54}
```

The row is now available in your Kafka topic.

## Features

- Real-time change data capture (CDC)
- Automatic checkpointing and resume
- HTTP health check endpoint (`:8080`)
- Configurable batch sizes and flush intervals

## Stats Server

Librarian includes a built-in HTTP stats server that provides real-time visibility into your replication pipelines. Unlike traditional JVM-based connectors that expose generic JVM metrics, Librarian's stats are **data-pipeline centered**, giving you direct insight into what matters for debugging and monitoring your data flows.

### Value 

- **Pipeline-First Metrics**: See exactly how many events have been processed, bytes transferred, and error counts—not garbage collection stats
- **Direct Debugging**: Quickly identify connection issues, event errors, or stalled replicators without parsing logs
- **Zero Configuration**: Stats server starts automatically on port 8080 with every replicator
- **Lightweight**: JSON API with sub-millisecond response times

### API Endpoints

#### GET `/api/v1/replicators`

Returns detailed statistics for all active replicators:

```bash
curl -s localhost:8080/api/v1/replicators | jq .
```

```json
{
  "count": 1,
  "replicators": [
    {
      "id": "mongodb.test.users",
      "state": "streaming",
      "stats": {
        "source": {
          "total_events": 0,
          "total_bytes": 0,
          "last_event_at": "0001-01-01T00:00:00Z",
          "last_connect_at": "2025-11-20T08:32:03.197972-05:00",
          "connection_healthy": true,
          "connection_retries": 1,
          "event_error_count": 0,
          "source_specific": {
            "collection": "users",
            "database": "test"
          }
        },
        "target": {
          "total_events": 0,
          "connection_healthy": true,
          "connection_retries": 0,
          "event_error_count": 0,
          "last_write_at": "0001-01-01T00:00:00Z",
          "write_error_count": 0,
          "target_specific": {
            "brokers": "localhost:9092",
            "topic": "order-events"
          }
        },
        "replicator": {
          "started_at": "2025-11-20T08:32:03.142297-05:00",
          "uptime_seconds": 3,
          "state": "streaming",
          "checkpoint_count": 0,
          "last_checkpoint_at": "0001-01-01T00:00:00Z",
          "signals_received": 0
        }
      }
    }
  ]
}
```

### Understanding the Stats

The stats API provides three levels of detail:

- **Source Stats**: Connection health, event counts, bytes transferred, and source-specific metadata (database, collection, table)
- **Target Stats**: Write performance, connection status, error counts, and target-specific metadata (brokers, topics, buckets)
- **Replicator Stats**: Overall state, uptime, checkpoint frequency, and signal handling

Use these stats to:
- Monitor replication lag by checking `last_event_at` and `last_write_at`
- Detect connection issues via `connection_healthy` and `connection_retries`
- Track error rates with `event_error_count` and `write_error_count`
- Verify replicator state transitions and uptime

## Debezium Message Compatibility

Librarian produces change events in a Debezium-compatible message format, allowing you to use existing Debezium consumers and downstream tools without modification.

### Message Structure

Each change event follows the standard Debezium envelope structure:

```json
{
  "payload": {
    "before": {...},
    "after": {...},
    "source": {
      "version": "1.0.0",
      "connector": "librarian",
      "name": "replicator-id",
      "ts_ms": 1234567890,
      "snapshot": "false",
      "db": "database-name",
      "schema": "schema-name",
      "table": "table-name",
      "lsn": 12345,
      "txId": 678
    },
    "op": "c",
    "ts_ms": 1234567890,
    "transaction": {
      "id": "tx-id",
      "total_order": 1,
      "data_collection_order": 1
    }
  }
}
```

### Operation Codes

Librarian uses standard Debezium operation codes:

- `c` - Create/Insert
- `u` - Update
- `d` - Delete
- `r` - Read (snapshot)

### Source Metadata

The `source` field contains metadata about the origin of the change event:

- **MongoDB**: Includes resume token information, collection name, and timestamp
- **PostgreSQL**: Includes LSN (Log Sequence Number), transaction ID, schema, and table name

### Compatibility

Because Librarian produces Debezium-compatible messages, you can:

- Use existing Debezium consumers without modification
- Leverage Debezium-aware tools and frameworks (e.g., Kafka Connect transformations)
- Mix Librarian and Debezium connectors in the same pipeline
- Apply Debezium-specific filtering and routing logic

## Direct Source Consumption

For advanced use cases, you can consume change events directly from the Postgres source without using the full replicator. This gives you fine-grained control over event processing while still leveraging Librarian's PostgreSQL logical replication handling.

### Example: Consuming PostgreSQL Events

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/turbolytics/librarian/pkg/postgres"
	"github.com/turbolytics/librarian/pkg/replicator"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()

	// Initialize logger
	logger, _ := zap.NewDevelopment()

	// Parse connection URL
	// Note: You must create the publication manually first:
	//   CREATE PUBLICATION librarian_pub_test FOR ALL TABLES;
	sourceURL, err := url.Parse("postgres://test:test@localhost:5432/test?slot=librarian_slot&sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	// Initialize the source
	source, err := postgres.NewSource(sourceURL, logger)
	if err != nil {
		log.Fatal(err)
	}

	// Connect to PostgreSQL (establishes replication connection, creates slot, etc.)
	if err := source.Connect(ctx, nil); err != nil {
		log.Fatal(err)
	}
	defer source.Disconnect(ctx)

	logger.Info("Connected to PostgreSQL, streaming changes...")

	// Read events from the stream
	for {
		event, err := source.Next(ctx)

		// ErrNoEventsFound is returned when the receive timeout expires
		// This is normal and just means no changes happened in the last second
		if errors.Is(err, replicator.ErrNoEventsFound) {
			continue
		}

		if err != nil {
			logger.Error("Error reading event", zap.Error(err))
			time.Sleep(1 * time.Second)
			continue
		}

		// Process the event
		fmt.Printf("Received %s event on %s.%s\n",
			event.Payload.Op,
			event.Payload.Source.Schema,
			event.Payload.Source.Table)

		fmt.Printf("  Before: %+v\n", event.Payload.Before)
		fmt.Printf("  After:  %+v\n", event.Payload.After)
		fmt.Printf("  LSN:    %d\n", event.Payload.Source.Lsn)
	}
}
```

### Key Considerations

- **Publication Setup**: You must manually create a PostgreSQL publication before connecting. The source does not auto-create publications.
- **Replication Slot**: The source automatically creates a replication slot if one doesn't exist for the given slot name.
- **No Events != Error**: The `Next()` method returns `ErrNoEventsFound` when no events are available within the timeout (1 second). This is normal behavior in streaming scenarios.
- **Heartbeats**: The source automatically handles PostgreSQL keepalive messages and sends standby status updates.
- **Connection Management**: Use `defer source.Disconnect(ctx)` to ensure proper cleanup of replication connections.
- **Event Filtering**: At this level, you receive all change events from tables in the publication. Apply your own filtering logic as needed.

### When to Use Direct Consumption

Direct source consumption is useful when:
- Building custom event processing pipelines
- Implementing specialized transformation logic
- Integrating with non-standard targets
- Requiring fine-grained control over checkpointing and recovery
- Prototyping or debugging replication behavior

For most use cases, the full `replicate` command provides better ergonomics and built-in checkpointing.

## License

MIT
