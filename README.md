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

## License

MIT
