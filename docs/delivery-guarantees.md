# Delivery Guarantees

## Overview

Librarian provides **at-least-once delivery semantics** for all replication operations. This means that every change event from the source database is guaranteed to be delivered to the target at least once. In the event of failures or restarts, some events may be delivered multiple times, but no events will be lost.

## At-Least-Once Delivery Architecture

The at-least-once delivery guarantee is achieved through a carefully orchestrated sequence of operations:

1. **Event Capture**: Change events are captured from the source (MongoDB ChangeStream or Postgres WAL)
2. **Buffered Write**: Events are written to the target (e.g., Kafka producer buffer)
3. **Flush**: Target is flushed to ensure all buffered events are durably persisted
4. **Checkpoint Save**: Only after successful flush, the checkpoint (position/offset) is saved to persistent storage
5. **Source Notification**: Source is notified of the persisted position for upstream acknowledgment

### Critical Invariant

**The checkpoint/offset is NEVER updated until:**
- `Target.Flush()` succeeds (confirming durable persistence)
- AND the checkpoint is saved to local persistent storage

This ensures that if the process crashes at any point before both conditions are met, events will be re-delivered upon restart.

## Checkpoint Strategies by Source Type

### Comparison Table

| Aspect | MongoDB | Postgres |
|--------|---------|----------|
| **Checkpoint Type** | Resume Token (base64-encoded BSON) | WAL LSN (Log Sequence Number) |
| **Storage Location** | Filesystem (JSON file) | Filesystem (JSON file) |
| **State in Source System** | None (stateless from MongoDB's perspective) | Replication Slot on primary server |
| **Position Tracking** | `changeStream.ResumeToken()` | `pglogrepl.LSN` |
| **Resume Mechanism** | `SetResumeAfter(resumeToken)` | `StartReplication(slotName, startLSN)` |
| **Source Acknowledgment** | No-op (resume token already saved locally) | Standby Status Update with `WALFlushPosition` |
| **Upstream State Update** | Implicit on next query | Explicit via `SendStandbyStatusUpdate()` |
| **Failure Recovery** | Resume from last saved token | Resume from last saved LSN |
| **Source State Cleanup** | Automatic (no server-side state) | Replication slot must be manually dropped |

### MongoDB Checkpoint Implementation

**Location**: `pkg/mongo/source.go`

MongoDB uses **resume tokens** from change streams as checkpoints. The resume token is an opaque BSON value that represents a point in the oplog.

#### Resume Token Capture

When an event is processed, the resume token is captured and encoded:

```go
// pkg/mongo/source.go:180-181
token := base64.StdEncoding.EncodeToString(s.changeStream.ResumeToken())
```

The token is attached to the event as the `Position`:

```go
// pkg/mongo/source.go:213-214
return replicator.Event{
    Position: []byte(token),  // Resume token as checkpoint
    Payload: replicator.Payload{...},
}, nil
```

#### Resuming from Checkpoint

When connecting with a saved checkpoint, the resume token is decoded and passed to the change stream:

```go
// pkg/mongo/source.go:74-87
if checkpoint != nil {
    var resumeToken bson.Raw
    resumeTokenBytes, err := base64.StdEncoding.DecodeString(string(checkpoint.Position))
    if err != nil {
        return err
    }
    resumeToken = bson.Raw(resumeTokenBytes)
    opts.SetResumeAfter(resumeToken)
    s.logger.Info("Resuming from checkpoint",
        zap.String("database", s.database),
        zap.String("collection", s.collection),
        zap.Any("resume_token", checkpoint.Position))
}
```

#### Source Notification

For MongoDB, the `Checkpoint()` method is a no-op because:
- MongoDB change streams are stateless from the server's perspective
- The resume token is already durably saved to the filesystem
- On restart, MongoDB will automatically resume from the saved token

```go
// pkg/mongo/source.go:249-260
func (s *Source) Checkpoint(ctx context.Context, checkpoint *replicator.Checkpoint) error {
    // For MongoDB, this is a no-op since the checkpoint (resume token) is already
    // persisted to storage by the Replicator after a successful flush.
    // MongoDB change streams will automatically resume from the saved resume token
    // on reconnection, so we don't need to send any acknowledgment back to MongoDB.
    return nil
}
```

### Postgres Checkpoint Implementation

**Location**: `pkg/postgres/source.go`

Postgres uses **Log Sequence Numbers (LSNs)** to track position in the Write-Ahead Log (WAL). The replication state is maintained both locally and in a replication slot on the primary server.

#### LSN Tracking

Postgres tracks two separate LSNs:

```go
// pkg/postgres/source.go:30-33
currentLSN   pglogrepl.LSN  // Latest LSN received from WAL
persistedLSN pglogrepl.LSN  // Last LSN durably persisted to target
```

- **`currentLSN`**: Updated as WAL messages are received
- **`persistedLSN`**: Updated only after successful flush and checkpoint save

#### LSN Capture

Each change event includes the current LSN as its position:

```go
// Example from handleInsert (pkg/postgres/source.go:257)
Position: []byte(s.currentLSN.String())
```

When a commit message is received, the LSN is updated:

```go
// pkg/postgres/source.go:395
s.currentLSN = msg.CommitLSN
```

#### Replication Slot State

Postgres maintains the replication state in a **replication slot** on the primary server. The slot tracks:
- Slot name (e.g., `librarian_slot`)
- Confirmed flush LSN (last position acknowledged as durable)
- Restart LSN (position where replication will restart)

#### Standby Status Updates

The source regularly sends standby status updates to inform Postgres of replication progress:

```go
// pkg/postgres/source.go:140-157
err := pglogrepl.SendStandbyStatusUpdate(ctx, s.replConn, pglogrepl.StandbyStatusUpdate{
    WALWritePosition: s.currentLSN,   // We've received up to currentLSN
    WALFlushPosition: flushLSN,       // We've flushed up to persistedLSN
    WALApplyPosition: flushLSN,       // We've applied up to persistedLSN
    ClientTime:       time.Now(),
    ReplyRequested:   false,
})
```

**Important**: `WALFlushPosition` is set to `persistedLSN`, NOT `currentLSN`. This ensures Postgres only considers data as flushed after librarian has durably persisted it to the target.

#### Source Notification (Checkpoint Method)

After a successful flush and checkpoint save, the replicator calls `Source.Checkpoint()` to notify Postgres:

```go
// pkg/postgres/source.go:569-610
func (s *Source) Checkpoint(ctx context.Context, checkpoint *replicator.Checkpoint) error {
    if checkpoint == nil {
        return nil
    }

    // Parse the LSN from the checkpoint position
    lsn, err := pglogrepl.ParseLSN(string(checkpoint.Position))
    if err != nil {
        return fmt.Errorf("failed to parse LSN from checkpoint: %w", err)
    }

    // Update the persisted LSN
    s.persistedLSN = lsn

    // Send immediate status update to Postgres with the persisted LSN
    err = pglogrepl.SendStandbyStatusUpdate(ctx, s.replConn, pglogrepl.StandbyStatusUpdate{
        WALWritePosition: s.currentLSN,   // We've received up to currentLSN
        WALFlushPosition: s.persistedLSN, // We've persisted up to persistedLSN
        WALApplyPosition: s.persistedLSN, // We've applied up to persistedLSN
        ClientTime:       time.Now(),
        ReplyRequested:   false,
    })
    if err != nil {
        return fmt.Errorf("failed to send standby status update: %w", err)
    }

    s.logger.Info("Sent standby status update with persisted LSN",
        zap.String("write_lsn", s.currentLSN.String()),
        zap.String("flush_lsn", s.persistedLSN.String()))

    return nil
}
```

This notification updates the replication slot's confirmed flush position, allowing Postgres to:
- Safely clean up old WAL files
- Know which data has been successfully replicated
- Determine restart position in case of connection loss

## Flush Mechanism

**Location**: `pkg/replicator/replicator.go:247-275`

The flush mechanism is the critical component that ensures delivery guarantees:

```go
case <-flushChan:
    r.logger.Debug("Flushing to target")

    // Step 1: Flush to ensure all buffered events are durably written
    if err := r.Target.Flush(ctx); err != nil {
        r.logger.Error("Error flushing to target", zap.Error(err))
        return err
    }

    // Step 2: After successful flush, persist the checkpoint
    if err := r.checkpoint(ctx, r.lastCheckpoint); err != nil {
        r.logger.Error("Error during checkpointing after flush", zap.Error(err))
        return err
    }

    // Step 3: Notify the source that data has been durably persisted
    if err := r.Source.Checkpoint(ctx, r.lastCheckpoint); err != nil {
        r.logger.Error("Error notifying source of checkpoint", zap.Error(err))
        // Non-fatal: checkpoint is already saved, source will sync on next flush
    }
```

### Sequence of Operations

1. **Flush Trigger**: A timer fires (default 5 seconds) or batch size is reached
2. **Target Flush**: `Target.Flush()` blocks until all buffered writes are acknowledged
   - For Kafka: waits for broker acknowledgments
   - For other targets: ensures data is written to durable storage
3. **Checkpoint Save**: Position is atomically saved to filesystem (see below)
4. **Source Notification**: Source is informed of the persisted position
   - For Postgres: sends standby status update with new flush LSN
   - For MongoDB: no-op (position already saved)

### Checkpoint Save Implementation

**Location**: `pkg/replicator/checkpoint.go`

Checkpoints are saved atomically using a write-sync-rename pattern:

```go
// Write to temp file
tempPath := checkpointPath + ".tmp"
if err := os.WriteFile(tempPath, data, 0644); err != nil {
    return err
}

// Sync to disk (fsync)
if file, err := os.OpenFile(tempPath, os.O_RDWR, 0644); err == nil {
    file.Sync()
    file.Close()
}

// Atomic rename
if err := os.Rename(tempPath, checkpointPath); err != nil {
    os.Remove(tempPath)
    return err
}
```

This ensures:
- Writes are atomic (rename is atomic on POSIX systems)
- Data is synced to disk before making it visible
- Partial writes cannot corrupt the checkpoint file

## Offset Update Timing

**Critical**: The checkpoint position is tracked during event processing but ONLY saved after a successful flush.

### Position Tracking

```go
// pkg/replicator/replicator.go:305-309
// Track the latest event position for checkpointing after flush
r.lastCheckpoint = &Checkpoint{
    ReplicatorID: r.ID,
    Position:     event.Position,  // This is ONLY saved AFTER flush
    Timestamp:    time.Now(),
}
```

This checkpoint is held in memory and only persisted when flush succeeds.

### Event Processing Flow

```go
// pkg/replicator/replicator.go:297-309
case event := <-eventChan:
    // Write to target (may be buffered)
    if err := r.Target.Write(ctx, event); err != nil {
        return err
    }

    // Track position (in memory only)
    r.lastCheckpoint = &Checkpoint{
        ReplicatorID: r.ID,
        Position:     event.Position,
        Timestamp:    time.Now(),
    }
```

### Recovery Scenario

If the process crashes between Write and Flush:
1. Events may be in target's buffer but not yet durable
2. Checkpoint still points to previous flush position
3. On restart, events are re-read from source starting at old checkpoint
4. Events are re-delivered to target (at-least-once semantics)

## Target Implementations

Librarian is designed to support multiple target types beyond Kafka. Each target must implement the `Target` interface:

```go
type Target interface {
    Close(ctx context.Context) error
    Connect(ctx context.Context) error
    Disconnect(ctx context.Context) error
    Write(ctx context.Context, event Event) error
    Flush(ctx context.Context) error
    Stats() TargetStats
}
```

### Kafka Target

**Location**: `internal/integrations/kafka/repository.go`

The Kafka target uses the Confluent Kafka producer with the following flush behavior:

```go
// Flush waits for all queued messages to be delivered
func (r *Repository) Flush(ctx context.Context) error {
    outstanding := r.producer.Flush(60000) // 60 second timeout
    if outstanding > 0 {
        return fmt.Errorf("failed to flush all messages, %d messages outstanding", outstanding)
    }
    return nil
}
```

The Kafka producer buffers messages for performance. `Flush()` blocks until:
- All messages are sent to brokers
- Brokers acknowledge receipt (based on `acks` configuration)
- Or timeout occurs (returns error)

### Future Targets

While Kafka is currently the only replication target, librarian is designed to support additional targets such as:
- HTTP/Webhook endpoints
- Message queues (RabbitMQ, NATS, etc.)
- Cloud streams (AWS Kinesis, Google Pub/Sub, Azure Event Hubs)
- Databases (for direct replication)
- Custom targets implementing the Target interface

Each target must implement proper flush semantics to ensure data is durably persisted before returning from `Flush()`.

## Failure Scenarios and Recovery

### Process Crash Before Flush

**Scenario**: Process crashes after writing events to target buffer but before flush

**Recovery**:
1. On restart, load checkpoint from filesystem
2. Resume from saved position (resume token or LSN)
3. Re-read and re-deliver events since last checkpoint
4. Events are delivered at-least-once to target

### Target Flush Failure

**Scenario**: `Target.Flush()` returns error (e.g., Kafka broker down)

**Behavior**:
1. Flush error is returned to main replication loop
2. Process exits with error
3. No checkpoint is saved
4. No source notification is sent

**Recovery**:
1. Orchestrator restarts process
2. Resume from last successful checkpoint
3. Re-attempt delivery of buffered events

### Checkpoint Save Failure

**Scenario**: `Target.Flush()` succeeds but checkpoint save fails

**Behavior**:
1. Checkpoint save error is returned to main loop
2. Process exits with error
3. Source is not notified

**Recovery**:
1. On restart, resume from last saved checkpoint
2. Events since last checkpoint are re-delivered
3. This is safe because flush succeeded - events are already in target

### Source Notification Failure

**Scenario**: Flush and checkpoint save succeed but `Source.Checkpoint()` fails

**Behavior**:
1. Error is logged but not returned (non-fatal)
2. Process continues running
3. Source will be notified on next successful flush

**MongoDB Impact**: None (notification is no-op)

**Postgres Impact**:
- Replication slot's confirmed flush LSN is not updated
- Postgres may retain more WAL files than necessary
- Will be corrected on next successful notification

## Monitoring and Observability

The replicator tracks checkpoint statistics:

```go
r.stats.Replicator.CheckpointCount++
r.stats.Replicator.LastCheckpointAt = time.Now()
```

Key metrics to monitor:
- **Checkpoint frequency**: How often checkpoints are saved
- **Last checkpoint time**: Time since last successful checkpoint
- **Flush duration**: Time taken for target flush operations
- **Checkpoint lag**: Difference between current position and checkpointed position

For Postgres specifically, monitoring the replication slot lag is important:
- `pg_replication_slots.confirmed_flush_lsn`: Last LSN confirmed by librarian
- `pg_current_wal_lsn()`: Current WAL position
- Lag = current WAL LSN - confirmed flush LSN

## Configuration

Key configuration parameters affecting delivery guarantees:

- **Checkpoint Batch Size** (`checkpoint-batch-size`): Number of events between checkpoints
  - Higher values = better performance, larger replay window on restart
  - Lower values = more checkpoints, smaller replay window

- **Flush Interval**: Time between flush operations (default: 5 seconds)
  - Higher values = better throughput, higher latency on restart
  - Lower values = lower restart latency, more overhead

- **Target-Specific Settings**:
  - Kafka `acks`: Configure broker acknowledgment level (default: all)
  - Kafka `linger.ms`: Message batching delay
  - Kafka `buffer.memory`: Producer buffer size

## Best Practices

1. **Checkpoint Directory**: Ensure checkpoint directory is on durable storage
   - Use local disk, not network filesystem
   - Ensure proper fsync support

2. **Replication Slot Management** (Postgres):
   - Monitor replication slot lag
   - Set up alerts for excessive lag
   - Have procedures to drop/recreate slots if needed

3. **Target Configuration**:
   - Configure appropriate timeouts for flush operations
   - Ensure target system can handle replay of recent events (idempotency)
   - Monitor target lag and backpressure

4. **Idempotency**: Design downstream consumers to handle duplicate events
   - Use event IDs for deduplication
   - Make operations idempotent where possible
   - Consider using exactly-once semantics at application level

5. **Testing**: Regularly test failure scenarios
   - Process crashes during replication
   - Target unavailability
   - Checkpoint file corruption
   - Network partitions

## Summary

Librarian's at-least-once delivery guarantee is built on a foundation of:

1. **Separation of concerns**: Current position vs. persisted position
2. **Flush-first semantics**: Checkpoints only after confirmed durability
3. **Atomic checkpoint saves**: Write-sync-rename pattern
4. **Source-specific acknowledgment**: Resume tokens (MongoDB) vs. LSN acknowledgment (Postgres)
5. **Graceful failure handling**: Safe defaults on errors

This architecture ensures that while events may be delivered multiple times in failure scenarios, no events are ever lost during replication.
