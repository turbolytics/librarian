package replicator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type Signal string

const (
	SignalPause   Signal = "pause"
	SignalResume  Signal = "resume"
	SignalStop    Signal = "stop"
	SignalRestart Signal = "restart"
)

var (
	// ErrNoEventsFound is returned when no events are found in the source
	ErrNoEventsFound = errors.New("no events found")
)

type TargetOptions struct {
	FlushTimeout time.Duration
}

type SourceOptions struct {
	// interval to poll the source when no events are found
	// source.checkpoint.batch_size
	CheckpointBatchSize int

	// source.empty.poll_interval
	EmptyPollInterval time.Duration
	/*
	   - source.mongodb.batch_size
	   - source.mongodb.starting_position
	   - source.mongodb.resume_token
	   - source.mongodb.max_await_time
	   - source.mongodb.full_document
	*/
}

type Source interface {
	Connect(context.Context, *Checkpoint) error
	Disconnect(context.Context) error
	Next(ctx context.Context) (Event, error)
	Stats() SourceStats
}

type Target interface {
	Close(ctx context.Context) error
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error
	Write(ctx context.Context, event Event) error
	Flush(ctx context.Context) error
	Stats() TargetStats
}

type Replicator struct {
	Checkpointer  Checkpointer
	Source        Source
	SourceOptions SourceOptions
	State         *FSM
	Target        Target
	TargetOptions TargetOptions

	ID string

	// Control channel for receiving signals
	controlChan    chan Signal
	lastCheckpoint *Checkpoint
	logger         *zap.Logger
	stats          Stats
}

type ReplicatorOption func(*Replicator)

func WithCheckpointer(checkpointer Checkpointer) ReplicatorOption {
	return func(r *Replicator) {
		r.Checkpointer = checkpointer
	}
}

func WithSourceOptions(sourceOptions SourceOptions) ReplicatorOption {
	return func(r *Replicator) {
		r.SourceOptions = sourceOptions
	}
}

func WithID(id string) ReplicatorOption {
	return func(r *Replicator) {
		r.ID = id
	}
}

func WithSource(source Source) ReplicatorOption {
	return func(r *Replicator) {
		r.Source = source
	}
}

func WithLogger(logger *zap.Logger) ReplicatorOption {
	return func(r *Replicator) {
		r.logger = logger
	}
}

func WithTarget(target Target) ReplicatorOption {
	return func(r *Replicator) {
		r.Target = target
	}
}

func WithTargetOptions(targetOptions TargetOptions) ReplicatorOption {
	return func(r *Replicator) {
		r.TargetOptions = targetOptions
	}
}

func New(opts ...ReplicatorOption) (*Replicator, error) {
	r := &Replicator{
		Checkpointer: &NoopCheckpointer{},
		SourceOptions: SourceOptions{
			EmptyPollInterval: 100 * time.Millisecond,
		},
		logger:      zap.NewNop(),
		controlChan: make(chan Signal, 1), // Buffered to prevent blocking
	}

	for _, opt := range opts {
		opt(r)
	}

	r.State = NewFSM(
		FSMWithInitialState(StateCreated),
		FSMWithLogger(r.logger.Named("fsm")),
	)

	r.logger.Info("Replicator created", zap.String("state", string(r.State.Current())))
	return r, nil
}

/*
3 ways that replicator will be deployed:
- Standalone daemon - User runs a single replicator instance to replicate data from source to target
- Embedded library - Replicator is embedded within another application to provide replication capabilities
- Ephemeral - As part of librarian process
*/

// Run the replicator
func (r *Replicator) Run(ctx context.Context) error {
	if err := r.State.Transition(StateConnecting); err != nil {
		return err
	}

	r.logger.Info("Starting replicator",
		zap.String("state", string(r.State.Current())),
		zap.String("source-options", fmt.Sprintf("%+v", r.SourceOptions)),
		zap.String("target-options", fmt.Sprintf("%+v", r.TargetOptions)),
	)

	// Initialize replicator stats
	r.stats.Replicator.StartedAt = time.Now()
	r.stats.Replicator.State = r.State.Current()

	// load the checkpoint from the Checkpointer
	var checkpoint *Checkpoint
	var err error
	checkpoint, err = r.Checkpointer.Load(ctx, r.ID)
	if err != nil {
		r.State.Transition(StateError)
		return err
	}

	if checkpoint != nil {
		r.logger.Info("Loaded checkpoint",
			zap.String("replicator_id", r.ID),
			zap.String("position", string(checkpoint.Position)),
			zap.Time("timestamp", checkpoint.Timestamp))
	} else {
		r.logger.Info("No checkpoint found, starting fresh",
			zap.String("replicator_id", r.ID))
	}

	// connect to target
	if err := r.Target.Connect(ctx); err != nil {
		r.State.Transition(StateError)
		return err
	}

	// Connect to source
	if err := r.Source.Connect(ctx, checkpoint); err != nil {
		r.State.Transition(StateError)
		return err
	}

	if err := r.State.Transition(StateStreaming); err != nil {
		return err
	}

	r.logger.Info("Replicator started", zap.String("state", string(r.State.Current())))

	var flushTicker *time.Ticker
	var flushChan <-chan time.Time
	if r.TargetOptions.FlushTimeout > 0 {
		flushTicker = time.NewTicker(r.TargetOptions.FlushTimeout)
		flushChan = flushTicker.C
		defer flushTicker.Stop()
	}

	// TODO add flush ticker based on target options
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Context cancelled, stopping replicator")
			r.State.Transition(StateStopped)
			return r.Source.Disconnect(ctx)

		case signal := <-r.controlChan:
			r.stats.Replicator.SignalsReceived++
			if err := r.handleSignal(ctx, signal); err != nil {
				r.logger.Error("Error handling signal",
					zap.String("signal", string(signal)),
					zap.Error(err))
				return err
			}

			// If stopped, exit the loop
			if r.State.Current() == StateStopped {
				return nil
			}

			// If paused, wait for resume or other signals
			if r.State.Current() == StatePaused {
				continue
			}
		case <-flushChan:
			r.logger.Debug("Flushing to target")
			if err := r.Target.Flush(ctx); err != nil {
				r.logger.Error("Error flushing to target", zap.Error(err))
				return err
			}
		default:
			// Only process events if we're in streaming state
			if r.State.Current() != StateStreaming {
				time.Sleep(100 * time.Millisecond) // Brief pause
				continue
			}

			// Process next event with timeout to allow signal checking
			event, err := r.Source.Next(ctx)

			if err == ErrNoEventsFound {
				time.Sleep(r.SourceOptions.EmptyPollInterval)
				continue
			}

			if err != nil {
				r.State.Transition(StateError)
				return err
			}

			// Write event to target
			if err := r.Target.Write(ctx, event); err != nil {
				r.logger.Error("Error writing to target", zap.Error(err))
				r.State.Transition(StateError)
				return err
			}

			if err := r.checkpoint(ctx, event); err != nil {
				r.logger.Error("Error checkpointing", zap.Error(err))
				return err
			}

			// Update replicator stats (source stats are tracked in source itself)
			r.stats.Replicator.State = r.State.Current()
			if !r.stats.Replicator.StartedAt.IsZero() {
				r.stats.Replicator.UptimeSeconds = int64(time.Since(r.stats.Replicator.StartedAt).Seconds())
			}
		}
	}
}

// SendSignal sends a control signal to the replicator
func (r *Replicator) SendSignal(signal Signal) {
	select {
	case r.controlChan <- signal:
		r.logger.Info("Signal sent", zap.String("signal", string(signal)))
	default:
		r.logger.Warn("Control channel full, signal dropped", zap.String("signal", string(signal)))
	}
}

func (r *Replicator) handleSignal(ctx context.Context, signal Signal) error {
	currentState := r.State.Current()

	switch signal {
	case SignalPause:
		if currentState == StateStreaming {
			r.logger.Info("Pausing replicator")
			return r.State.Transition(StatePaused)
		}
		r.logger.Warn("Cannot pause from current state", zap.String("state", string(currentState)))

	case SignalResume:
		if currentState == StatePaused {
			r.logger.Info("Resuming replicator")
			return r.State.Transition(StateStreaming)
		}
		r.logger.Warn("Cannot resume from current state", zap.String("state", string(currentState)))

	case SignalStop:
		r.logger.Info("Stopping replicator")
		r.State.Transition(StateStopped)
		return r.Source.Disconnect(ctx)

	case SignalRestart:
		r.logger.Info("Restarting replicator")

		// Disconnect and reconnect
		if err := r.Source.Disconnect(ctx); err != nil {
			r.logger.Error("Error disconnecting during restart", zap.Error(err))
		}

		if err := r.State.Transition(StateConnecting); err != nil {
			return err
		}

		if err := r.Source.Connect(ctx, r.lastCheckpoint); err != nil {
			r.State.Transition(StateError)
			return err
		}

		return r.State.Transition(StateStreaming)

	default:
		r.logger.Warn("Unknown signal received", zap.String("signal", string(signal)))
	}

	return nil
}

func (r *Replicator) checkpoint(ctx context.Context, latestEvent Event) error {
	if r.Checkpointer == nil || r.SourceOptions.CheckpointBatchSize == 0 {
		return nil
	}

	checkpoint := &Checkpoint{
		ReplicatorID: r.ID,
		Position:     latestEvent.Position,
		Timestamp:    time.Now(),
	}

	if err := r.Checkpointer.Save(ctx, checkpoint); err != nil {
		return err
	}

	r.lastCheckpoint = checkpoint

	// Update checkpoint stats
	r.stats.Replicator.CheckpointCount++
	r.stats.Replicator.LastCheckpointAt = time.Now()

	r.logger.Info("Checkpoint saved",
		zap.String("replicator_id", r.ID),
		zap.String("position", string(checkpoint.Position)),
		zap.Time("timestamp", checkpoint.Timestamp))

	return nil
}

// Stats returns comprehensive stats including source and replicator metrics
func (r *Replicator) Stats() Stats {
	stats := Stats{
		Source:     r.Source.Stats(),
		Target:     r.Target.Stats(),
		Replicator: r.stats.Replicator,
	}

	// Update runtime stats
	stats.Replicator.State = r.State.Current()
	if !stats.Replicator.StartedAt.IsZero() {
		stats.Replicator.UptimeSeconds = int64(time.Since(stats.Replicator.StartedAt).Seconds())
	}

	return stats
}
