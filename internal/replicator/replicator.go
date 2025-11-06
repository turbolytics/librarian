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
	Connect(*Checkpoint) error
	Disconnect() error
	Next(ctx context.Context) (Event, error)
	GetStats() SourceStats
}

type Target interface {
	Close() error
	Write(ctx context.Context, event Event) error
	Flush(ctx context.Context) error
}

type Replicator struct {
	Checkpointer  Checkpointer
	Source        Source
	SourceOptions SourceOptions
	State         *FSM
	Stats         Stats

	ID string

	// Control channel for receiving signals
	controlChan    chan Signal
	lastCheckpoint *Checkpoint
	logger         *zap.Logger
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

func New(opts ...ReplicatorOption) (*Replicator, error) {
	r := &Replicator{
		Checkpointer: &NoopCheckpointer{},
		SourceOptions: SourceOptions{
			EmptyPollInterval: 5 * time.Second,
		},
		logger:      zap.NewNop(),
		controlChan: make(chan Signal, 1), // Buffered to prevent blocking
	}

	for _, opt := range opts {
		opt(r)
	}

	fmt.Println("Replicator Opts:", r.SourceOptions)

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

	r.logger.Info("Starting replicator", zap.String("state", string(r.State.Current())))

	// Initialize replicator stats
	r.Stats.Replicator.StartedAt = time.Now()
	r.Stats.Replicator.State = r.State.Current()

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

	// Connect to source
	if err := r.Source.Connect(checkpoint); err != nil {
		r.State.Transition(StateError)
		return err
	}

	if err := r.State.Transition(StateStreaming); err != nil {
		return err
	}

	r.logger.Info("Replicator started", zap.String("state", string(r.State.Current())))

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Context cancelled, stopping replicator")
			r.State.Transition(StateStopped)
			return r.Source.Disconnect()

		case signal := <-r.controlChan:
			r.Stats.Replicator.SignalsReceived++
			if err := r.handleSignal(signal); err != nil {
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

			if err := r.checkpoint(ctx, event); err != nil {
				r.logger.Error("Error checkpointing", zap.Error(err))
				return err
			}

			// Update replicator stats (source stats are tracked in source itself)
			r.Stats.Replicator.State = r.State.Current()
			if !r.Stats.Replicator.StartedAt.IsZero() {
				r.Stats.Replicator.UptimeSeconds = int64(time.Since(r.Stats.Replicator.StartedAt).Seconds())
			}

			// Process the event
			r.logger.Info("Received event", zap.String("event_id", event.ID))
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

func (r *Replicator) handleSignal(signal Signal) error {
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
		return r.Source.Disconnect()

	case SignalRestart:
		r.logger.Info("Restarting replicator")

		// Disconnect and reconnect
		if err := r.Source.Disconnect(); err != nil {
			r.logger.Error("Error disconnecting during restart", zap.Error(err))
		}

		if err := r.State.Transition(StateConnecting); err != nil {
			return err
		}

		if err := r.Source.Connect(r.lastCheckpoint); err != nil {
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
	r.Stats.Replicator.CheckpointCount++
	r.Stats.Replicator.LastCheckpointAt = time.Now()

	r.logger.Info("Checkpoint saved",
		zap.String("replicator_id", r.ID),
		zap.String("position", string(checkpoint.Position)),
		zap.Time("timestamp", checkpoint.Timestamp))

	return nil
}

// GetStats returns comprehensive stats including source and replicator metrics
func (r *Replicator) GetStats() Stats {
	stats := Stats{
		Source:     r.Source.GetStats(),
		Replicator: r.Stats.Replicator,
	}

	// Update runtime stats
	stats.Replicator.State = r.State.Current()
	if !stats.Replicator.StartedAt.IsZero() {
		stats.Replicator.UptimeSeconds = int64(time.Since(stats.Replicator.StartedAt).Seconds())
	}

	return stats
}
