package replicator

import (
	"context"
	"errors"
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
	Connect() error
	Disconnect() error
	Next(ctx context.Context) (Event, error)
	// Close()
	// GetSchema() (Schema, error)
	// GetCheckpoint() (Checkpoint, error)
	// SetCheckpoint(Checkpoint) error
	// Define methods for the Source interface
}

type Replicator struct {
	Source        Source
	SourceOptions SourceOptions
	State         *FSM
	Stats         Stats

	ID string

	// Control channel for receiving signals
	controlChan chan Signal
	logger      *zap.Logger
}

type ReplicatorOption func(*Replicator)

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
		SourceOptions: SourceOptions{
			EmptyPollInterval: 5 * time.Second,
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

	r.logger.Info("Starting replicator", zap.String("state", string(r.State.Current())))

	// Connect to source
	if err := r.Source.Connect(); err != nil {
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

			// Update stats
			r.Stats.Source.TotalEvents++
			r.Stats.Source.LastEventReceivedAt = time.Now()

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

		if err := r.Source.Connect(); err != nil {
			r.State.Transition(StateError)
			return err
		}

		return r.State.Transition(StateStreaming)

	default:
		r.logger.Warn("Unknown signal received", zap.String("signal", string(signal)))
	}

	return nil
}
