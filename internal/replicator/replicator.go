package replicator

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
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

	ID     string
	logger *zap.Logger
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
		logger: zap.NewNop(),
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

	// begin consuming the stream
	// consume from status channel as well, which will send signals to pause, stop, or handle errors
	for {
		event, err := r.Source.Next(ctx)
		// check if error is ErrNoEventsFound, if so sleep and continue
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

		// Process the event (e.g., transform and send to target)
		r.logger.Info("Received event", zap.String("event_id", event.ID))
	}
}
