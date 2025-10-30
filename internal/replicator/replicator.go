package replicator

import "go.uber.org/zap"

type Source interface {
	Connect() error
	Disconnect() error
	// Stream() (<-chan Event, error)
	// GetSchema() (Schema, error)
	// GetCheckpoint() (Checkpoint, error)
	// SetCheckpoint(Checkpoint) error
	// Define methods for the Source interface
}

type Replicator struct {
	Source Source
	State  *FSM

	logger *zap.Logger
}

type ReplicatorOption func(*Replicator)

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
		State: NewFSM(
			FSMWithInitialState(StateCreated),
		),

		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(r)
	}

	r.logger.Info("Replicator created", zap.String("state", string(r.State.Current())))
	return r, nil
}

/*
3 ways that replicator will be deployed:
- Standalone daemon - User runs a single replicator instance to replicate data from source to target
- Embedded library - Replicator is embedded within another application to provide replication capabilities
- Ephemeral - As part of librarian process
*/

// Start a replicator
func (r *Replicator) Start() error {
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
	return nil
}

// StartServer starts the HTTP command server
func (r *Replicator) StartServer(addr string) error {
	// Initialize HTTP server with routes for monitoring/control
	// This is separate from the replication logic
	return nil
}
