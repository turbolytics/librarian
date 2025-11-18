package replicator

import (
	"fmt"
	"sync"

	"go.uber.org/zap"
)

var (
	ErrInvalidTransition = fmt.Errorf("invalid state transition")
)

type State string

const (
	StateCreated State = "created"
	// StateInitializing State = "initializing"
	StateConnecting   State = "connecting"
	StateStreaming    State = "streaming"
	StatePaused       State = "paused"
	StateStopped      State = "stopped"
	StateReconnecting State = "reconnecting"
	// StateDraining State = "draining"
	StateError State = "error"
)

type FSM struct {
	mu          sync.Mutex
	Transitions map[State]map[State]struct{}

	current State
	logger  *zap.Logger
}

type FSMOption func(*FSM)

func FSMWithLogger(logger *zap.Logger) FSMOption {
	return func(f *FSM) {
		f.logger = logger
	}
}

func FSMWithInitialState(state State) FSMOption {
	return func(f *FSM) {
		f.current = state
	}
}

func NewFSM(opts ...FSMOption) *FSM {
	f := &FSM{
		current: StateCreated,
		logger:  zap.NewNop(),

		Transitions: map[State]map[State]struct{}{
			StateCreated: {
				StateConnecting: {},
				StateStopped:    {}, // Can stop before starting
			},
			StateConnecting: {
				StateStreaming: {},
				StateError:     {},
				StateStopped:   {}, // Can stop during connection
			},
			StateStreaming: {
				StatePaused:       {},
				StateStopped:      {}, // Graceful stop
				StateReconnecting: {},
				StateError:        {},
			},
			StatePaused: {
				StateStreaming: {}, // Resume
				StateStopped:   {}, // Stop while paused
				StateError:     {},
			},
			StateReconnecting: {
				StateStreaming: {},
				StateError:     {},
				StateStopped:   {}, // Give up reconnecting
			},
			StateError: {
				StateConnecting: {}, // Retry connection
				StateStopped:    {}, // Give up and stop
			},
			StateStopped: {
				StateConnecting: {}, // Restart
			},
		},
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func (f *FSM) Current() State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

func (f *FSM) canTransition(to State) bool {
	if _, ok := f.Transitions[f.current][to]; ok {
		return true
	}
	return false
}

func (f *FSM) Transition(to State) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.canTransition(to) {
		f.logger.Error("Invalid state transition",
			zap.String("current", string(f.current)),
			zap.String("from", string(f.current)),
			zap.String("to", string(to)),
		)
		return ErrInvalidTransition
	}
	previous := f.current
	f.current = to

	f.logger.Info("State transitioned",
		zap.String("state", string(f.current)),
		zap.String("from", string(previous)),
	)
	return nil
}
