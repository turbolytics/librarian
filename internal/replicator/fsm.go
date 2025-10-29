package replicator

import "fmt"

type State string

const (
	StateCreated      State = "created"
	StateInitializing State = "initializing"
	StateConnecting   State = "connecting"
	StateReady        State = "ready"
	StatePaused       State = "paused"
	StateStopped      State = "stopped"
	StateReconnecting State = "reconnecting"
	// StateDraining State = "draining"
	StateError State = "error"
)

type FSM struct {
	current     State
	Transitions map[State]map[State]struct{}
}

func NewFSM(initial State) *FSM {
	return &FSM{
		current: initial,
		Transitions: map[State]map[State]struct{}{
			StateCreated: {
				StateInitializing: {},
			},
			StateInitializing: {
				StateConnecting: {},
				StateError:      {},
			},
		},
	}
}

func (f *FSM) Current() State {
	return f.current
}

func (f *FSM) CanTransition(to State) bool {
	if _, ok := f.Transitions[f.current][to]; ok {
		return true
	}
	return false
}

func (f *FSM) Transition(to State) error {
	if !f.CanTransition(to) {
		return fmt.Errorf("invalid transition from %s to %s", f.current, to)
	}
	f.current = to
	return nil
}
