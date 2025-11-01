package replicator

import (
	"testing"
)

func TestNewReplicator(t *testing.T) {
	r, err := New()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if r.State.Current() != StateCreated {
		t.Fatalf("expected state %v, got %v", StateCreated, r.State)
	}
}
