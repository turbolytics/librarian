package replicator

import "time"

type SourceStats struct {
	TotalEvents         int64     `json:"total_events"`
	LastEventReceivedAt time.Time `json:"last_event_received_at,omitempty"`

	// Number of successfully processed events
	// FailedEvents        int64
}

type Stats struct {
	Source SourceStats `json:"source,omitempty"`
}
