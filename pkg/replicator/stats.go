package replicator

import "time"

type SourceStats struct {
	TotalEvents       int64     `json:"total_events"`
	TotalBytes        int64     `json:"total_bytes"`
	LastEventAt       time.Time `json:"last_event_at"`
	LastConnectAt     time.Time `json:"last_connect_at"`
	ConnectionHealthy bool      `json:"connection_healthy"`
	ConnectionRetries int64     `json:"connection_retries"`
	EventErrorCount   int64     `json:"event_error_count"`
	LastError         string    `json:"last_error,omitempty"`

	// Source-specific metrics
	SourceSpecific map[string]interface{} `json:"source_specific,omitempty"`
}

type TargetStats struct {
	TotalEvents       int64     `json:"total_events"`
	ConnectionHealthy bool      `json:"connection_healthy"`
	ConnectionRetries int64     `json:"connection_retries"`
	EventErrorCount   int64     `json:"event_error_count"`
	LastError         string    `json:"last_error,omitempty"`
	LastWriteAt       time.Time `json:"last_write_at"`
	WriteErrorCount   int       `json:"write_error_count"`

	// Target-specific metrics
	TargetSpecific map[string]interface{} `json:"target_specific,omitempty"`
}

type ReplicatorStats struct {
	StartedAt        time.Time `json:"started_at"`
	UptimeSeconds    int64     `json:"uptime_seconds"`
	State            State     `json:"state"`
	CheckpointCount  int64     `json:"checkpoint_count"`
	LastCheckpointAt time.Time `json:"last_checkpoint_at"`
	SignalsReceived  int64     `json:"signals_received"`
}

type Stats struct {
	Source     SourceStats     `json:"source,omitempty"`
	Target     TargetStats     `json:"target,omitempty"`
	Replicator ReplicatorStats `json:"replicator"`
}
