package replicator

// Operation represents Debezium operation codes
type Operation string

const (
	OpCreate Operation = "c" // create/insert
	OpUpdate Operation = "u" // update
	OpDelete Operation = "d" // delete
	OpRead   Operation = "r" // read (snapshot)
)

// EventSource contains metadata about the source of the change event (Debezium format)
type EventSource struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	TsMs      int64  `json:"ts_ms"`
	Snapshot  string `json:"snapshot"`
	Db        string `json:"db"`
	Sequence  string `json:"sequence,omitempty"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	TxId      uint32 `json:"txId,omitempty"`
	Lsn       int64  `json:"lsn,omitempty"`
	Xmin      *int64 `json:"xmin"`
}

// Payload contains the change event data in Debezium format
type Payload struct {
	Before      map[string]interface{} `json:"before"`
	After       map[string]interface{} `json:"after"`
	Source      EventSource            `json:"source"`
	Op          Operation              `json:"op"`
	TsMs        int64                  `json:"ts_ms"`
	Transaction *Transaction           `json:"transaction"`
}

// Transaction contains transaction metadata (optional in Debezium)
type Transaction struct {
	Id                  string `json:"id"`
	TotalOrder          int64  `json:"total_order"`
	DataCollectionOrder int64  `json:"data_collection_order"`
}

// Event represents a Debezium-compatible change data capture event
type Event struct {
	// Schema is optional and contains the schema for the payload
	Schema interface{} `json:"schema,omitempty"`

	// Payload contains the actual change data
	Payload Payload `json:"payload"`

	// Position is used internally for checkpointing (not part of Debezium format)
	Position []byte `json:"-"`
}

func (e Event) IsZero() bool {
	return e.Payload.Source.Table == "" && e.Payload.TsMs == 0
}
