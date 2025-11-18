package replicator

type Operation string

const (
	OpInsert Operation = "insert"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

type Event struct {
	ID   string    `json:"id,omitempty"`
	Time int64     `json:"time,omitempty"`
	Op   Operation `json:"op,omitempty"`

	// Envelope for top level fields
	// Operation: Type
	// Operation: Time
	// Primary Keys? Event Target ID?
	Payload interface{} `json:"payload,omitempty"`

	Position []byte `json:"position,omitempty"`
}

func (e Event) IsZero() bool {
	return e.ID == "" && e.Time == 0 && e.Payload == nil
}
