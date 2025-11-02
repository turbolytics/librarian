package replicator

type Event struct {
	ID   string
	Time int64

	// Envelope for top level fields
	// Operation: Type
	// Operation: Time
	// Primary Keys? Event Target ID?
	Payload interface{}
}

func (e Event) IsZero() bool {
	return e.ID == "" && e.Time == 0 && e.Payload == nil
}
