package internal

// Record is a struct that contains a set of fields and their corresponding values.
// It is used to represent a row of data from a source.
// Field order is critical for some serializers, so we keep them in a separate slice.
type Record struct {
	fields []string
	values []any
}

func NewRecord(fields []string, values []any) *Record {
	return &Record{
		fields: fields,
		values: values,
	}
}

func (r *Record) Len() int {
	return len(r.fields)
}

func (r *Record) Values() []any {
	return r.values
}

func (r *Record) Map() map[string]any {
	m := make(map[string]any)
	for i, field := range r.fields {
		m[field] = r.values[i]
	}
	return m
}
