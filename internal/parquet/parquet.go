package parquet

import "context"

type Field struct {
	Name          string
	Type          string
	ConvertedType string
}

type Preserver struct {
	BatchSize int
	Schema    []Field
}

func (p *Preserver) Preserve(ctx context.Context) error {
	return nil
}
