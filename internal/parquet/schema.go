package parquet

import (
	"fmt"
	"github.com/turbolytics/librarian/internal"
	"strings"
	"time"
)

type Field struct {
	Name          string
	Type          string
	ConvertedType string
}

type Schema []Field

func (s Schema) ToGoParquetSchema() []string {
	schema := make([]string, len(s))
	for i, field := range s {
		parts := []string{
			fmt.Sprintf("name=%s", field.Name),
			fmt.Sprintf("type=%s", field.Type),
		}
		if field.ConvertedType != "" {
			parts = append(parts, fmt.Sprintf("convertedtype=%s", field.ConvertedType))
		}
		schema[i] = strings.Join(parts, ", ")
	}

	return schema
}

func (s Schema) RecordToParquetRow(r *internal.Record) ([]any, error) {
	if len(s) != r.Len() {
		return nil, fmt.Errorf(
			"schema and record fields mismatch: schema has %d fields, record has %d fields",
			len(s),
			r.Len(),
		)
	}

	row := make([]any, len(s))
	values := r.Values()

	for i, field := range s {
		// apply the mapper functions
		row[i] = values[i]
		switch field.ConvertedType {
		case "TIMESTAMP_MICROS":
			row[i] = values[i].(time.Time).UnixMicro()
		}
	}

	return row, nil
}
