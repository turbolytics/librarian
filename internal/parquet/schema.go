package parquet

import (
	"fmt"
	"github.com/turbolytics/librarian/internal"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Field struct {
	Name           string
	Type           string
	ConvertedType  string
	RepetitionType string
	Scale          *int
	Precision      *int
	Length         *int
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
		if field.RepetitionType != "" {
			parts = append(parts, fmt.Sprintf("repetitiontype=%s", field.RepetitionType))
		}
		if field.Scale != nil {
			parts = append(parts, fmt.Sprintf("scale=%d", *field.Scale))
		}
		if field.Precision != nil {
			parts = append(parts, fmt.Sprintf("precision=%d", *field.Precision))
		}
		if field.Length != nil {
			parts = append(parts, fmt.Sprintf("length=%d", *field.Length))
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

		// What could go wrong :joy:

		// The goal is to make SQL data types work with parquet data types
		// There are logical SQL Datatypes (such as DECIMAL) backed by go types (float64)
		// that need to be converted to parquet data types.
		// TODO create a table of type conversions
		switch field.ConvertedType {
		case "DATE":
			if values[i] != nil {
				row[i] = int32(values[i].(time.Time).Unix())
			}
		case "DECIMAL":
			fmt.Printf(
				"name=%q value=%q type=%q\n",
				field.Name,
				values[i],
				reflect.TypeOf(values[i]),
			)
			switch v := values[i].(type) {
			case float64:
				str1 := strconv.FormatFloat(
					v,
					'f',
					*field.Precision,
					64,
				)
				row[i] = str1
			}
		case "TIMESTAMP_MICROS":
			row[i] = values[i].(time.Time).UnixMicro()
		}
	}

	return row, nil
}
