package parquet

import (
	"fmt"
	"github.com/turbolytics/librarian/internal"
	"reflect"
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
		pv, err := dbValueToParquetValue(values[i], field)
		if err != nil {
			return nil, err
		}
		row[i] = pv
	}

	return row, nil
}

func dbValueToParquetValue(v any, field Field) (any, error) {
	switch field.ConvertedType {
	case "DATE":
		if v != nil {
			return int32(v.(time.Time).Unix()), nil
		}
	case "DECIMAL":
		fmt.Printf(
			"name=%q value=%q type=%q\n",
			field.Name,
			v,
			reflect.TypeOf(v),
		)
		switch typedv := v.(type) {
		case string:
			bs, err := StringToDECIMAL_BYTE_ARRAY(typedv, *field.Precision, *field.Scale)
			if err != nil {
				return nil, err
			}
			fmt.Printf(
				"name=%q value=%q type=%q\n converted=%q\n",
				field.Name,
				v,
				reflect.TypeOf(v),
				string(bs),
			)
			return string(bs), nil
		}
	case "TIMESTAMP_MICROS":
		return v.(time.Time).UnixMicro(), nil
	default:
		return v, nil
	}

	return nil, nil
}
