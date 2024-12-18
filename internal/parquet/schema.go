package parquet

import (
	"fmt"
	"github.com/turbolytics/librarian/internal"
	"github.com/xitongsys/parquet-go/types"
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
			t := v.(time.Time)
			epoch := time.Unix(0, 0)
			duration := t.Sub(epoch)
			return int32(duration.Hours() / 24), nil
		}
	case "DECIMAL":
		switch typedv := v.(type) {
		case string:
			// map DECIMAL (string) to an Integer
			i, err := strconv.ParseInt(strings.Replace(typedv, ".", "", -1), 10, 64)
			if err != nil {
				return nil, err
			}
			return i, nil
		}
	case "TIME_MILLIS":
		return types.TimeToTIME_MILLIS(v.(time.Time), false), nil
	case "TIME_MICROS":
		return types.TimeToTIME_MICROS(v.(time.Time), false), nil
	case "TIMESTAMP_MILLIS":
		return types.TimeToTIMESTAMP_MILLIS(v.(time.Time), false), nil
	case "TIMESTAMP_MICROS":
		return types.TimeToTIMESTAMP_MICROS(v.(time.Time), false), nil
	default:
		return v, nil
	}

	return nil, nil
}
