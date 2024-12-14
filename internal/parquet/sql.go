package parquet

import (
	"fmt"
	"strings"
)

/*
select column_name, data_type, character_maximum_length, column_default, is_nullable
from INFORMATION_SCHEMA.COLUMNS where table_name =
*/

type Column struct {
	Name                   string
	DataType               string
	CharacterMaximumLength *int
	ColumnDefault          *string
	IsNullable             string
}

func (c Column) Field() (Field, error) {
	f := Field{
		Name: c.Name,
	}
	dtParts := strings.Split(c.DataType, " ")
	switch dtParts[0] {
	case "integer":
		f.Type = "INT64"
	case "character":
		f.Type = "BYTE_ARRAY"
		f.ConvertedType = "UTF8"
	case "timestamp":
		f.Type = "INT64"
		f.ConvertedType = "TIMESTAMP_MILLIS"
	case "date":
		f.Type = "INT32"
		f.ConvertedType = "DATE"
	case "numeric":
		f.Type = "INT64"
		f.ConvertedType = "DECIMAL"
	default:
		return Field{}, fmt.Errorf("unsupported data type: %q", c.DataType)
	}

	switch c.IsNullable {
	case "YES":
		f.RepetitionType = "OPTIONAL"
	}

	return f, nil
}

func ColumnsToSchema(columns []Column) (Schema, error) {
	var schema Schema
	for _, column := range columns {
		f, err := column.Field()
		if err != nil {
			return nil, err
		}
		schema = append(schema, f)
	}
	return schema, nil
}
