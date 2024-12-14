package parquet

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
	"strconv"
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

func PostgresSQLParserColumnToField(column *sqlparser.ColumnDefinition) (Field, error) {
	f := Field{
		Name: column.Name.String(),
	}

	switch column.Type.SQLType() {
	case sqltypes.Int64:
		f.Type = "INT64"
	case sqltypes.VarChar:
		f.Type = "BYTE_ARRAY"
		f.ConvertedType = "UTF8"
	case sqltypes.Timestamp:
		f.Type = "INT64"
		f.ConvertedType = "TIMESTAMP_MILLIS"
	case sqltypes.Date:
		f.Type = "INT32"
		f.ConvertedType = "DATE"
	case sqltypes.Decimal:
		scale, err := strconv.Atoi(string(column.Type.Scale.Val))
		if err != nil {
			return Field{}, err
		}
		length, err := strconv.Atoi(string(column.Type.Length.Val))
		if err != nil {
			return Field{}, fmt.Errorf("invalid length value: %v", err)
		}

		f.Type = "INT64"
		f.ConvertedType = "DECIMAL"
		f.Scale = &scale
		f.Length = &length
	default:
		return Field{}, fmt.Errorf("unsupported data type: %q", column.Type.SQLType())
	}

	if column.Type.NotNull {
		f.RepetitionType = "REQUIRED"
	} else {
		f.RepetitionType = "OPTIONAL"
	}

	return f, nil
}
