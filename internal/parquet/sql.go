package parquet

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
	"strconv"
)

func PostgresSQLParserColumnToField(column *sqlparser.ColumnDefinition) (Field, error) {
	f := Field{
		Name: column.Name.String(),
	}

	switch column.Type.SQLType() {
	case sqltypes.Int32:
		// The go SQL types will return an int64 in the case of an INTEGER
		// The purpose of this parser is to generate correct and seamless
		// parquet schemas from the SQL schema.
		// Encoding an INT32 as and INT64 will ensure that the parquet schema works
		f.Type = "INT64"
	case sqltypes.Int64:
		f.Type = "INT64"
	case sqltypes.VarChar:
		f.Type = "BYTE_ARRAY"
		f.ConvertedType = "UTF8"
	case sqltypes.Text:
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
