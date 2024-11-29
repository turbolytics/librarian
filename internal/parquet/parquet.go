package parquet

import (
	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

func ParseCreateTableStmt(stmt string) error {
	_, err := sqlparser.Parse(stmt)
	if err != nil {
		return err
	}

	return nil
}
