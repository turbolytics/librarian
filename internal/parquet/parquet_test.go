package parquet

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateTableToParquet(t *testing.T) {

	t.Run("invalid create table sql", func(t *testing.T) {
		sql := "invalid sql"
		err := ParseCreateTableStmt(sql)
		assert.Error(t, err)
	})

}
