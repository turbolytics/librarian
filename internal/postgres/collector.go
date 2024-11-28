package postgres

import "database/sql"

type Collector struct {
	db *sql.DB
}

func NewCollector(db *sql.DB) *Collector {
	return &Collector{db: db}
}
