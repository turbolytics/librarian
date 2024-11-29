package postgres

import "github.com/jackc/pgx/v5"

type Source struct {
	conn   *pgx.Conn
	schema string
	table  string
}

type SourceOption func(*Source)

func WithSchema(schema string) SourceOption {
	return func(s *Source) {
		s.schema = schema
	}
}

func WithTable(table string) SourceOption {
	return func(s *Source) {
		s.table = table
	}
}

func NewSource(conn *pgx.Conn, opts ...SourceOption) *Source {
	return &Source{conn: conn}
}
