package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type Source struct {
	Conn   *pgx.Conn
	Schema string
	Table  string
}

func (s *Source) Close(ctx context.Context) error {
	return s.Conn.Close(ctx)
}

type Record map[string]any

type snapshot struct {
	rows pgx.Rows
}

func (s *snapshot) Next() (Record, error) {
	// return s.rows.Next()
	return nil, nil
}

func (s *Source) Snapshot(ctx context.Context, query string) (*snapshot, error) {
	rows, err := s.Conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return &snapshot{rows: rows}, nil
}

type SourceOption func(*Source)

func WithSchema(schema string) SourceOption {
	return func(s *Source) {
		s.Schema = schema
	}
}

func WithTable(table string) SourceOption {
	return func(s *Source) {
		s.Table = table
	}
}

func NewSource(conn *pgx.Conn, opts ...SourceOption) *Source {
	s := Source{Conn: conn}
	for _, opt := range opts {
		opt(&s)
	}
	return &s
}
