package sql

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/turbolytics/librarian/internal"
)

type Source struct {
	DB     *sql.DB
	Schema string
	Table  string
	Query  string
}

func (s *Source) Name() string {
	return fmt.Sprintf("%s.%s", s.Schema, s.Table)
}

// Count returns the expected count of records in the snapshot
// TODO this should be executed in the same transaction that the
// actual snapshot is executed in for correctness.
func (s *Source) Count(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM (%s)`, s.Query)
	row := s.DB.QueryRowContext(ctx, query)
	var c int
	err := row.Scan(&c)
	return c, err
}

func (s *Source) Close(ctx context.Context) error {
	return s.DB.Close()
}

type Snapshot struct {
	rows    *sql.Rows
	columns []string
	query   string
}

func (s *Snapshot) Query() string {
	return s.query
}

func (s *Snapshot) Close() error {
	return s.rows.Close()
}

func (s *Snapshot) Next() (*internal.Record, error) {
	row := s.rows.Next()
	if !row {
		return nil, io.EOF
	}

	values := make([]any, len(s.columns))
	valuePtrs := make([]any, len(s.columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	err := s.rows.Scan(valuePtrs...)
	if err != nil {
		return nil, err
	}

	record := internal.NewRecord(s.columns, values)

	return record, nil
}

func (s *Source) Snapshot(ctx context.Context) (*Snapshot, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s", s.Schema, s.Table)
	rows, err := s.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(fields))
	for i, name := range fields {
		columns[i] = string(name)
	}

	return &Snapshot{
		rows:    rows,
		columns: columns,
		query:   s.Query,
	}, nil
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

func WithQuery(query string) SourceOption {
	return func(s *Source) {
		s.Query = query
	}
}

func NewSource(db *sql.DB, opts ...SourceOption) *Source {
	s := Source{
		DB: db,
	}

	for _, opt := range opts {
		opt(&s)
	}

	if s.Query == "" {
		s.Query = fmt.Sprintf("SELECT * FROM %s.%s", s.Schema, s.Table)
	}

	return &s
}
