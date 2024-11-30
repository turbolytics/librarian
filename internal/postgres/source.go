package postgres

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/librarian/internal"
)

type Source struct {
	Conn   *pgx.Conn
	Schema string
	Table  string
}

func (s *Source) Close(ctx context.Context) error {
	return s.Conn.Close(ctx)
}

type snapshot struct {
	rows    pgx.Rows
	columns []string
}

func (s *snapshot) Close() error {
	s.rows.Close()
	return nil
}

func (s *snapshot) Next() (internal.Record, error) {
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

	record := make(map[string]any)
	for i, col := range s.columns {
		record[col] = values[i]
	}

	return record, nil
}

func (s *Source) Snapshot(ctx context.Context) (*snapshot, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s", s.Schema, s.Table)
	rows, err := s.Conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	fields := rows.FieldDescriptions()

	columns := make([]string, len(fields))
	for i, fd := range fields {
		columns[i] = string(fd.Name)
	}

	return &snapshot{
		rows:    rows,
		columns: columns,
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

func NewSource(conn *pgx.Conn, opts ...SourceOption) *Source {
	s := Source{Conn: conn}
	for _, opt := range opts {
		opt(&s)
	}
	return &s
}
