package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"

	"github.com/turbolytics/librarian/internal"
)

type Source struct {
	Schema string
	Table  string
	Query  string

	db     *sql.DB
	logger *zap.Logger
}

func (s *Source) Name() string {
	return fmt.Sprintf("%s.%s", s.Schema, s.Table)
}

func (s *Source) Close(ctx context.Context) error {
	return s.db.Close()
}

type Snapshot struct {
	logger  *zap.Logger
	rows    *sql.Rows
	columns []string
	query   string

	tx *sql.Tx
}

// Count returns the expected count of records in the snapshot
// TODO this should be executed in the same transaction that the
// actual snapshot is executed in for correctness.
func (s *Snapshot) Count(ctx context.Context) (int, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM (%s);`, s.query)
	row := s.tx.QueryRowContext(ctx, query)
	var c int
	err := row.Scan(&c)
	return c, err
}

func (s *Snapshot) Query() string {
	return s.query
}

// Init begins the snapshot process. At this point the only valid
// subsequent calls are Next() until the snapshot is closed.
func (s *Snapshot) Init(ctx context.Context) error {
	s.logger.Info("taking snapshot", zap.String("query", s.query))
	rows, err := s.tx.QueryContext(ctx, s.query)
	if err != nil {
		return err
	}

	cts, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	columnTypes := make([]string, len(cts))
	dbTypes := make([]string, len(cts))
	for _, ct := range cts {
		columnTypes = append(columnTypes, ct.ScanType().Name())
		dbTypes = append(dbTypes, ct.DatabaseTypeName())
	}

	fields, err := rows.Columns()
	if err != nil {
		return err
	}

	columns := make([]string, len(fields))
	for i, name := range fields {
		columns[i] = string(name)
	}

	s.rows = rows
	s.columns = columns
	return nil
}

func (s *Snapshot) Close() error {
	var wErr error
	if err := s.rows.Close(); err != nil {
		wErr = errors.Join(wErr, err)
	}
	if err := s.tx.Commit(); err != nil {
		wErr = errors.Join(wErr, err)
	}

	return wErr
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		logger: s.logger,
		query:  s.Query,
		tx:     tx,
	}, nil
}

type SourceOption func(*Source)

func WithSchema(schema string) SourceOption {
	return func(s *Source) {
		s.Schema = schema
	}
}

func WithLogger(logger *zap.Logger) SourceOption {
	return func(s *Source) {
		s.logger = logger
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
		db: db,
	}

	for _, opt := range opts {
		opt(&s)
	}

	if s.Query == "" {
		s.Query = fmt.Sprintf("SELECT * FROM %s.%s", s.Schema, s.Table)
	}

	return &s
}
