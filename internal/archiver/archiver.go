package archiver

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/turbolytics/librarian/internal/postgres"
)

type Option func(*Archiver)

func WithLogger(logger *zap.Logger) Option {
	return func(a *Archiver) {
		a.logger = logger
	}
}

func WithSource(source *postgres.Source) Option {
	return func(a *Archiver) {
		a.source = source
	}
}

type Archiver struct {
	logger *zap.Logger
	source *postgres.Source
}

func (a *Archiver) Close(ctx context.Context) error {
	// return a.source.Close(ctx)
	return nil
}

func (a *Archiver) Run(ctx context.Context) error {
	// 1. Collect data from source
	rows, err := a.source.Conn.Query(ctx, "SELECT * FROM "+a.source.Table)
	if err != nil {
		return err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()

	columns := make([]string, len(fields))
	for i, fd := range fields {
		columns[i] = string(fd.Name)
	}

	for rows.Next() {
		// Prepare a slice to hold the raw row values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			return err
		}

		record := make(map[string]any)
		for i, col := range columns {
			record[col] = values[i]
		}

		fmt.Println(record)
	}
	// 2. Persist data to repository
	// 3. Return nil
	return nil
}

func New(opts ...Option) *Archiver {
	var a Archiver
	for _, opt := range opts {
		opt(&a)
	}
	return &a
}
