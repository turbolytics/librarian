package archiver

import (
	"context"
	"fmt"
	"io"

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
	snapshot, err := a.source.Snapshot(ctx)
	if err != nil {
		return err
	}
	defer snapshot.Close()

	for {
		record, err := snapshot.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if record == nil {
			break
		}

		fmt.Println(record)
	}
	// 2. Preserve data using the repository
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
