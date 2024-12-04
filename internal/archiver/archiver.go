package archiver

import (
	"context"
	"io"

	"go.uber.org/zap"

	"github.com/turbolytics/librarian/internal/parquet"
	"github.com/turbolytics/librarian/internal/sql"
)

type Option func(*Archiver)

func WithLogger(logger *zap.Logger) Option {
	return func(a *Archiver) {
		a.logger = logger
	}
}

func WithSource(source *sql.Source) Option {
	return func(a *Archiver) {
		a.source = source
	}
}

func WithPreserver(preserver *parquet.Preserver) Option {
	return func(a *Archiver) {
		a.preserver = preserver
	}
}

type Archiver struct {
	logger    *zap.Logger
	source    *sql.Source
	preserver *parquet.Preserver
}

func (a *Archiver) Close(ctx context.Context) error {
	return a.source.Close(ctx)
}

func (a *Archiver) Run(ctx context.Context) error {
	// 1. Collect data from source
	snapshot, err := a.source.Snapshot(ctx)
	if err != nil {
		return err
	}
	defer snapshot.Close()

	// 2. Preserve data to repository
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

		if err := a.preserver.Preserve(ctx, record); err != nil {
			return err
		}
	}

	// 3. Flush data to repository
	return a.preserver.Flush(ctx)
}

func New(opts ...Option) *Archiver {
	var a Archiver
	for _, opt := range opts {
		opt(&a)
	}
	return &a
}
