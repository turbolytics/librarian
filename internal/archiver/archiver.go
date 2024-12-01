package archiver

import (
	"context"
	"io"

	"go.uber.org/zap"

	"github.com/turbolytics/librarian/internal/parquet"
	"github.com/turbolytics/librarian/internal/postgres"
	"github.com/turbolytics/librarian/internal/s3"
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

func WithPreserver(preserver *parquet.Preserver) Option {
	return func(a *Archiver) {
		a.preserver = preserver
	}
}

func WithRepository(repository *s3.S3) Option {
	return func(a *Archiver) {
		a.repository = repository
	}
}

type Archiver struct {
	logger     *zap.Logger
	source     *postgres.Source
	preserver  *parquet.Preserver
	repository *s3.S3
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

	return a.preserver.Flush(ctx)
}

func New(opts ...Option) *Archiver {
	var a Archiver
	for _, opt := range opts {
		opt(&a)
	}
	return &a
}
