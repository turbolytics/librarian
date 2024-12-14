package archiver

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/turbolytics/librarian/internal"
	"github.com/turbolytics/librarian/internal/catalog"
	"io"
	"time"

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

func WithRepository(repository internal.Repository) Option {
	return func(a *Archiver) {
		a.repository = repository
	}
}

type Archiver struct {
	logger     *zap.Logger
	source     *sql.Source
	preserver  *parquet.Preserver
	repository internal.Repository
}

func (a *Archiver) Close(ctx context.Context) error {
	return a.source.Close(ctx)
}

func (a *Archiver) Snapshot(ctx context.Context, id uuid.UUID) error {
	// Initialize the catalog
	clog := catalog.Catalog{
		ID:        id,
		StartTime: time.Now().UTC(),
		Source:    a.source.Name(),
	}

	// 1. Collect data from source
	snapshot, err := a.source.Snapshot(ctx)
	if err != nil {
		return err
	}
	defer snapshot.Close()

	expectedRows, err := snapshot.Count(ctx)

	if err != nil {
		return err
	}
	clog.NumSourceRecords = expectedRows

	a.logger.Info(
		"source snapshot",
		zap.String("source", a.source.Name()),
		zap.Int("expected_rows", expectedRows),
	)

	if err := snapshot.Init(ctx); err != nil {
		return err
	}

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

	// 3. Signal that processing is done.
	if err = a.preserver.Flush(ctx); err != nil {
		return err
	}

	// 4. Update and flush catalog
	clog.NumRecordsProcessed = a.preserver.NumRecordsProcessed()
	clog.Completed = true
	clog.EndTime = time.Now().UTC()
	a.logger.Info("catalog", zap.Any("catalog", clog))

	bs, err := json.Marshal(clog)
	if err != nil {
		return err
	}

	return a.repository.Write(
		ctx,
		"catalog.json",
		bytes.NewReader(bs),
	)
}

func New(opts ...Option) *Archiver {
	var a Archiver
	for _, opt := range opts {
		opt(&a)
	}
	return &a
}
