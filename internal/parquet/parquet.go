package parquet

import (
	"bytes"
	"context"
	"github.com/google/uuid"
	"github.com/turbolytics/librarian/internal"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
)

type Option func(*Preserver)

/*
Preservation is the active process of saving things.

The preserver knows about buffering batch sizes and partitions.

The preserver may support different encryption or compression requirements.

Preservers may have a durable buffer, or an ephemeral buffer. It's up to the preserver to manage its buffer.

Preservers blindly call into a repository as many or as little times as they need with concurrency or not.
*/

type Preserver struct {
	// BatchSizeNumRecords int
	Schema Schema

	repository    internal.Repository
	w             *writer.CSVWriter
	currentBuffer *bytes.Buffer
	logger        *zap.Logger

	numRecordsProcessed int
}

func (p *Preserver) NumRecordsProcessed() int {
	return p.numRecordsProcessed
}

func (p *Preserver) Preserve(ctx context.Context, record *internal.Record) error {
	// check if buffer is initialized
	if p.currentBuffer == nil {
		p.currentBuffer = &bytes.Buffer{}

		var err error
		p.w, err = writer.NewCSVWriterFromWriter(
			p.Schema.ToGoParquetSchema(),
			p.currentBuffer,
			4,
		)
		if err != nil {
			return err
		}
	}

	row, err := p.Schema.RecordToParquetRow(record)
	if err != nil {
		return err
	}

	p.numRecordsProcessed++
	return p.w.Write(row)
}

func (p *Preserver) Flush(ctx context.Context) error {
	if p.currentBuffer.Len() == 0 {
		return nil
	}

	if err := p.w.WriteStop(); err != nil {
		return err
	}

	file := uuid.New().String() + ".parquet"

	p.logger.Debug(
		"flushing parquet file",
		zap.String("file", file),
	)
	return p.repository.Write(ctx, file, p.currentBuffer)
}

func WithRepository(repository internal.Repository) Option {
	return func(p *Preserver) {
		p.repository = repository
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(p *Preserver) {
		p.logger = logger
	}
}

func WithSchema(schema []Field) Option {
	return func(p *Preserver) {
		p.Schema = schema
	}
}

func WithBatchSizeNumRecords(batchSizeNumRecords int) Option {
	return func(p *Preserver) {
		// p.BatchSizeNumRecords = batchSizeNumRecords
	}
}

func New(opts ...Option) *Preserver {
	p := &Preserver{
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(p)
	}

	p.logger.Info(
		"parquet preserver initialized",
		zap.Any("schema", p.Schema),
		zap.Any("go-parquet", p.Schema.ToGoParquetSchema()),
	)
	return p
}
