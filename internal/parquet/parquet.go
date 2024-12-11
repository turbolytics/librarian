package parquet

import (
	"bytes"
	"context"
	"github.com/google/uuid"
	"github.com/turbolytics/librarian/internal"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
	"io"
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

	batchSizeNumRecords int

	buf *bytes.Buffer

	logger     *zap.Logger
	repository internal.Repository
	w          *writer.CSVWriter

	numRecordsProcessed int
}

func (p *Preserver) NumRecordsProcessed() int {
	return p.numRecordsProcessed
}

func (p *Preserver) flush(ctx context.Context, r io.Reader) error {

	file := uuid.New().String() + ".parquet"

	p.logger.Debug(
		"flushing parquet file",
		zap.String("file", file),
	)
	return p.repository.Write(ctx, file, r)
}

func (p *Preserver) initWriter() error {
	p.buf = &bytes.Buffer{}

	var err error
	p.w, err = writer.NewCSVWriterFromWriter(
		p.Schema.ToGoParquetSchema(),
		p.buf,
		4,
	)

	return err
}

// Preserve serializes a record to a parquet file.
func (p *Preserver) Preserve(ctx context.Context, record *internal.Record) error {
	row, err := p.Schema.RecordToParquetRow(record)
	if err != nil {
		return err
	}

	p.numRecordsProcessed++
	if err := p.w.Write(row); err != nil {
		return err
	}

	if p.batchSizeNumRecords > 0 && ((p.numRecordsProcessed % p.batchSizeNumRecords) == 0) {
		if err := p.Flush(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (p *Preserver) Flush(ctx context.Context) error {
	// Stop the current writer, it will be re-initialized
	if err := p.w.WriteStop(); err != nil {
		return err
	}

	// Copy the buffer, so other writers can continue concurrently
	part := &bytes.Buffer{}
	if _, err := io.Copy(part, p.buf); err != nil {
		return err
	}

	// Reinitialize the writer for the next batch
	if err := p.initWriter(); err != nil {
		return err
	}

	if part.Len() == 0 {
		return nil
	}

	return p.flush(ctx, part)
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
		p.batchSizeNumRecords = batchSizeNumRecords
	}
}

func New(opts ...Option) (*Preserver, error) {
	p := &Preserver{
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(p)
	}

	if err := p.initWriter(); err != nil {
		return nil, err
	}

	p.logger.Debug(
		"parquet preserver initialized",
		zap.Any("schema", p.Schema),
		zap.Any("go-parquet", p.Schema.ToGoParquetSchema()),
	)
	return p, nil
}
