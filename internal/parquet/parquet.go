package parquet

import (
	"bytes"
	"context"

	"github.com/turbolytics/librarian/internal"
	"go.uber.org/zap"
)

type Field struct {
	Name          string
	Type          string
	ConvertedType string
}

type Option func(*Preserver)

type Preserver struct {
	BatchSize int
	Schema    []Field

	currentBuffer *bytes.Buffer
	logger        *zap.Logger
}

func (p *Preserver) Preserve(ctx context.Context, record internal.Record) error {
	p.logger.Debug("preserving record", zap.Any("record", record))
	return nil
}

func (p *Preserver) Flush(ctx context.Context) error {
	p.logger.Info("flushing parquet file")
	return nil
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

func WithBatchSize(batchSize int) Option {
	return func(p *Preserver) {
		p.BatchSize = batchSize
	}
}

func New(opts ...Option) *Preserver {
	p := &Preserver{}
	for _, opt := range opts {
		opt(p)
	}
	return p
}
