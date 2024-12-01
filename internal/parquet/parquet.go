package parquet

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/turbolytics/librarian/internal"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/zap"
)

type Schema []Field

func (s Schema) ToGoParquetSchema() []string {
	schema := make([]string, len(s))
	for i, field := range s {
		parts := []string{
			fmt.Sprintf("name=%s", field.Name),
			fmt.Sprintf("type=%s", field.Type),
		}
		if field.ConvertedType != "" {
			parts = append(parts, fmt.Sprintf("convertedtype=%s", field.ConvertedType))
		}
		schema[i] = strings.Join(parts, ", ")
	}

	fmt.Println("schema", schema)

	return schema
}

type Field struct {
	Name          string
	Type          string
	ConvertedType string
}

type Option func(*Preserver)

type Preserver struct {
	// BatchSizeNumRecords int
	Schema Schema

	w             *writer.CSVWriter
	currentBuffer *bytes.Buffer
	logger        *zap.Logger
}

func (p *Preserver) Preserve(ctx context.Context, record *internal.Record) error {
	p.logger.Debug(
		"preserving record",
		zap.Any("record", record.Map()),
	)
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
	p.w.Write(record.Values())

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

	p.logger.Info("parquet preserver initialized", zap.Any("schema", p.Schema))
	return p
}
