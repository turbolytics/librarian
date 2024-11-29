package archiver

import (
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

func New(opts ...Option) *Archiver {
	var a Archiver
	for _, opt := range opts {
		opt(&a)
	}
	return &a
}
