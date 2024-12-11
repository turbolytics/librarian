package local

import (
	"context"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
)

type Option func(*Repository)

type Repository struct {
	basePath string
	prefix   string
	logger   *zap.Logger
}

func WithPrefix(prefix string) Option {
	return func(r *Repository) {
		r.prefix = prefix
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(r *Repository) {
		r.logger = logger
	}
}

func New(basePath string, opts ...Option) *Repository {
	r := &Repository{
		basePath: basePath,
	}

	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Repository) Write(ctx context.Context, key string, reader io.Reader) error {
	fullPath := filepath.Join(
		r.basePath,
		r.prefix,
		key,
	)
	r.logger.Info("writing file", zap.String("path", fullPath))

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
	return err
}
