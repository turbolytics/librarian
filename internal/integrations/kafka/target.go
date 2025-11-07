package kafka

import (
	"context"
	"net/url"

	"github.com/turbolytics/librarian/internal/replicator"
	"go.uber.org/zap"
)

type Repository struct {
}

func NewRepository(ctx context.Context, uri *url.URL, logger *zap.Logger) (*Repository, error) {
	return &Repository{}, nil
}

func (r *Repository) Write(ctx context.Context, event replicator.Event) error {
	return nil
}

func (r *Repository) Close(ctx context.Context) error {
	return nil
}

func (r *Repository) Stats() replicator.TargetStats {
	return replicator.TargetStats{}
}
