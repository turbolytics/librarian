package s3

import (
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
)

type Option func(*Repository)

func WithRegion(region string) Option {
	return func(r *Repository) {
		r.Region = region
	}
}

func WithBucket(bucket string) Option {
	return func(r *Repository) {
		r.Bucket = bucket
	}
}

func WithPrefix(prefix string) Option {
	return func(r *Repository) {
		r.Prefix = prefix
	}
}

func WithLogger(l *zap.Logger) Option {
	return func(r *Repository) {
		r.logger = l
	}
}

func WithForcePathStyle(forcePathStyle bool) Option {
	return func(r *Repository) {
		r.ForcePathStyle = forcePathStyle
	}
}

func WithEndpoint(endpoint string) Option {
	return func(r *Repository) {
		r.Endpoint = endpoint
	}
}

type Repository struct {
	logger   *zap.Logger
	uploader *s3manager.Uploader

	Endpoint       string
	Region         string
	Bucket         string
	Prefix         string
	ForcePathStyle bool
}

func New(opts ...Option) *Repository {
	r := &Repository{}

	for _, o := range opts {
		o(r)
	}
	return r
}
