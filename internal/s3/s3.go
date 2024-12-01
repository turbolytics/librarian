package s3

import (
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
)

type Option func(*S3)

func WithRegion(region string) Option {
	return func(s *S3) {
		s.Region = region
	}
}

func WithBucket(bucket string) Option {
	return func(s *S3) {
		s.Bucket = bucket
	}
}

func WithPrefix(prefix string) Option {
	return func(s *S3) {
		s.Prefix = prefix
	}
}

func WithLogger(l *zap.Logger) Option {
	return func(s *S3) {
		s.logger = l
	}
}

func WithForcePathStyle(forcePathStyle bool) Option {
	return func(s *S3) {
		s.ForcePathStyle = forcePathStyle
	}
}

func WithEndpoint(endpoint string) Option {
	return func(s *S3) {
		s.Endpoint = endpoint
	}
}

type S3 struct {
	logger   *zap.Logger
	uploader *s3manager.Uploader

	Endpoint       string
	Region         string
	Bucket         string
	Prefix         string
	ForcePathStyle bool
}

func New(opts ...Option) *S3 {
	s := &S3{}

	for _, o := range opts {
		o(s)
	}
	return s
}
