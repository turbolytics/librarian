package aws

import (
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
)

type Option func(*S3)

func WithLogger(l *zap.Logger) Option {
	return func(s *S3) {
		s.logger = l
	}
}

type S3 struct {
	logger   *zap.Logger
	uploader *s3manager.Uploader

	Region         string
	Bucket         string
	Prefix         string
	ForcePathStyle bool
}
