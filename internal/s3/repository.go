package s3

import (
	"bufio"
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"go.uber.org/zap"
	"io"
	"path/filepath"
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

	awsConfig := &aws.Config{
		Region:           aws.String(r.Region),
		S3ForcePathStyle: aws.Bool(r.ForcePathStyle),
	}

	if r.Endpoint != "" {
		awsConfig.Endpoint = aws.String(r.Endpoint)
	}

	sess, _ := session.NewSession(awsConfig)
	r.uploader = s3manager.NewUploader(sess)

	return r
}

func (r *Repository) Write(ctx context.Context, key string, reader io.Reader) error {
	objPath := filepath.Join(
		r.Prefix,
		key,
	)

	r.logger.Debug(
		"S3 logger write",
		zap.String("key", key),
		zap.String("prefix", r.Prefix),
		zap.String("object_path", objPath),
		zap.String("bucket", r.Bucket),
	)

	_, err := r.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(r.Bucket),

		// Can also use the `filepath` standard library package to modify the
		// filename as need for an S3 object key. Such as turning absolute path
		// to a relative path.
		Key: aws.String(objPath),

		// The file to be uploaded. io.ReadSeeker is preferred as the Uploader
		// will be able to optimize memory when uploading large content. io.Reader
		// is supported, but will require buffering of the reader's bytes for
		// each part.
		Body: bufio.NewReader(reader),
	})
	return err
}
