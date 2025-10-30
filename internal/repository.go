package internal

import (
	"context"
	"io"
)

type Repository interface {
	Write(ctx context.Context, path string, reader io.Reader) error
	Flush() error
}
