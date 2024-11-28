package preserver

import (
	"context"
	"fmt"
)

type Stdout struct{}

func (s *Stdout) Preserve(ctx context.Context, data []byte) error {
	fmt.Println(string(data))
	return nil
}
