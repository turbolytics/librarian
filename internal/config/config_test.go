package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewArchiverFromFile(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		librarian, err := NewLibrarianFromFile("../../dev/examples/postgres.archiver.yml")
		assert.NoError(t, err)
		assert.NotNil(t, librarian)
		assert.Equal(t, "postgres-example-1", librarian.Archiver.Name)
	})
}
