package archiver

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/turbolytics/librarian/internal/catalog"
	"os"
	"path/filepath"
	"testing"
	"text/template"
	"time"
)

func TestIntegrationPostgresSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("single file", func(t *testing.T) {
		ctx := context.Background()

		// Start a PostgreSQL container
		pgContainer, err := postgres.Run(ctx,
			"postgres:16",
			postgres.WithInitScripts(filepath.Join("testdata", "init-db.sql")),
			postgres.WithDatabase("test"),
			postgres.WithUsername("test"),
			postgres.WithPassword("test"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(5*time.Second)),
		)
		require.NoError(t, err)

		t.Cleanup(func() {
			if err := pgContainer.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate pgContainer: %s", err)
			}
		})

		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		assert.NoError(t, err)

		// Create a temporary directory for the snapshot
		tempDir, err := os.MkdirTemp("", "snapshot")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Create a YAML config file in the temporary directory
		configPath := filepath.Join(tempDir, "config.yml")
		configTemplate := `
archiver:
  source:
    connection_string: "{{.ConnStr}}"
    schema: public
    table: users
    query: "SELECT id, name, email FROM users"

  repository:
    type: local
    local:
      path: "{{.TempDir}}"

  preserver:
    type: parquet
    parquet:
      schema:
        - name: id
          type: INT64
          repetition_type: OPTIONAL

        - name: name
          type: BYTE_ARRAY
          converted_type: UTF8
          repetition_type: OPTIONAL

        - name: email
          type: BYTE_ARRAY
          converted_type: UTF8
          repetition_type: OPTIONAL`

		tmpl, err := template.New("config").Parse(configTemplate)
		require.NoError(t, err)

		configData := struct {
			ConnStr string
			TempDir string
		}{
			ConnStr: connStr,
			TempDir: tempDir,
		}

		configFile, err := os.Create(configPath)
		require.NoError(t, err)
		defer configFile.Close()

		err = tmpl.Execute(configFile, configData)
		require.NoError(t, err)

		bs, err := os.ReadFile(configPath)
		assert.NoError(t, err)
		fmt.Println(string(bs))

		// Call the Cobra snapshot command entry point
		cmd := newSnapshotCommand()
		cmd.SetArgs([]string{"--config", configPath})
		err = cmd.ExecuteContext(ctx)
		require.NoError(t, err)

		// remove the config file
		os.Remove(configPath)

		// TODO - get the snapshot ID from the command output
		files, err := os.ReadDir(tempDir)
		require.NoError(t, err)
		require.Len(t, files, 1)

		// Get the name of the file
		snapshotID := files[0].Name()
		snapshotPath := filepath.Join(tempDir, snapshotID)

		// Verify the catalog.json written to the temp directory
		catalogPath := filepath.Join(snapshotPath, "catalog.json")
		data, err := os.ReadFile(catalogPath)
		require.NoError(t, err)

		var log catalog.Catalog
		err = json.Unmarshal(data, &log)
		require.NoError(t, err)

		assert.Equal(t, true, log.Success)
		assert.Equal(t, 5, log.NumSourceRecords)
		assert.Equal(t, 5, log.NumRecordsProcessed)
	})
}
