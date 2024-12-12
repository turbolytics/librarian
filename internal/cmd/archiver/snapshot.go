package archiver

import (
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/turbolytics/librarian/internal/s3"
	"path"

	"github.com/turbolytics/librarian/internal"
	"github.com/turbolytics/librarian/internal/archiver"
	"github.com/turbolytics/librarian/internal/config"
	"github.com/turbolytics/librarian/internal/local"
	"github.com/turbolytics/librarian/internal/parquet"
	lsql "github.com/turbolytics/librarian/internal/sql"

	_ "github.com/jackc/pgx/v5/stdlib"

	"go.uber.org/zap"
)

func newInvokeCommand() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Invokes a snapshot. Data is collected from the source and preserved.",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			logger, _ := zap.NewDevelopment()
			defer logger.Sync()
			l := logger.Named("archiver.snaphot")
			l.Info("starting archiver!")

			sid := uuid.Must(uuid.NewUUID())

			c, err := config.NewLibrarianFromFile(configPath)
			if err != nil {
				return err
			}

			db, err := sql.Open("pgx", c.Archiver.Source.ConnectionString)
			if err != nil {
				return err
			}

			defer db.Close()

			if err := db.PingContext(ctx); err != nil {
				return err
			}

			source := lsql.NewSource(
				db,
				lsql.WithSchema(c.Archiver.Source.Schema),
				lsql.WithTable(c.Archiver.Source.Table),
				lsql.WithQuery(c.Archiver.Source.Query),
				lsql.WithLogger(l),
			)

			var repository internal.Repository
			switch c.Archiver.Repository.Type {
			case "local":
				repository = local.New(
					c.Archiver.Repository.LocalConfig.Path,
					local.WithPrefix(sid.String()),
					local.WithLogger(l),
				)
			case "s3":
				repository = s3.New(
					s3.WithLogger(l),
					s3.WithRegion(c.Archiver.Repository.S3Config.Region),
					s3.WithBucket(c.Archiver.Repository.S3Config.Bucket),
					s3.WithEndpoint(c.Archiver.Repository.S3Config.Endpoint),
					s3.WithPrefix(
						path.Join(
							c.Archiver.Repository.S3Config.Prefix,
							sid.String(),
						),
					),
					s3.WithForcePathStyle(c.Archiver.Repository.S3Config.ForcePathStyle),
				)
			default:
				return fmt.Errorf("unknown repository type: %s", c.Archiver.Repository.Type)
			}

			preserver, err := parquet.New(
				parquet.WithLogger(l),
				parquet.WithSchema(
					config.ParquetFields(
						c.Archiver.Preserver.Parquet.Schema,
					),
				),
				parquet.WithRepository(repository),
				parquet.WithBatchSizeNumRecords(c.Archiver.Preserver.BatchSizeNumRecords),
			)

			if err != nil {
				return err
			}

			/*
				s3 := s3.New(
					s3.WithLogger(l),
					s3.WithRegion(c.Archiver.Repository.Region),
					s3.WithBucket(c.Archiver.Repository.Bucket),
					s3.WithPrefix(c.Archiver.Repository.Prefix),
					s3.WithEndpoint(c.Archiver.Repository.Endpoint),
					s3.WithForcePathStyle(c.Archiver.Repository.ForcePathStyle),
				)
			*/

			a := archiver.New(
				archiver.WithLogger(l),
				archiver.WithSource(source),
				archiver.WithPreserver(preserver),
				archiver.WithRepository(repository),
			)

			defer a.Close(ctx)

			if err := a.Snapshot(ctx, sid); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file")
	cmd.MarkFlagRequired("config")

	return cmd
}
