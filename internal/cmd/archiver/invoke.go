package archiver

import (
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"

	"github.com/turbolytics/librarian/internal/archiver"
	"github.com/turbolytics/librarian/internal/config"
	"github.com/turbolytics/librarian/internal/parquet"
	"github.com/turbolytics/librarian/internal/postgres"
	"github.com/turbolytics/librarian/internal/s3"

	"go.uber.org/zap"
)

func newInvokeCommand() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "invoke",
		Short: "Invokes the archival of data. Data is collected from the source and preserved.",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			logger, _ := zap.NewDevelopment()
			defer logger.Sync()
			l := logger.Named("archiver.invoke")
			l.Info("starting archiver!")

			c, err := config.NewLibrarianFromFile(configPath)
			if err != nil {
				return err
			}

			l.Info("config", zap.Any("config", c))

			conn, err := pgx.Connect(
				ctx,
				c.Archiver.Source.ConnectionString,
			)
			if err != nil {
				return err
			}

			if err := conn.Ping(ctx); err != nil {
				return err
			}

			source := postgres.NewSource(
				conn,
				postgres.WithSchema(c.Archiver.Source.Schema),
				postgres.WithTable(c.Archiver.Source.Table),
			)

			preserver := parquet.New(
				parquet.WithLogger(l),
				parquet.WithSchema(
					config.ParquetFields(
						c.Archiver.Preserver.Schema,
					),
				),
				// parquet.WithBatchSizeNumRecords(c.Archiver.Preserver.BatchSizeNumRecords),
			)

			s3 := s3.New(
				s3.WithLogger(l),
				s3.WithRegion(c.Archiver.Repository.Region),
				s3.WithBucket(c.Archiver.Repository.Bucket),
				s3.WithPrefix(c.Archiver.Repository.Prefix),
				s3.WithEndpoint(c.Archiver.Repository.Endpoint),
				s3.WithForcePathStyle(c.Archiver.Repository.ForcePathStyle),
			)

			a := archiver.New(
				archiver.WithLogger(l),
				archiver.WithSource(source),
				archiver.WithPreserver(preserver),
				archiver.WithRepository(s3),
			)

			defer a.Close(ctx)

			if err := a.Run(ctx); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file")
	cmd.MarkFlagRequired("config")

	return cmd
}
