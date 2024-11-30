package archiver

import (
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"

	"github.com/turbolytics/librarian/internal/archiver"
	"github.com/turbolytics/librarian/internal/config"
	"github.com/turbolytics/librarian/internal/postgres"
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
			l := logger.Named("librarian.archiver.invoke")
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

			a := archiver.New(
				archiver.WithSource(source),
				archiver.WithLogger(l),
			)

			defer a.Close(ctx)

			l.Info("archiver", zap.Any("archiver", a))

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
