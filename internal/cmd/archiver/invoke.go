package archiver

import (
	"github.com/spf13/cobra"

	"github.com/turbolytics/librarian/internal/config"
	"go.uber.org/zap"
)

func newInvokeCommand() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "invoke",
		Short: "Invokes the archival of data. Data is collected from the source and preserved.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewDevelopment()
			defer logger.Sync()
			l := logger.Named("librarian.archiver.invoke")
			l.Info("starting archiver!")

			c, err := config.NewLibrarianFromFile(configPath)
			if err != nil {
				return err
			}

			l.Info("config", zap.Any("config", c))

			archiver, err := config.InitializeArchiver(c)
			if err != nil {
				return err
			}

			l.Info("archiver", zap.Any("archiver", archiver))

			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file")
	cmd.MarkFlagRequired("config")

	return cmd
}
