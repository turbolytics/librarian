package archiver

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/turbolytics/librarian/internal/integrations/mongo"
	"github.com/turbolytics/librarian/internal/replicator"
	"go.uber.org/zap"
)

func newReplicateCommand() *cobra.Command {
	var sourceURL string

	var cmd = &cobra.Command{
		Use:   "replicate",
		Short: "Replicates data from source to target",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewDevelopment()
			defer logger.Sync()
			l := logger.Named("librarian.replicator")
			l.Info("starting replicator!")

			l.Info("replicating data...")
			u, err := url.Parse(sourceURL)
			if err != nil {
				return fmt.Errorf("invalid source URL: %w", err)
			}

			var source replicator.Source
			// initialize the source based on connection string protocol
			switch u.Scheme {
			case "mongodb":
				l.Info("initializing MongoDB source", zap.String("url", sourceURL))
				source, err = mongo.NewSource(cmd.Context(), u, l)
				if err != nil {
					return fmt.Errorf("failed to create MongoDB source: %w", err)
				}
			default:
				return fmt.Errorf("unsupported source protocol: %s", u.Scheme)
			}

			r, err := replicator.New(
				replicator.WithSource(source),
				replicator.WithLogger(l),
			)
			if err != nil {
				return fmt.Errorf("failed to create replicator: %w", err)
			}

			if err := r.Run(cmd.Context()); err != nil {
				return fmt.Errorf("replicator failed to start: %w", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&sourceURL, "source", "s", "", "Source URL for replication (e.g., mongodb://user:pass@host/db)")
	cmd.MarkFlagRequired("source")
	return cmd
}
