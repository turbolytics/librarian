package archiver

import (
	"fmt"
	"net/url"
	"time"

	"github.com/spf13/cobra"
	"github.com/turbolytics/librarian/internal/integrations/mongo"
	"github.com/turbolytics/librarian/internal/replicator"
	"go.uber.org/zap"
)

func newReplicateCommand() *cobra.Command {
	var sourceURL string
	var replicatorID string

	sourceOpts := replicator.SourceOptions{
		CheckpointBatchSize: 0,
		EmptyPollInterval:   5 * time.Second,
	}

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

			checkpointer := replicator.NewFilesystemCheckpointer("./dev/checkpoints", l)

			r, err := replicator.New(
				replicator.WithSource(source),
				replicator.WithLogger(l),
				replicator.WithID(replicatorID),
				replicator.WithCheckpointer(checkpointer),
				replicator.WithSourceOptions(sourceOpts),
			)
			if err != nil {
				return fmt.Errorf("failed to create replicator: %w", err)
			}

			go func() {
				if err := r.Run(cmd.Context()); err != nil {
					l.Error("replicator error", zap.Error(err))
				}
			}()

			s := replicator.NewServer(l)
			s.RegisterReplicator(r)

			go func() {
				if err := s.Start(cmd.Context(), ":8080"); err != nil {
					l.Error("replicator server error", zap.Error(err))
				}
			}()

			<-cmd.Context().Done()

			return nil
		},
	}

	cmd.Flags().IntVar(&sourceOpts.CheckpointBatchSize, "source-checkpoint-batch-size", 0, "Batch size for checkpointing. 0 disables checkpointing")
	cmd.Flags().StringVarP(&sourceURL, "source", "s", "", "Source URL for replication (e.g., mongodb://user:pass@host/db)")
	cmd.Flags().StringVarP(&replicatorID, "id", "i", "", "ID of the replicator instance")
	cmd.MarkFlagsRequiredTogether("source", "id")
	return cmd
}
