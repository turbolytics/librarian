package archiver

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/turbolytics/librarian/internal/integrations/kafka"
	"github.com/turbolytics/librarian/pkg/mongo"
	"github.com/turbolytics/librarian/pkg/postgres"
	"github.com/turbolytics/librarian/pkg/replicator"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func newReplicateCommand() *cobra.Command {
	var sourceURL string
	var targetURL string
	var replicatorID string

	sourceOpts := replicator.SourceOptions{
		CheckpointBatchSize: 0,
		EmptyPollInterval:   500 * time.Millisecond,
	}

	targetOpts := replicator.TargetOptions{
		FlushTimeout: 5 * time.Second,
	}

	var cmd = &cobra.Command{
		Use:   "replicate",
		Short: "Replicates data from source to target",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Configure log level from environment variable, defaulting to INFO
			logLevel := zapcore.InfoLevel
			if levelStr := os.Getenv("LIBRARIAN_LOG_LEVEL"); levelStr != "" {
				switch strings.ToUpper(levelStr) {
				case "DEBUG":
					logLevel = zapcore.DebugLevel
				case "INFO":
					logLevel = zapcore.InfoLevel
				case "WARN", "WARNING":
					logLevel = zapcore.WarnLevel
				case "ERROR":
					logLevel = zapcore.ErrorLevel
				case "DPANIC":
					logLevel = zapcore.DPanicLevel
				case "PANIC":
					logLevel = zapcore.PanicLevel
				case "FATAL":
					logLevel = zapcore.FatalLevel
				default:
					return fmt.Errorf("invalid log level: %s (valid levels: DEBUG, INFO, WARN, ERROR, DPANIC, PANIC, FATAL)", levelStr)
				}
			}

			config := zap.NewDevelopmentConfig()
			config.Level = zap.NewAtomicLevelAt(logLevel)
			logger, err := config.Build()
			if err != nil {
				return fmt.Errorf("failed to create logger: %w", err)
			}
			defer logger.Sync()
			l := logger.Named("librarian.replicator")

			l.Info("starting replicator!")

			l.Info("replicating data...")
			sourceURLParsed, err := url.Parse(sourceURL)
			if err != nil {
				return fmt.Errorf("invalid source URL: %w", err)
			}

			targetURLParsed, err := url.Parse(targetURL)
			if err != nil {
				return fmt.Errorf("invalid target URL: %w", err)
			}

			var source replicator.Source
			// initialize the source based on connection string protocol
			switch sourceURLParsed.Scheme {
			case "mongodb":
				l.Info("initializing MongoDB source", zap.String("url", sourceURL))
				source, err = mongo.NewSource(
					cmd.Context(),
					sourceURLParsed,
					l,
				)
				if err != nil {
					return fmt.Errorf("failed to create MongoDB source: %w", err)
				}

			case "postgres":
				l.Info("initializing Postgres source", zap.String("url", sourceURL))
				source, err = postgres.NewSource(
					sourceURLParsed,
					l,
				)
				if err != nil {
					return fmt.Errorf("failed to create Postgres source: %w", err)
				}
			default:
				return fmt.Errorf("unsupported source protocol: %s", sourceURLParsed.Scheme)
			}

			var target replicator.Target
			switch targetURLParsed.Scheme {
			case "kafka":
				l.Info("initializing kafka target", zap.String("url", targetURL))
				target, err = kafka.NewRepository(
					cmd.Context(),
					targetURLParsed,
					l,
				)
			default:
				return fmt.Errorf("unsupported target protocol: %s", targetURLParsed.Scheme)
			}

			checkpointer := replicator.NewFilesystemCheckpointer("./dev/checkpoints", l)

			r, err := replicator.New(
				replicator.WithLogger(l),
				replicator.WithID(replicatorID),
				replicator.WithCheckpointer(checkpointer),
				replicator.WithSource(source),
				replicator.WithSourceOptions(sourceOpts),
				replicator.WithTarget(target),
				replicator.WithTargetOptions(targetOpts),
			)
			if err != nil {
				return fmt.Errorf("failed to create replicator: %w", err)
			}

			go func() {
				if err := r.Run(cmd.Context()); err != nil {
					l.Error("replicator error", zap.Error(err))
					os.Exit(1)
				}
			}()

			s := replicator.NewServer(l)
			s.RegisterReplicator(r)

			go func() {
				if err := s.Start(cmd.Context(), ":8080"); err != nil {
					l.Error("replicator server error", zap.Error(err))
					os.Exit(1)
				}
			}()

			<-cmd.Context().Done()

			return nil
		},
	}

	cmd.Flags().DurationVar(&targetOpts.FlushTimeout, "target-flush-timeout", 0, "Flush timeout for target. 0 disables flushing")
	cmd.Flags().IntVar(&sourceOpts.CheckpointBatchSize, "source-checkpoint-batch-size", 0, "Batch size for checkpointing. 0 disables checkpointing")
	cmd.Flags().StringVarP(&sourceURL, "source", "s", "", "Source URL for replication (e.g., mongodb://user:pass@host/db)")
	cmd.Flags().StringVarP(&targetURL, "target", "t", "", "Target URL for replication (e.g., mongodb://user:pass@host/db)")
	cmd.Flags().StringVarP(&replicatorID, "id", "i", "", "ID of the replicator instance")
	cmd.MarkFlagsRequiredTogether("source", "id")
	return cmd
}
