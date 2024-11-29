package cmd

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/spf13/cobra"
	"github.com/turbolytics/librarian/internal/archiver"
	"go.uber.org/zap"
)

func NewArchiverCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "archiver",
		Short: "Manages the archival of data",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("welcome to librarian archiver!")
			return nil
		},
	}
	cmd.AddCommand(newArchiveStartCommand())
	cmd.AddCommand(newArchiveCollectCommand())
	return cmd
}

func newArchiveCollectCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "collect",
		Short: "Collects data from a source",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("collecting data from source")
			return nil
		},
	}
}

func newArchiveStartCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "start",
		Short: "Starts the archival daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewDevelopment()
			defer logger.Sync()
			l := logger.Named("librarian.archiver")
			l.Info("starting archiver!")

			a := archiver.New(archiver.WithLogger(l))

			logMiddleware := func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					start := time.Now()
					ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

					defer func() {
						logger.Info("request",
							zap.String("from", r.RemoteAddr),
							zap.String("protocol", r.Proto),
							zap.String("method", r.Method),
							zap.String("path", r.URL.Path),
							zap.Int("status", ww.Status()),
							zap.Int("bytes", ww.BytesWritten()),
							zap.Duration("duration", time.Since(start)),
						)
					}()

					next.ServeHTTP(ww, r)
				})
			}

			r := chi.NewRouter()
			r.Use(logMiddleware)

			a.RegisterRoutes(r)

			address := fmt.Sprintf(":%d", 8080)
			l.Info("Starting server",
				zap.Int("port", 8080),
			)

			if err := http.ListenAndServe(address, r); err != nil {
				log.Fatalf("Failed to start server: %v", err)
			}

			return nil
		},
	}

	return cmd
}
