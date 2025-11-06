package replicator

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

type Server struct {
	logger      *zap.Logger
	replicators map[string]*Replicator
	mu          sync.RWMutex
}

type ReplicatorInfo struct {
	ID     string `json:"id"`
	State  State  `json:"state"`
	Source string `json:"source,omitempty"`

	Stats Stats `json:"stats,omitempty"`
}

func NewServer(logger *zap.Logger) *Server {
	return &Server{
		logger:      logger,
		replicators: make(map[string]*Replicator),
	}
}

func (s *Server) RegisterReplicator(r *Replicator) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.replicators[r.ID] = r
	s.logger.Info("replicator registered",
		zap.String("replicator_id", r.ID),
		zap.String("state", string(r.State.Current())))
}

func (s *Server) UnregisterReplicator(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if r, exists := s.replicators[id]; exists {
		delete(s.replicators, id)
		s.logger.Info("replicator unregistered",
			zap.String("replicator_id", id),
			zap.String("state", string(r.State.Current())))
	}
}

func (s *Server) Routes() chi.Router {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	r.Route("/api/v1/replicators", func(r chi.Router) {
		r.Get("/", s.listReplicators)
		r.Get("/{id}", s.getReplicator)
		// r.Delete("/{id}", s.deleteReplicator)

		// Control endpoints
		r.Post("/{id}/pause", s.signalHandler(SignalPause))
		r.Post("/{id}/resume", s.signalHandler(SignalResume))
		r.Post("/{id}/restart", s.signalHandler(SignalRestart))
		r.Post("/{id}/stop", s.signalHandler(SignalStop))
	})

	return r
}

// signalHandler returns a handler function for the given signal
func (s *Server) signalHandler(signal Signal) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")

		s.mu.RLock()
		rep, exists := s.replicators[id]
		s.mu.RUnlock()

		if !exists {
			http.Error(w, "replicator not found", http.StatusNotFound)
			return
		}

		rep.SendSignal(signal)

		s.logger.Info("signal sent to replicator",
			zap.String("replicator_id", id),
			zap.String("signal", string(signal)))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":        string(signal) + " signal sent",
			"replicator_id": id,
		})
	}
}

func (s *Server) listReplicators(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	replicators := make([]ReplicatorInfo, 0, len(s.replicators))
	for _, rep := range s.replicators {
		replicators = append(replicators, ReplicatorInfo{
			ID:    rep.ID,
			State: rep.State.Current(),
			Stats: rep.GetStats(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"replicators": replicators,
		"count":       len(replicators),
	})
}

func (s *Server) getReplicator(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	s.mu.RLock()
	rep, exists := s.replicators[id]
	s.mu.RUnlock()

	if !exists {
		http.Error(w, "replicator not found", http.StatusNotFound)
		return
	}

	info := ReplicatorInfo{
		ID:    rep.ID,
		State: rep.State.Current(),
		Stats: rep.GetStats(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func (s *Server) Start(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:    addr,
		Handler: s.Routes(),
	}

	s.logger.Info("starting replicator server", zap.String("addr", addr))

	go func() {
		<-ctx.Done()
		s.logger.Info("shutting down replicator server")
		srv.Shutdown(context.Background())
	}()

	return srv.ListenAndServe()
}
