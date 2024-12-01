package archiver

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (a *Archiver) Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (a *Archiver) RegisterRoutes(r *chi.Mux) {
	r.Get("/health", a.Health)
}
