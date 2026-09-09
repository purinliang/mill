// Package httpapi exposes the job service through HTTP.
package httpapi

import (
	"context"
	"log"
	"net/http"

	"github.com/purinliang/mill/internal/job"
)

// Store defines the job operations required by the HTTP adapter.
type Store interface {
	Create(context.Context, string, job.Submission) (job.Job, bool, error)
	Get(context.Context, string) (job.Job, error)
}

// Handler serves job submission and status requests.
type Handler struct {
	store  Store
	logger *log.Logger
}

// NewHandler creates a job HTTP handler backed by store.
func NewHandler(store Store, logger *log.Logger) *Handler {
	if logger == nil {
		logger = log.Default()
	}
	return &Handler{store: store, logger: logger}
}

// RegisterRoutes registers the job collection and resource endpoints.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/jobs", h.handleCollection)
	mux.HandleFunc("/jobs/{id}", h.handleResource)
}

func (h *Handler) handleCollection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	h.submit(w, r)
}

func (h *Handler) handleResource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	h.get(w, r)
}
