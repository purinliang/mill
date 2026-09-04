package job

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net/http"
)

const maxRequestBodyBytes = 64 << 10

type Store interface {
	Create(context.Context, string, Submission) (Job, bool, error)
	Get(context.Context, string) (Job, error)
}

type Handler struct {
	store  Store
	logger *log.Logger
}

type errorResponse struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHandler(store Store, logger *log.Logger) *Handler {
	if logger == nil {
		logger = log.Default()
	}
	return &Handler{store: store, logger: logger}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/jobs", h.handleCollection)
	mux.HandleFunc("/jobs/{id}", h.handleResource)
}

func (h *Handler) handleCollection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	h.create(w, r)
}

func (h *Handler) handleResource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	h.get(w, r)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}

	idempotencyKeys := r.Header.Values("Idempotency-Key")
	if len(idempotencyKeys) != 1 || idempotencyKeys[0] == "" {
		writeError(w, http.StatusBadRequest, "missing_idempotency_key", "exactly one Idempotency-Key header is required")
		return
	}
	if err := validateIdempotencyKey(idempotencyKeys[0]); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key", err.Error())
		return
	}

	var submission Submission
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&submission); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be one valid job submission object")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must contain exactly one JSON object")
		return
	}

	normalizedSubmission, err := normalizeSubmission(submission)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	createdJob, created, err := h.store.Create(r.Context(), idempotencyKeys[0], normalizedSubmission)
	var validationError *ValidationError
	if errors.As(err, &validationError) {
		writeError(w, http.StatusBadRequest, "invalid_manifest", validationError.Error())
		return
	}
	if errors.Is(err, ErrIdempotencyConflict) {
		writeError(w, http.StatusConflict, "idempotency_conflict", "the idempotency key is already associated with a different submission")
		return
	}
	if errors.Is(err, ErrManifestConflict) {
		writeError(w, http.StatusConflict, "manifest_conflict", "the dataset manifest differs from the materialized task set")
		return
	}
	if err != nil {
		h.logger.Printf("create job: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "an internal error occurred")
		return
	}

	w.Header().Set("Location", "/jobs/"+createdJob.ID)
	if created {
		writeJSON(w, http.StatusCreated, createdJob)
		return
	}
	writeJSON(w, http.StatusOK, createdJob)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validJobID(id) {
		writeError(w, http.StatusBadRequest, "invalid_job_id", "job ID must be a UUID")
		return
	}

	job, err := h.store.Get(r.Context(), id)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "job_not_found", "job was not found")
		return
	}
	if err != nil {
		h.logger.Printf("get job %s: %v", id, err)
		writeError(w, http.StatusInternalServerError, "internal_error", "an internal error occurred")
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func methodNotAllowed(w http.ResponseWriter, allowedMethod string) {
	w.Header().Set("Allow", allowedMethod)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method is not allowed for this resource")
}

func writeError(w http.ResponseWriter, statusCode int, code, message string) {
	writeJSON(w, statusCode, errorResponse{
		Error: apiError{Code: code, Message: message},
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(value)
}
