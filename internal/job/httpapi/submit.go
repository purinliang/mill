// This file handles job-submission requests.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/purinliang/mill/internal/job"
)

const maxRequestBodyBytes = 64 << 10

func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(
			w,
			http.StatusUnsupportedMediaType,
			"unsupported_media_type",
			"Content-Type must be application/json",
		)
		return
	}

	idempotencyKeys := r.Header.Values("Idempotency-Key")
	if len(idempotencyKeys) != 1 || idempotencyKeys[0] == "" {
		writeError(
			w,
			http.StatusBadRequest,
			"missing_idempotency_key",
			"exactly one Idempotency-Key header is required",
		)
		return
	}
	if err := job.ValidateIdempotencyKey(idempotencyKeys[0]); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid_idempotency_key",
			err.Error(),
		)
		return
	}

	var submission job.Submission
	decoder := json.NewDecoder(
		http.MaxBytesReader(w, r.Body, maxRequestBodyBytes),
	)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&submission); err != nil {
		writeInvalidRequest(w)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid_request",
			"request body must contain exactly one JSON object",
		)
		return
	}

	normalized, err := job.NormalizeSubmission(submission)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	createdJob, created, err := h.store.Create(
		r.Context(),
		idempotencyKeys[0],
		normalized,
	)
	if h.writeSubmissionError(w, err) {
		return
	}

	w.Header().Set("Location", "/jobs/"+createdJob.ID)
	if created {
		writeJSON(w, http.StatusCreated, createdJob)
		return
	}
	writeJSON(w, http.StatusOK, createdJob)
}

func (h *Handler) writeSubmissionError(
	w http.ResponseWriter,
	err error,
) bool {
	if err == nil {
		return false
	}

	var validationError *job.ValidationError
	switch {
	case errors.As(err, &validationError):
		writeError(
			w,
			http.StatusBadRequest,
			"invalid_input",
			validationError.Error(),
		)
	case errors.Is(err, job.ErrIdempotencyConflict):
		writeError(
			w,
			http.StatusConflict,
			"idempotency_conflict",
			"the idempotency key is already associated with a different submission",
		)
	case errors.Is(err, job.ErrInputConflict):
		writeError(
			w,
			http.StatusConflict,
			"input_conflict",
			"the input differs from the logical shards already planned for the job",
		)
	default:
		h.logger.Printf("submit job: %v", err)
		writeError(
			w,
			http.StatusInternalServerError,
			"internal_error",
			"an internal error occurred",
		)
	}
	return true
}

func writeInvalidRequest(w http.ResponseWriter) {
	writeError(
		w,
		http.StatusBadRequest,
		"invalid_request",
		"request body must be one valid job submission object",
	)
}
