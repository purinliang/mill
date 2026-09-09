// This file handles job-status requests.
package httpapi

import (
	"errors"
	"net/http"

	"github.com/purinliang/mill/internal/job"
)

func (h *Handler) getJobStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !job.ValidID(id) {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid_job_id",
			"job ID must be a UUID",
		)
		return
	}

	value, err := h.store.Get(r.Context(), id)
	if errors.Is(err, job.ErrNotFound) {
		writeError(
			w,
			http.StatusNotFound,
			"job_not_found",
			"job was not found",
		)
		return
	}
	if err != nil {
		h.logger.Printf("get job %s: %v", id, err)
		writeError(
			w,
			http.StatusInternalServerError,
			"internal_error",
			"an internal error occurred",
		)
		return
	}

	writeJSON(w, http.StatusOK, value)
}
