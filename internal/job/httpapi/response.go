// This file writes consistent JSON responses and HTTP errors.
package httpapi

import (
	"encoding/json"
	"net/http"
)

type errorResponse struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func methodNotAllowed(w http.ResponseWriter, allowedMethod string) {
	w.Header().Set("Allow", allowedMethod)
	writeError(
		w,
		http.StatusMethodNotAllowed,
		"method_not_allowed",
		"method is not allowed for this resource",
	)
}

func writeError(
	w http.ResponseWriter,
	statusCode int,
	code string,
	message string,
) {
	writeJSON(w, statusCode, errorResponse{
		Error: apiError{Code: code, Message: message},
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(value)
}
