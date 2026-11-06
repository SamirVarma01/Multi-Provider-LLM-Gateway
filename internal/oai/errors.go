// Package oai defines OpenAI-compatible wire types shared by the gateway's
// public API, the OpenAI-format provider adapter, and the mock provider.
package oai

import (
	"encoding/json"
	"net/http"

	"github.com/SamirVarma01/llm-gateway/internal/obs"
)

// Error types used in error bodies.
const (
	ErrTypeInvalidRequest = "invalid_request_error"
	ErrTypeAuthentication = "authentication_error"
	ErrTypeNotFound       = "not_found_error"
	ErrTypeRateLimit      = "rate_limit_error"
	ErrTypeBudget         = "insufficient_quota"
	ErrTypeUpstream       = "upstream_error"
	ErrTypeAPI            = "api_error"
)

// ErrorBody is the OpenAI error envelope.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail is the content of an OpenAI error envelope.
type ErrorDetail struct {
	Message   string `json:"message"`
	Type      string `json:"type"`
	Code      string `json:"code,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// WriteError writes an OpenAI-shaped JSON error with the request ID from r.
func WriteError(w http.ResponseWriter, r *http.Request, status int, typ, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorBody{Error: ErrorDetail{
		Message:   msg,
		Type:      typ,
		Code:      code,
		RequestID: obs.RequestID(r.Context()),
	}})
}
