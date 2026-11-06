package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/auth"
	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
)

// maxBudgetUSD keeps nano-USD budgets well inside int64.
const maxBudgetUSD = 1e9

func adminAuth(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if token == "" || !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
				oai.WriteError(w, r, http.StatusUnauthorized, oai.ErrTypeAuthentication, "invalid_admin_token",
					"Invalid admin token.")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type createKeyRequest struct {
	Name      string   `json:"name"`
	RPMLimit  *int     `json:"rpm_limit"`
	TPMLimit  *int     `json:"tpm_limit"`
	BudgetUSD *float64 `json:"budget_usd"`
}

type keyResponse struct {
	ID        uuid.UUID  `json:"id"`
	Key       string     `json:"key,omitempty"` // only on creation
	Name      string     `json:"name"`
	Prefix    string     `json:"prefix"`
	RPMLimit  *int       `json:"rpm_limit"`
	TPMLimit  *int       `json:"tpm_limit"`
	BudgetUSD *float64   `json:"budget_usd"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at"`
}

func toKeyResponse(k auth.Key) keyResponse {
	out := keyResponse{
		ID: k.ID, Name: k.Name, Prefix: k.Prefix, RPMLimit: k.RPMLimit, TPMLimit: k.TPMLimit,
		CreatedAt: k.CreatedAt, RevokedAt: k.RevokedAt,
	}
	if k.BudgetNUSD != nil {
		usd := float64(*k.BudgetNUSD) / 1e9
		out.BudgetUSD = &usd
	}
	return out
}

func checkLimit(field string, v *int) *apiError {
	if v != nil && *v <= 0 {
		return badRequest("invalid_limit", "%s must be positive or null.", field)
	}
	return nil
}

// budgetNUSD validates a budget in USD and converts it. nil means unlimited.
func budgetNUSD(usd *float64) (*int64, *apiError) {
	if usd == nil {
		return nil, nil
	}
	if b := *usd; math.IsNaN(b) || b < 0 || b > maxBudgetUSD {
		return nil, badRequest("invalid_budget", "budget_usd must be between 0 and %g, or null.", float64(maxBudgetUSD))
	}
	nusd := int64(math.Round(*usd * 1e9))
	return &nusd, nil
}

func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	var req createKeyRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		badRequest("invalid_json", "Invalid JSON body: %v", err).write(w, r)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		badRequest("missing_name", "'name' is required.").write(w, r)
		return
	}
	for field, v := range map[string]*int{"rpm_limit": req.RPMLimit, "tpm_limit": req.TPMLimit} {
		if aerr := checkLimit(field, v); aerr != nil {
			aerr.write(w, r)
			return
		}
	}
	budget, aerr := budgetNUSD(req.BudgetUSD)
	if aerr != nil {
		aerr.write(w, r)
		return
	}
	nk := auth.NewKey{Name: req.Name, RPMLimit: req.RPMLimit, TPMLimit: req.TPMLimit, BudgetNUSD: budget}

	k, plaintext, err := s.opts.Keys.CreateKey(r.Context(), nk)
	if err != nil {
		obs.Logger(r.Context()).Error("create key failed", "err", err)
		oai.WriteError(w, r, http.StatusInternalServerError, oai.ErrTypeAPI, "internal_error", "Could not create key.")
		return
	}
	obs.Logger(r.Context()).Info("api key created", "key_id", k.ID.String(), "name", k.Name)
	resp := toKeyResponse(k)
	resp.Key = plaintext
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.opts.Keys.ListKeys(r.Context())
	if err != nil {
		obs.Logger(r.Context()).Error("list keys failed", "err", err)
		oai.WriteError(w, r, http.StatusInternalServerError, oai.ErrTypeAPI, "internal_error", "Could not list keys.")
		return
	}
	out := make([]keyResponse, len(keys))
	for i, k := range keys {
		out[i] = toKeyResponse(k)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

// decodeKeyUpdate parses a PATCH body. A field that is absent stays as it is
// and a field that is null becomes unlimited, which a plain struct decode
// cannot tell apart, so it works from the raw fields.
func decodeKeyUpdate(w http.ResponseWriter, r *http.Request) (auth.KeyUpdate, *apiError) {
	var u auth.KeyUpdate
	var fields map[string]json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&fields); err != nil {
		return u, badRequest("invalid_json", "Invalid JSON body: %v", err)
	}
	for name, raw := range fields {
		var aerr *apiError
		switch name {
		case "rpm_limit":
			u.SetRPMLimit = true
			u.RPMLimit, aerr = decodeLimit(name, raw)
		case "tpm_limit":
			u.SetTPMLimit = true
			u.TPMLimit, aerr = decodeLimit(name, raw)
		case "budget_usd":
			var usd *float64
			if err := json.Unmarshal(raw, &usd); err != nil {
				return u, badRequest("invalid_budget", "budget_usd must be a number or null.")
			}
			u.SetBudget = true
			u.BudgetNUSD, aerr = budgetNUSD(usd)
		default:
			return u, badRequest("invalid_json", "Unknown field %q.", name)
		}
		if aerr != nil {
			return u, aerr
		}
	}
	return u, nil
}

func decodeLimit(field string, raw json.RawMessage) (*int, *apiError) {
	var v *int
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, badRequest("invalid_limit", "%s must be a positive integer or null.", field)
	}
	return v, checkLimit(field, v)
}

func (s *Server) handleUpdateKey(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		badRequest("invalid_id", "Invalid key id.").write(w, r)
		return
	}
	u, aerr := decodeKeyUpdate(w, r)
	if aerr != nil {
		aerr.write(w, r)
		return
	}
	k, err := s.opts.Keys.UpdateKey(r.Context(), id, u)
	switch {
	case errors.Is(err, auth.ErrNotFound):
		oai.WriteError(w, r, http.StatusNotFound, oai.ErrTypeNotFound, "key_not_found", "No such key.")
		return
	case err != nil:
		obs.Logger(r.Context()).Error("update key failed", "err", err)
		oai.WriteError(w, r, http.StatusInternalServerError, oai.ErrTypeAPI, "internal_error", "Could not update key.")
		return
	}
	obs.Logger(r.Context()).Info("api key updated", "key_id", k.ID.String())
	writeJSON(w, http.StatusOK, toKeyResponse(k))
}

func (s *Server) handleRevokeKey(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		badRequest("invalid_id", "Invalid key id.").write(w, r)
		return
	}
	k, err := s.opts.Keys.RevokeKey(r.Context(), id)
	switch {
	case errors.Is(err, auth.ErrNotFound):
		oai.WriteError(w, r, http.StatusNotFound, oai.ErrTypeNotFound, "key_not_found", "No such key.")
		return
	case err != nil:
		obs.Logger(r.Context()).Error("revoke key failed", "err", err)
		oai.WriteError(w, r, http.StatusInternalServerError, oai.ErrTypeAPI, "internal_error", "Could not revoke key.")
		return
	}
	obs.Logger(r.Context()).Info("api key revoked", "key_id", k.ID.String())
	writeJSON(w, http.StatusOK, toKeyResponse(k))
}
