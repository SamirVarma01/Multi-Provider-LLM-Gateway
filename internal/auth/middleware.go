package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
)

type ctxKey struct{}

// WithKey returns ctx carrying the authenticated key.
func WithKey(ctx context.Context, k Key) context.Context {
	return context.WithValue(ctx, ctxKey{}, k)
}

// FromContext returns the authenticated key, if any.
func FromContext(ctx context.Context) (Key, bool) {
	k, ok := ctx.Value(ctxKey{}).(Key)
	return k, ok
}

// Middleware rejects requests without a valid, unrevoked API key and stores
// the key in the request context. Unknown and revoked keys get the same
// response so the API does not reveal which keys once existed.
func Middleware(l Lookuper) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := bearerToken(r)
			if !ok {
				oai.WriteError(w, r, http.StatusUnauthorized, oai.ErrTypeAuthentication, "missing_api_key",
					"Missing API key. Send it as 'Authorization: Bearer <key>'.")
				return
			}
			if !strings.HasPrefix(token, KeyPrefix) {
				oai.WriteError(w, r, http.StatusUnauthorized, oai.ErrTypeAuthentication, "invalid_api_key",
					"Invalid API key.")
				return
			}
			k, err := l.LookupByHash(r.Context(), HashKey(token))
			switch {
			case errors.Is(err, ErrNotFound) || (err == nil && k.Revoked()):
				oai.WriteError(w, r, http.StatusUnauthorized, oai.ErrTypeAuthentication, "invalid_api_key",
					"Invalid API key.")
				return
			case err != nil:
				obs.Logger(r.Context()).Error("auth lookup failed", "err", err)
				oai.WriteError(w, r, http.StatusServiceUnavailable, oai.ErrTypeAPI, "auth_unavailable",
					"Authentication is temporarily unavailable.")
				return
			}
			ctx := WithKey(r.Context(), k)
			ctx = obs.WithLogger(ctx, obs.Logger(ctx).With("key_id", k.ID.String()))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	scheme, token, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}
