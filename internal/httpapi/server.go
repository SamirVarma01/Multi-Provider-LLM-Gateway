// Package httpapi implements the gateway's public and admin HTTP APIs (design §4).
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/auth"
	"github.com/SamirVarma01/llm-gateway/internal/estimate"
	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
	"github.com/SamirVarma01/llm-gateway/internal/router"
)

// KeyAdmin manages API keys for the admin API.
type KeyAdmin interface {
	CreateKey(ctx context.Context, nk auth.NewKey) (auth.Key, string, error)
	ListKeys(ctx context.Context) ([]auth.Key, error)
	RevokeKey(ctx context.Context, id uuid.UUID) (auth.Key, error)
}

// ReadyCheck is one dependency probed by /readyz.
type ReadyCheck struct {
	Name  string
	Check func(context.Context) error
}

// Options configures a Server.
type Options struct {
	Logger         *slog.Logger
	Auth           auth.Lookuper
	Keys           KeyAdmin
	Router         *router.Router
	Estimator      estimate.Estimator // fills in usage providers do not report
	Ready          []ReadyCheck
	AdminToken     string
	RequestTimeout time.Duration
	MaxBodyBytes   int64
}

// Server serves the gateway APIs.
type Server struct {
	opts Options
	now  func() time.Time

	onOutcome func(outcome) // test hook, called with every chat request's outcome
}

// New returns a Server.
func New(opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Server{opts: opts, now: time.Now}
}

// PublicHandler serves /v1/*, /healthz and /readyz.
func (s *Server) PublicHandler() http.Handler {
	authed := auth.Middleware(s.opts.Auth)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("POST /v1/chat/completions", authed(http.HandlerFunc(s.handleChatCompletions)))
	mux.Handle("GET /v1/models", authed(http.HandlerFunc(s.handleModels)))
	mux.HandleFunc("/", notFound)
	return obs.Middleware(s.opts.Logger)(mux)
}

// AdminHandler serves /admin/*, guarded by the admin token.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/keys", s.handleCreateKey)
	mux.HandleFunc("GET /admin/keys", s.handleListKeys)
	mux.HandleFunc("POST /admin/keys/{id}/revoke", s.handleRevokeKey)
	mux.HandleFunc("/", notFound)
	return obs.Middleware(s.opts.Logger)(adminAuth(s.opts.AdminToken)(mux))
}

func notFound(w http.ResponseWriter, r *http.Request) {
	oai.WriteError(w, r, http.StatusNotFound, oai.ErrTypeNotFound, "not_found",
		"No route for "+r.Method+" "+r.URL.Path+".")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
