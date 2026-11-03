// Package obs holds logging and request-ID plumbing shared by all HTTP services.
package obs

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// RequestIDHeader carries the gateway-assigned request ID on responses and on
// upstream requests.
const RequestIDHeader = "X-Request-ID"

type ctxKey int

const (
	requestIDKey ctxKey = iota
	loggerKey
)

// NewLogger returns a JSON slog logger writing to w.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// WithRequestID returns ctx carrying id.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestID returns the request ID in ctx, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// WithLogger returns ctx carrying l.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

// Logger returns the request-scoped logger in ctx, or slog.Default().
func Logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

// Middleware assigns every request a fresh UUIDv7 request ID, exposes it in the
// X-Request-ID response header, attaches a logger carrying it, and writes one
// access-log line when the handler returns. A client-supplied X-Request-ID is
// never trusted as the ID; it is only logged as client_request_id.
func Middleware(base *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			id := uuid.Must(uuid.NewV7()).String()
			l := base.With("request_id", id)
			if cid := r.Header.Get(RequestIDHeader); cid != "" {
				l = l.With("client_request_id", cid)
			}
			ctx := WithLogger(WithRequestID(r.Context(), id), l)
			w.Header().Set(RequestIDHeader, id)

			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r.WithContext(ctx))

			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}
			l.LogAttrs(ctx, slog.LevelInfo, "http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Int64("bytes", rec.bytes),
				slog.Duration("duration", time.Since(start)),
				slog.Bool("client_gone", r.Context().Err() != nil),
			)
		})
	}
}

// statusRecorder captures the status code and body size. Unwrap lets
// http.ResponseController reach the underlying writer's Flush and deadlines.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += int64(n)
	return n, err
}

// FlushError is found by http.ResponseController before Unwrap, so a flush that
// implicitly sends a 200 is recorded.
func (s *statusRecorder) FlushError() error {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return http.NewResponseController(s.ResponseWriter).Flush()
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
