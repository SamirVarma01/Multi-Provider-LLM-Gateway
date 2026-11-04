package obs

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestMiddlewareAssignsRequestID(t *testing.T) {
	var logs bytes.Buffer
	var seenID string
	var seenLogger *slog.Logger
	h := Middleware(NewLogger(&logs, slog.LevelInfo))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenID = RequestID(r.Context())
		seenLogger = Logger(r.Context())
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("Flush through recorder: %v", err)
		}
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(RequestIDHeader, "client-supplied")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	id, err := uuid.Parse(seenID)
	if err != nil || id.Version() != 7 {
		t.Fatalf("request ID %q is not a UUIDv7 (err=%v)", seenID, err)
	}
	if got := rr.Header().Get(RequestIDHeader); got != seenID {
		t.Errorf("response header = %q, want %q", got, seenID)
	}
	if seenLogger == slog.Default() {
		t.Error("handler did not get a request-scoped logger")
	}

	var line map[string]any
	if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
		t.Fatalf("access log is not one JSON line: %v\n%s", err, logs.String())
	}
	if line["request_id"] != seenID || line["client_request_id"] != "client-supplied" {
		t.Errorf("access log ids = %v / %v", line["request_id"], line["client_request_id"])
	}
	// Flush writes an implicit 200 before the handler's WriteHeader; the recorder
	// must report what the client actually received.
	if line["status"] != float64(http.StatusOK) {
		t.Errorf("access log status = %v, want 200", line["status"])
	}
}

func TestLoggerDefault(t *testing.T) {
	if Logger(httptest.NewRequest(http.MethodGet, "/", nil).Context()) != slog.Default() {
		t.Error("Logger without a context logger should be slog.Default()")
	}
}
