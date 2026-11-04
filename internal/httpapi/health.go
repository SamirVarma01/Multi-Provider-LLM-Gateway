package httpapi

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// readyCheckTimeout bounds each dependency probe in /readyz.
const readyCheckTimeout = 2 * time.Second

// handleHealthz reports liveness: the process is serving.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz reports readiness: every dependency answered in time.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	var mu sync.Mutex
	failed := map[string]string{}
	var wg sync.WaitGroup
	for _, c := range s.opts.Ready {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), readyCheckTimeout)
			defer cancel()
			if err := c.Check(ctx); err != nil {
				mu.Lock()
				failed[c.Name] = err.Error()
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(failed) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "failed": failed})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
