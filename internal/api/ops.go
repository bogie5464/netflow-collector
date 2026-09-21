package api

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

// readyBudget bounds every backend ping on /readyz.
const readyBudget = 2 * time.Second

// registerOps mounts the three public operational endpoints on the outer
// mux, outside the authenticated /v1 subtree.
func (s *Server) registerOps(mux *http.ServeMux) {
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
}

// healthz means the process is alive and its HTTP server can answer.
// Nothing else: it stays 200 while every backend is down, so a database
// blip never becomes a restart loop.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeData(w, map[string]string{"status": "ok"}, meta{})
}

// readyz pings every configured backend with a shared 2 s budget and is
// 503 backend_unavailable when any of them fails.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readyBudget)
	defer cancel()

	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		status  = map[string]string{}
		healthy = true
	)
	for name, b := range s.sinks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := ping(ctx, b)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				status[name] = "unavailable"
				healthy = false
				s.log.Warn("readiness ping failed", "request_id", RequestID(r.Context()), "backend", name, "err", err)
				return
			}
			status[name] = "ok"
		}()
	}
	wg.Wait()

	if !healthy {
		names := make([]string, 0, len(status))
		for name, st := range status {
			if st != "ok" {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		writeError(w, r, http.StatusServiceUnavailable, CodeBackendUnavailable, "backend not ready: "+joinNames(names), nil)
		return
	}
	writeData(w, map[string]any{"status": "ready", "backends": status}, meta{})
}

// ping is one readiness round trip. A sink that offers flow.Pinger answers
// with it; otherwise the cheapest query the storage contract allows — one
// record over an empty one-microsecond window. A sink that offers neither
// cannot be shown ready, and saying so is better than guessing.
func ping(ctx context.Context, s flow.Sink) error {
	switch v := s.(type) {
	case flow.Pinger:
		return v.Ping(ctx)
	case flow.Querier:
		now := time.Now().UTC()
		_, err := v.Query(ctx, flow.FlowQuery{Start: now.Add(-time.Microsecond), End: now, Limit: 1})
		return err
	default:
		return errors.New("sink offers no readiness probe")
	}
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}
