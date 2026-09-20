// Package api serves the query API over net/http. It depends on flow.Querier
// and never imports a concrete backend package.
//
// One envelope, every endpoint, including errors:
//
//	{"data": ..., "meta": {"next_cursor": ..., "has_more": ...}}
//	{"error": {"code", "message", "details", "request_id"}}
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/netip"
	"time"

	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
)

// Error codes are a stable machine contract; messages may change.
const (
	CodeValidation         = "validation_error"    // 422
	CodeBadRequest         = "bad_request"         // 400
	CodeUnauthorized       = "unauthorized"        // 401
	CodeNotFound           = "not_found"           // 404
	CodeMethodNotAllowed   = "method_not_allowed"  // 405
	CodeBackendUnavailable = "backend_unavailable" // 503
	CodeInternal           = "internal_error"      // 500
)

// Exporter is one row of the exporters table as the API renders it.
type Exporter struct {
	ID          int64      `json:"id"`
	IPAddress   netip.Addr `json:"ip_address"`
	Label       *string    `json:"label"`
	FirstSeenAt time.Time  `json:"first_seen_at"`
	LastSeenAt  time.Time  `json:"last_seen_at"`
}

// ExporterLister is an optional capability a backend may offer for
// GET /v1/exporters. It is discovered by type assertion so flow.Backend
// stays exactly as narrow as the storage contract requires.
type ExporterLister interface {
	ListExporters(ctx context.Context) ([]Exporter, error)
}

// Server holds the handler's dependencies.
type Server struct {
	querier  flow.Querier
	backends map[string]flow.Backend
	cfg      config.Config
	log      *slog.Logger
}

// New builds the HTTP handler. The /v1 subtree is mounted exactly once,
// behind the API key middleware; everything registered on the outer mux
// (the ops endpoints) stays public.
func New(q flow.Querier, backends map[string]flow.Backend, cfg config.Config) http.Handler {
	s := &Server{querier: q, backends: backends, cfg: cfg, log: slog.Default().With("component", "api")}
	v1 := http.NewServeMux()
	for _, r := range routes {
		h := r.handler
		v1.HandleFunc(r.Method+" "+r.Path, func(w http.ResponseWriter, req *http.Request) { h(s, w, req) })
		v1.HandleFunc(r.Path, methodNotAllowed) // any other method on a known path
	}
	v1.HandleFunc("/", notFound)

	mux := http.NewServeMux()
	mux.Handle("/v1/", requireAPIKey(cfg.APIKeys, s.log, v1))
	mux.HandleFunc("/", notFound)
	return s.middleware(mux)
}

// middleware assigns a request id, logs one line per request, and recovers
// panics into a 500 envelope.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := newRequestID()
		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), ctxKeyRequestID{}, id)
		r = r.WithContext(ctx)
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic serving request", "request_id", id, "panic", p)
				writeError(rw, r, http.StatusInternalServerError, CodeInternal, "internal error", nil)
			}
			s.log.Info("request", "request_id", id, "method", r.Method, "path", r.URL.Path,
				"status", rw.status, "duration_ms", time.Since(start).Milliseconds())
		}()
		next.ServeHTTP(rw, r)
	})
}

type ctxKeyRequestID struct{}

// RequestID returns the id assigned to this request.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyRequestID{}).(string)
	return id
}

func newRequestID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "req_" + hex.EncodeToString(b[:])
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

// Envelope shapes.
type (
	meta struct {
		NextCursor string `json:"next_cursor,omitempty"`
		HasMore    bool   `json:"has_more"`
	}
	successEnvelope struct {
		Data any  `json:"data"`
		Meta meta `json:"meta"`
	}
	// Detail names one offending field on a validation_error.
	Detail struct {
		Field   string `json:"field"`
		Message string `json:"message"`
	}
	errorBody struct {
		Code      string   `json:"code"`
		Message   string   `json:"message"`
		Details   []Detail `json:"details,omitempty"`
		RequestID string   `json:"request_id"`
	}
	errorEnvelope struct {
		Error errorBody `json:"error"`
	}
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeData(w http.ResponseWriter, data any, m meta) {
	writeJSON(w, http.StatusOK, successEnvelope{Data: data, Meta: m})
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string, details []Detail) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{
		Code: code, Message: msg, Details: details, RequestID: RequestID(r.Context()),
	}})
}

func notFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusNotFound, CodeNotFound, "no route matches "+r.URL.Path, nil)
}

func methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusMethodNotAllowed, CodeMethodNotAllowed, r.Method+" is not allowed on "+r.URL.Path, nil)
}
