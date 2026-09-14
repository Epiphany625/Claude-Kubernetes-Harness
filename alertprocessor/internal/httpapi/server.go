// Package httpapi serves the webhook and the health probes.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/processor"
)

// Pinger is the readiness contract a dependency has to satisfy. Both
// store.Store and queue.Publisher already do, which is why /readyz can check
// them without either package knowing about HTTP.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Server owns the HTTP surface.
type Server struct {
	cfg       config.HTTPConfig
	processor *processor.Processor
	log       *slog.Logger

	// Named so /readyz can say which dependency is down.
	deps map[string]Pinger

	http *http.Server
}

// New builds the server and its routes.
func New(
	cfg config.HTTPConfig,
	proc *processor.Processor,
	deps map[string]Pinger,
	log *slog.Logger,
) *Server {
	s := &Server{cfg: cfg, processor: proc, deps: deps, log: log}

	mux := http.NewServeMux()

	// Method-qualified patterns (Go 1.22+): a GET to the webhook path gets 405
	// from the router rather than reaching the handler and being rejected there.
	mux.HandleFunc("POST "+cfg.WebhookPath, s.handleWebhook)

	// Liveness. Deliberately answers from process state alone -- no Postgres, no
	// RabbitMQ. A dependency outage must not make Kubernetes restart every
	// replica at once, which turns a recoverable problem into a reconnect storm
	// against a dependency that is already struggling.
	mux.HandleFunc("GET /healthz", s.handleHealthz)

	// Readiness. This is where dependencies belong: a pod that cannot reach
	// Postgres should be taken out of the Service so Alertmanager's retry lands
	// on one that can.
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	s.http = &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	return s
}

// Handler exposes the mux for tests, which drive it through httptest rather
// than binding a port.
func (s *Server) Handler() http.Handler { return s.http.Handler }

// ListenAndServe blocks until the server stops.
func (s *Server) ListenAndServe() error {
	s.log.Info("http server listening",
		"addr", s.cfg.Addr,
		"webhook_path", s.cfg.WebhookPath)
	s.log.Warn("webhook authentication is disabled; " +
		"all incoming webhook calls are accepted")
	return s.http.ListenAndServe()
}

// Shutdown stops accepting connections and waits for in-flight requests.
//
// Draining matters more here than for a typical service: a request cut off
// mid-flight may have recorded an event without publishing it, and the sender
// will retry into a pod that is going away.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// Bounded independently of the request: a hung dependency must not hold the
	// probe open until the kubelet's own timeout fires, or readiness flaps on
	// timeout rather than on the actual check.
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	type depStatus struct {
		Status string `json:"status"`
		Error  string `json:"error,omitempty"`
	}
	results := make(map[string]depStatus, len(s.deps))
	ready := true

	for name, dep := range s.deps {
		if err := dep.Ping(ctx); err != nil {
			ready = false
			results[name] = depStatus{Status: "down", Error: err.Error()}
			continue
		}
		results[name] = depStatus{Status: "up"}
	}

	code := http.StatusOK
	status := "ready"
	if !ready {
		code = http.StatusServiceUnavailable
		status = "not ready"
	}
	writeJSON(w, code, map[string]any{"status": status, "dependencies": results})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// errorResponse is the body shape for every non-2xx answer from the webhook.
type errorResponse struct {
	Error   string             `json:"error"`
	Summary *processor.Summary `json:"summary,omitempty"`
}
