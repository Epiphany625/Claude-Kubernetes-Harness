// Package httpapi serves the webhook, the health probes and the metrics
// endpoint.
package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/obs"
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
	metrics   *obs.Metrics
	log       *slog.Logger

	// Named so /readyz and the dependency_up metric can say which one is down.
	deps map[string]Pinger

	http *http.Server
}

// New builds the server and its routes.
func New(
	cfg config.HTTPConfig,
	proc *processor.Processor,
	metrics *obs.Metrics,
	deps map[string]Pinger,
	log *slog.Logger,
) *Server {
	s := &Server{cfg: cfg, processor: proc, metrics: metrics, deps: deps, log: log}

	// Seed one gauge per dependency now rather than waiting for the first
	// readiness probe. A gauge with no series makes
	// `alertprocessor_dependency_up == 0` match nothing, so the alert watching it
	// would be silently disarmed for the first ten seconds of the pod's life --
	// which is exactly the window in which a dependency problem is most likely.
	//
	// 1 is the honest starting value: main opens both dependencies before
	// building the server and refuses to start if either failed, so reaching
	// here means both answered.
	for name := range deps {
		metrics.SetDependencyUp(name, true)
	}

	mux := http.NewServeMux()

	// Method-qualified patterns (Go 1.22+): a GET to the webhook path gets 405
	// from the router rather than reaching the handler and being rejected there.
	mux.Handle("POST "+cfg.WebhookPath, s.withMetrics(http.HandlerFunc(s.handleWebhook)))

	// Liveness. Deliberately answers from process state alone -- no Postgres, no
	// RabbitMQ. A dependency outage must not make Kubernetes restart every
	// replica at once, which turns a recoverable problem into a reconnect storm
	// against a dependency that is already struggling.
	mux.HandleFunc("GET /healthz", s.handleHealthz)

	// Readiness. This is where dependencies belong: a pod that cannot reach
	// Postgres should be taken out of the Service so Alertmanager's retry lands
	// on one that can.
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	if metrics != nil && metrics.Registry() != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry(), promhttp.HandlerOpts{}))
	}

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
		"webhook_path", s.cfg.WebhookPath,
		"auth", s.cfg.AuthEnabled())
	if !s.cfg.AuthEnabled() {
		// Loud on purpose. An unauthenticated webhook lets anything with network
		// access to the pod inject events the harness will act on.
		s.log.Warn("webhook authentication is DISABLED; " +
			"set " + config.EnvPrefix + "WEBHOOK_TOKEN to require a bearer token")
	}
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
			s.metrics.SetDependencyUp(name, false)
			continue
		}
		results[name] = depStatus{Status: "up"}
		s.metrics.SetDependencyUp(name, true)
	}

	code := http.StatusOK
	status := "ready"
	if !ready {
		code = http.StatusServiceUnavailable
		status = "not ready"
	}
	writeJSON(w, code, map[string]any{"status": status, "dependencies": results})
}

// withMetrics records the outcome of a webhook request. It wraps only the
// webhook: instrumenting the probes would bury the signal under kubelet traffic.
func (s *Server) withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.metrics.WebhookHandled(strconv.Itoa(rec.code), time.Since(start))
	})
}

// authorize checks the bearer token Alertmanager presents.
func (s *Server) authorize(r *http.Request) bool {
	if !s.cfg.AuthEnabled() {
		return true
	}
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if len(header) <= len(prefix) || header[:len(prefix)] != prefix {
		return false
	}
	// Constant time: a byte-at-a-time comparison leaks the token's prefix to
	// anything that can time the response, and this endpoint is a retry target
	// so an attacker gets as many attempts as they like.
	return subtle.ConstantTimeCompare([]byte(header[len(prefix):]), []byte(s.cfg.WebhookToken)) == 1
}

type statusRecorder struct {
	http.ResponseWriter
	code    int
	written bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.written {
		return
	}
	r.code = code
	r.written = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.written = true
	}
	return r.ResponseWriter.Write(b)
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
