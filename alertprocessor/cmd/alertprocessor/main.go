// Command alertprocessor receives Alertmanager webhooks, records each alert as
// an event in PostgreSQL with status "waiting", and publishes it to RabbitMQ for
// the harness to act on.
//
// See ../../README.md for the architecture and ../../CLAUDE.md for the
// invariants that hold across the packages.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/httpapi"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/obs"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/processor"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/queue"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "alertprocessor: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		// Before the logger exists, because the logger's own configuration is
		// part of what may have failed.
		return fmt.Errorf("invalid configuration:\n%w", err)
	}

	log := obs.NewLogger(cfg.Log.Level, cfg.Log.Format)
	metrics := obs.NewMetrics(prometheus.NewRegistry())

	// Signals cancel this context, which unwinds the whole startup sequence --
	// including a connection attempt that is retrying against a dependency that
	// will never come up.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("starting alertprocessor",
		"postgres", config.Redacted(cfg.Postgres.DSN),
		"postgres_pool_mode", string(cfg.Postgres.PoolMode),
		"amqp", config.Redacted(cfg.AMQP.URL),
		"exchange", cfg.AMQP.Exchange,
		"queue", cfg.AMQP.Queue)

	db, err := store.Open(ctx, cfg.Postgres, log)
	if err != nil {
		// Fail to start rather than start degraded. A pod that is up but cannot
		// record events would pass its liveness probe, accept webhooks, and
		// return 500 for each -- burning Alertmanager's retry budget on a
		// problem that a CrashLoopBackOff would have made obvious.
		return fmt.Errorf("postgres: %w", err)
	}
	defer db.Close()

	publisher, err := queue.Open(ctx, cfg.AMQP, log)
	if err != nil {
		return fmt.Errorf("rabbitmq: %w", err)
	}
	defer func() { _ = publisher.Close() }()

	proc := processor.New(db, publisher, metrics, log)

	srv := httpapi.New(cfg.HTTP, proc, metrics, map[string]httpapi.Pinger{
		"postgres": db,
		"rabbitmq": publisher,
	}, log)

	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
		log.Info("shutdown signal received", "grace_period", cfg.HTTP.ShutdownTimeout)
	}

	// Drain in-flight requests. A webhook cut off mid-flight may have recorded
	// an event without publishing it; letting it finish is cheaper than leaving
	// the row for a retry to reconcile.
	drainStart := time.Now()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown failed after %s: %w", cfg.HTTP.ShutdownTimeout, err)
	}

	// Only now are the dependencies safe to close -- via the deferred Close
	// calls above. Shutting the pool before the server has drained would fail
	// the very requests we just waited for.
	log.Info("shutdown complete", "drained_in", time.Since(drainStart).String())
	return nil
}
