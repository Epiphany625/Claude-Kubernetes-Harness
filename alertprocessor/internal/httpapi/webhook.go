package httpapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/alertmanager"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/processor"
)

// handleWebhook receives one Alertmanager delivery.
//
// # The status code is a control signal, not a formality
//
// Alertmanager retries a webhook that answers 5xx and gives up on one that
// answers 4xx. That makes the choice of code the pipeline's entire durability
// story:
//
//   - 400 for a body we cannot parse. Retrying will not make it parse, and
//     holding the sender in a retry loop over it delays real alerts behind it.
//   - 401 for a bad token. Same reasoning.
//   - 500 when any alert in the batch failed for an operational reason --
//     Postgres unreachable, the broker refusing. These are exactly the cases a
//     retry fixes.
//
// Returning 200 on a partial failure is the tempting mistake: it looks tidy, and
// it drops the failed alerts on the floor with nothing anywhere recording that
// it happened.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(r) {
		s.log.Warn("rejected webhook with missing or invalid bearer token",
			"remote_addr", r.RemoteAddr)
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "invalid or missing bearer token"})
		return
	}

	// Belt and braces with the LimitReader inside Decode: MaxBytesReader also
	// stops the client from streaming an unbounded body into the connection.
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)

	payload, err := alertmanager.Decode(r.Body, s.cfg.MaxBodyBytes)
	switch {
	case errors.Is(err, alertmanager.ErrNoAlerts):
		// Alertmanager does not send empty batches, so this is almost always
		// something else POSTing at the endpoint -- a health checker, or a
		// misconfigured sender. Accepted rather than retried.
		s.log.Warn("webhook delivery contained no alerts", "remote_addr", r.RemoteAddr)
		writeJSON(w, http.StatusOK, processor.Summary{})
		return
	case err != nil:
		s.log.Warn("rejected malformed webhook payload",
			"remote_addr", r.RemoteAddr, "error", err)
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	if payload.Version != "" && payload.Version != alertmanager.PayloadVersion {
		// Processed anyway. The fields this service reads have been stable
		// across every version of the format, and dropping alerts over a version
		// string would be a self-inflicted outage on an Alertmanager upgrade.
		s.log.Warn("unexpected webhook payload version; processing anyway",
			"got", payload.Version, "expected", alertmanager.PayloadVersion)
	}
	if payload.TruncatedAlerts > 0 {
		// Alerts that will never arrive. Nothing here can recover them -- the
		// fix is maxAlerts: 0 in the AlertmanagerConfig -- so the only useful
		// response is to make the loss visible.
		s.log.Error("Alertmanager truncated this delivery; alerts have been lost before reaching us",
			"truncated", payload.TruncatedAlerts,
			"group_key", payload.GroupKey,
			"fix", "set maxAlerts: 0 on the webhook receiver in ops/alerting/alertmanagerconfig.yaml")
	}

	events := payload.ToEvents(time.Now())
	s.metrics.AlertsIngested(len(events))

	s.log.Info("webhook delivery received",
		"group_key", payload.GroupKey,
		"receiver", payload.Receiver,
		"status", payload.Status,
		"alerts", len(events))

	outcomes := s.processor.ProcessBatch(r.Context(), events)
	summary := processor.Summarize(outcomes)

	if summary.Failed > 0 {
		s.log.Error("webhook delivery partially failed; answering 500 so Alertmanager retries",
			"group_key", payload.GroupKey,
			"total", summary.Total,
			"failed", summary.Failed,
			"published", summary.Published)
		writeJSON(w, http.StatusInternalServerError, errorResponse{
			Error:   "one or more alerts could not be processed",
			Summary: &summary,
		})
		return
	}

	writeJSON(w, http.StatusOK, summary)
}
