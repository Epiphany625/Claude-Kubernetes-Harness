// Package humanapproval applies a human's decision on a tool call that paused
// an agent session.
package humanapproval

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/queue"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/store"
)

type Decision string

const (
	DecisionApproved Decision = "approved"
	DecisionDeclined Decision = "declined"
)

var (
	ErrInvalid         = errors.New("invalid human approval")
	ErrEventNotFound   = errors.New("event not found")
	ErrSessionMismatch = errors.New("session does not belong to event")
)

// HumanApproval is one decision on one tool for one paused session.
type HumanApproval struct {
	EventID   string   `json:"event_id"`
	SessionID string   `json:"session_id"`
	Tool      string   `json:"tool"`
	Decision  Decision `json:"decision"`
}

func (a HumanApproval) Validate() error {
	if err := uuid.Validate(a.EventID); err != nil {
		return fmt.Errorf("%w: event_id %q is not a uuid", ErrInvalid, a.EventID)
	}
	if err := uuid.Validate(a.SessionID); err != nil {
		return fmt.Errorf("%w: session_id %q is not a uuid", ErrInvalid, a.SessionID)
	}
	if a.Tool == "" {
		return fmt.Errorf("%w: tool is required", ErrInvalid)
	}
	if a.Decision != DecisionApproved && a.Decision != DecisionDeclined {
		return fmt.Errorf("%w: decision must be %q or %q, got %q",
			ErrInvalid, DecisionApproved, DecisionDeclined, a.Decision)
	}
	return nil
}

// Approver applies human approvals. Safe for concurrent use.
type Approver struct {
	store     store.Store
	publisher queue.Publisher
	log       *slog.Logger
}

func NewApprover(s store.Store, p queue.Publisher, log *slog.Logger) *Approver {
	return &Approver{store: s, publisher: p, log: log}
}

// Process applies a. On approval it records the tool, then republishes the
// stored event carrying the session id so the harness resumes that session.
// Recording first makes a retry after a failed publish safe.
func (ap *Approver) Process(ctx context.Context, a HumanApproval) error {
	if err := a.Validate(); err != nil {
		return err
	}
	log := ap.log.With("event_id", a.EventID, "session_id", a.SessionID,
		"tool", a.Tool, "decision", string(a.Decision))

	ev, found, err := ap.store.GetEvent(ctx, a.EventID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: %s", ErrEventNotFound, a.EventID)
	}
	if ev.SessionID != "" && ev.SessionID != a.SessionID {
		return fmt.Errorf("%w: event %s is on session %s, not %s",
			ErrSessionMismatch, a.EventID, ev.SessionID, a.SessionID)
	}

	if a.Decision == DecisionDeclined {
		log.Info("tool call declined by human")
		return nil
	}

	if err := ap.store.ApproveTool(ctx, a.EventID, a.SessionID, a.Tool); err != nil {
		if errors.Is(err, store.ErrApprovalConflict) {
			return fmt.Errorf("%w: %v", ErrSessionMismatch, err)
		}
		return err
	}

	ev.SessionID = a.SessionID
	if err := ap.publisher.Publish(ctx, ev); err != nil {
		log.Error("tool approved but event was not republished", "error", err)
		return err
	}
	log.Info("tool approved; event republished")
	return nil
}
