// Package humanapproval applies a human's decision on a tool call that paused
// an agent session.
package humanapproval

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/google/uuid"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
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

// HumanApproval is one decision on a set of tools for one paused session.
type HumanApproval struct {
	EventID   string   `json:"event_id"`
	SessionID string   `json:"session_id"`
	Tools     []string `json:"tools"`
	Decision  Decision `json:"decision"`
}

func (a HumanApproval) Validate() error {
	if err := a.validateRequest(); err != nil {
		return err
	}
	if a.Decision != DecisionApproved && a.Decision != DecisionDeclined {
		return fmt.Errorf("%w: decision must be %q or %q, got %q",
			ErrInvalid, DecisionApproved, DecisionDeclined, a.Decision)
	}
	return nil
}

// validateRequest checks everything but the decision, which a request still
// awaiting a human does not have.
func (a HumanApproval) validateRequest() error {
	if err := uuid.Validate(a.EventID); err != nil {
		return fmt.Errorf("%w: event_id %q is not a uuid", ErrInvalid, a.EventID)
	}
	if err := uuid.Validate(a.SessionID); err != nil {
		return fmt.Errorf("%w: session_id %q is not a uuid", ErrInvalid, a.SessionID)
	}
	if len(a.Tools) == 0 {
		return fmt.Errorf("%w: tools must name at least one tool", ErrInvalid)
	}
	if slices.Contains(a.Tools, "") {
		return fmt.Errorf("%w: tools must not contain an empty name", ErrInvalid)
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

// Lookup returns the stored event humanApproval would resume, after the same
// checks Process makes. The decision is ignored, so a request still awaiting a
// human passes.
func (ap *Approver) Lookup(ctx context.Context, humanApproval HumanApproval) (event.Event, error) {
	if err := humanApproval.validateRequest(); err != nil {
		return event.Event{}, err
	}
	ev, found, err := ap.store.GetEvent(ctx, humanApproval.EventID)
	if err != nil {
		return event.Event{}, err
	}
	if !found {
		return event.Event{}, fmt.Errorf("%w: %s", ErrEventNotFound, humanApproval.EventID)
	}
	if ev.SessionID != "" && ev.SessionID != humanApproval.SessionID {
		return event.Event{}, fmt.Errorf("%w: event %s is on session %s, not %s",
			ErrSessionMismatch, humanApproval.EventID, ev.SessionID, humanApproval.SessionID)
	}
	return ev, nil
}

// Process applies humanApproval. On approval it records every tool, then
// republishes the stored event once, carrying the session id so the harness
// resumes that session. Recording first makes a retry after a failed publish safe.
func (ap *Approver) Process(ctx context.Context, humanApproval HumanApproval) error {
	if err := humanApproval.Validate(); err != nil {
		return err
	}
	log := ap.log.With("event_id", humanApproval.EventID, "session_id", humanApproval.SessionID,
		"tools", humanApproval.Tools, "decision", string(humanApproval.Decision))

	ev, err := ap.Lookup(ctx, humanApproval)
	if err != nil {
		return err
	}

	if humanApproval.Decision == DecisionDeclined {
		log.Info("tool calls declined by human")
		return nil
	}

	if err := ap.store.ApproveTools(ctx, humanApproval.EventID, humanApproval.SessionID, humanApproval.Tools); err != nil {
		if errors.Is(err, store.ErrApprovalConflict) {
			return fmt.Errorf("%w: %v", ErrSessionMismatch, err)
		}
		return err
	}

	ev.SessionID = humanApproval.SessionID
	if err := ap.publisher.Publish(ctx, ev); err != nil {
		log.Error("tools approved but event was not republished", "error", err)
		return err
	}
	log.Info("tools approved; event republished")
	return nil
}
