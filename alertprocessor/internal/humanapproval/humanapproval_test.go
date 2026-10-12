package humanapproval

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/store"
)

const (
	eventID   = "11111111-1111-4111-8111-111111111111"
	sessionID = "22222222-2222-4222-8222-222222222222"
	otherID   = "33333333-3333-4333-8333-333333333333"
	tool      = "mcp__kubemcp__scale_deployment"
)

type fakeStore struct {
	store.Store
	events     map[string]event.Event
	approveErr error
	approved   []string
}

func (f *fakeStore) GetEvent(_ context.Context, id string) (event.Event, bool, error) {
	ev, ok := f.events[id]
	return ev, ok, nil
}

func (f *fakeStore) ApproveTool(_ context.Context, eventID, sessionID, tool string) error {
	if f.approveErr != nil {
		return f.approveErr
	}
	f.approved = append(f.approved, eventID+"/"+sessionID+"/"+tool)
	return nil
}

type fakePublisher struct {
	err       error
	published []event.Event
}

func (f *fakePublisher) Publish(_ context.Context, ev event.Event) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, ev)
	return nil
}

func (f *fakePublisher) Ping(context.Context) error { return nil }

func newApprover(ev event.Event) (*Approver, *fakeStore, *fakePublisher) {
	st := &fakeStore{events: map[string]event.Event{ev.EventID: ev}}
	pub := &fakePublisher{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewApprover(st, pub, log), st, pub
}

func approval(d Decision) HumanApproval {
	return HumanApproval{EventID: eventID, SessionID: sessionID, Tool: tool, Decision: d}
}

func TestApprovedRecordsToolThenRepublishesWithSession(t *testing.T) {
	ap, st, pub := newApprover(event.Event{EventID: eventID, AlertName: "KubePodCrashLooping"})

	if err := ap.Process(context.Background(), approval(DecisionApproved)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if want := eventID + "/" + sessionID + "/" + tool; len(st.approved) != 1 || st.approved[0] != want {
		t.Errorf("approved = %v, want [%s]", st.approved, want)
	}
	if len(pub.published) != 1 {
		t.Fatalf("published %d events, want 1", len(pub.published))
	}
	got := pub.published[0]
	if got.EventID != eventID || got.SessionID != sessionID || got.AlertName != "KubePodCrashLooping" {
		t.Errorf("republished %+v, want the stored event with session_id set", got)
	}
}

func TestDeclinedTouchesNothing(t *testing.T) {
	ap, st, pub := newApprover(event.Event{EventID: eventID})

	if err := ap.Process(context.Background(), approval(DecisionDeclined)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(st.approved) != 0 || len(pub.published) != 0 {
		t.Errorf("approved=%v published=%d, want neither", st.approved, len(pub.published))
	}
}

func TestProcessErrors(t *testing.T) {
	tests := []struct {
		name       string
		stored     event.Event
		approval   HumanApproval
		approveErr error
		publishErr error
		want       error
	}{
		{
			name:     "bad event id",
			stored:   event.Event{EventID: eventID},
			approval: HumanApproval{EventID: "nope", SessionID: sessionID, Tool: tool, Decision: DecisionApproved},
			want:     ErrInvalid,
		},
		{
			name:     "missing tool",
			stored:   event.Event{EventID: eventID},
			approval: HumanApproval{EventID: eventID, SessionID: sessionID, Decision: DecisionApproved},
			want:     ErrInvalid,
		},
		{
			name:     "unknown decision",
			stored:   event.Event{EventID: eventID},
			approval: approval("maybe"),
			want:     ErrInvalid,
		},
		{
			name:     "event not found",
			stored:   event.Event{EventID: otherID},
			approval: approval(DecisionApproved),
			want:     ErrEventNotFound,
		},
		{
			name:     "event on another session",
			stored:   event.Event{EventID: eventID, SessionID: otherID},
			approval: approval(DecisionApproved),
			want:     ErrSessionMismatch,
		},
		{
			name:       "approval row owned by another event",
			stored:     event.Event{EventID: eventID},
			approval:   approval(DecisionApproved),
			approveErr: fmt.Errorf("wrapped: %w", store.ErrApprovalConflict),
			want:       ErrSessionMismatch,
		},
		{
			name:       "publish fails",
			stored:     event.Event{EventID: eventID},
			approval:   approval(DecisionApproved),
			publishErr: errors.New("broker down"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ap, st, pub := newApprover(tc.stored)
			st.approveErr = tc.approveErr
			pub.err = tc.publishErr

			err := ap.Process(context.Background(), tc.approval)
			if err == nil {
				t.Fatal("Process succeeded, want an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
			if tc.publishErr == nil && len(pub.published) != 0 {
				t.Errorf("published %d events on a rejected approval", len(pub.published))
			}
		})
	}
}
