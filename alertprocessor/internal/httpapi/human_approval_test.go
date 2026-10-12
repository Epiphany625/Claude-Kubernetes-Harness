package httpapi_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
)

const (
	approvalEventID   = "11111111-1111-4111-8111-111111111111"
	approvalSessionID = "22222222-2222-4222-8222-222222222222"
)

func (h *harness) postApproval(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/approvals", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.server.Handler().ServeHTTP(rec, req)
	return rec
}

func approvalBody(eventID, decision string) string {
	return `{"event_id":"` + eventID + `","session_id":"` + approvalSessionID +
		`","tool":"mcp__kubemcp__scale_deployment","decision":"` + decision + `"}`
}

func TestHumanApprovalApprovedRepublishes(t *testing.T) {
	h := newHarness(t)
	h.store.stored = map[string]event.Event{approvalEventID: {EventID: approvalEventID}}

	rec := h.postApproval(t, approvalBody(approvalEventID, "approved"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if len(h.store.approved) != 1 {
		t.Errorf("approved = %v, want one entry", h.store.approved)
	}
	if h.pub.count() != 1 || h.pub.published[0].SessionID != approvalSessionID {
		t.Errorf("published %+v, want one event carrying the session id", h.pub.published)
	}
}

func TestHumanApprovalStatusCodes(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		getErr     error
		publishErr error
		want       int
	}{
		{"declined", approvalBody(approvalEventID, "declined"), nil, nil, http.StatusOK},
		{"malformed json", `{`, nil, nil, http.StatusBadRequest},
		{"unknown field", `{"eventID":"` + approvalEventID + `"}`, nil, nil, http.StatusBadRequest},
		{"invalid decision", approvalBody(approvalEventID, "maybe"), nil, nil, http.StatusBadRequest},
		{"unknown event", approvalBody("33333333-3333-4333-8333-333333333333", "approved"), nil, nil, http.StatusNotFound},
		{"store down", approvalBody(approvalEventID, "approved"), errors.New("db down"), nil, http.StatusInternalServerError},
		{"broker down", approvalBody(approvalEventID, "approved"), nil, errors.New("broker down"), http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.store.stored = map[string]event.Event{approvalEventID: {EventID: approvalEventID}}
			h.store.getErr = tc.getErr
			h.pub.err = tc.publishErr

			if rec := h.postApproval(t, tc.body); rec.Code != tc.want {
				t.Errorf("status = %d, want %d; body %s", rec.Code, tc.want, rec.Body)
			}
		})
	}
}

func TestHumanApprovalRejectsGet(t *testing.T) {
	h := newHarness(t)
	if rec := h.get(t, "/api/v1/approvals"); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}
