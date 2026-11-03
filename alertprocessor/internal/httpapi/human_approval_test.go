package httpapi_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
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
		`","tools":["mcp__kubemcp__scale_deployment","mcp__kubemcp__patch_resource"],"decision":"` + decision + `"}`
}

func TestHumanApprovalApprovedRepublishes(t *testing.T) {
	h := newHarness(t)
	h.store.stored = map[string]event.Event{approvalEventID: {EventID: approvalEventID}}

	rec := h.postApproval(t, approvalBody(approvalEventID, "approved"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if len(h.store.approved) != 2 {
		t.Errorf("approved = %v, want two entries", h.store.approved)
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
		{"tool as a string", strings.Replace(approvalBody(approvalEventID, "approved"), `"tools":[`, `"tool":"x","tools":[`, 1), nil, nil, http.StatusBadRequest},
		{"empty tools", `{"event_id":"` + approvalEventID + `","session_id":"` + approvalSessionID + `","tools":[],"decision":"approved"}`, nil, nil, http.StatusBadRequest},
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

func approvalPageURL(eventID, actions string) string {
	query := url.Values{
		"event_id":   {eventID},
		"session_id": {approvalSessionID},
		"summary":    {"web pods crash-loop on a bad image tag"},
		"actions":    {actions},
	}
	return "/api/v1/approvals?" + query.Encode()
}

const approvalPageActions = `[
	{"tool":"mcp__kubemcp__patch_resource","arguments":{"name":"web","image":"nginx:1.27"},"intent":"roll web back to a tag that exists","risk":"medium"},
	{"tool":"mcp__kubemcp__rollout_restart","arguments":{"name":"web"},"intent":"restart <web> pods","risk":"low"}
]`

func TestApprovalPageRendersProposedWrites(t *testing.T) {
	h := newHarness(t)
	h.store.stored = map[string]event.Event{approvalEventID: {EventID: approvalEventID, AlertName: "KubePodCrashLooping"}}

	rec := h.get(t, approvalPageURL(approvalEventID, approvalPageActions))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"KubePodCrashLooping",
		"web pods crash-loop on a bad image tag",
		"mcp__kubemcp__patch_resource",
		"roll web back to a tag that exists",
		`&#34;image&#34;: &#34;nginx:1.27&#34;`,
		"restart &lt;web&gt; pods",
		`"tools":["mcp__kubemcp__patch_resource","mcp__kubemcp__rollout_restart"]`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("page is missing %q", want)
		}
	}
	if len(h.store.approved) != 0 || h.pub.count() != 0 {
		t.Errorf("GET changed state: approved=%v published=%d", h.store.approved, h.pub.count())
	}
}

func TestApprovalPageStatusCodes(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want int
	}{
		{"no actions", approvalPageURL(approvalEventID, ""), http.StatusBadRequest},
		{"actions not json", approvalPageURL(approvalEventID, "[{"), http.StatusBadRequest},
		{"empty actions", approvalPageURL(approvalEventID, "[]"), http.StatusBadRequest},
		{"bad event id", approvalPageURL("nope", approvalPageActions), http.StatusBadRequest},
		{"unknown event", approvalPageURL("33333333-3333-4333-8333-333333333333", approvalPageActions), http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.store.stored = map[string]event.Event{approvalEventID: {EventID: approvalEventID}}

			if rec := h.get(t, tc.url); rec.Code != tc.want {
				t.Errorf("status = %d, want %d; body %s", rec.Code, tc.want, rec.Body)
			}
		})
	}
}
