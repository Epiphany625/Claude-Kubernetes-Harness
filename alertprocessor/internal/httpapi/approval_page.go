package httpapi

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"slices"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/humanapproval"
)

//go:embed templates/approval_page.html
var templateFS embed.FS

var approvalPageTemplate = template.Must(template.ParseFS(templateFS, "templates/approval_page.html"))

// proposedAction is one cluster write the agent asks a human to approve, as
// the harness encodes it in the page link's actions parameter.
type proposedAction struct {
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
	Intent    string          `json:"intent"`
	Risk      string          `json:"risk"`
}

type approvalPageAction struct {
	Tool      string
	Arguments string
	Intent    string
	Risk      string
}

// approvalPageRequest is what the page's buttons POST back, minus the decision.
type approvalPageRequest struct {
	EventID   string   `json:"event_id"`
	SessionID string   `json:"session_id"`
	Tools     []string `json:"tools"`
}

type approvalPage struct {
	Error   string
	Summary string
	Event   event.Event
	Actions []approvalPageAction
	Request approvalPageRequest
}

// handleApprovalPage renders the proposed writes with Approve and Decline
// buttons. It changes nothing: mail scanners open links, so only the buttons'
// POST to handleHumanApproval decides.
func (s *Server) handleApprovalPage(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	page := approvalPage{
		Summary: query.Get("summary"),
		Request: approvalPageRequest{EventID: query.Get("event_id"), SessionID: query.Get("session_id")},
	}

	actions, err := parseProposedActions(query.Get("actions"))
	if err != nil {
		page.Error = err.Error()
		s.renderApprovalPage(w, http.StatusBadRequest, page)
		return
	}
	for _, action := range actions {
		page.Actions = append(page.Actions, approvalPageAction{
			Tool: action.Tool, Arguments: indentJSON(action.Arguments), Intent: action.Intent, Risk: action.Risk,
		})
		if !slices.Contains(page.Request.Tools, action.Tool) {
			page.Request.Tools = append(page.Request.Tools, action.Tool)
		}
	}

	page.Event, err = s.approver.Lookup(r.Context(), humanapproval.HumanApproval{
		EventID: page.Request.EventID, SessionID: page.Request.SessionID, Tools: page.Request.Tools,
	})
	if err != nil {
		page.Error = err.Error()
		s.renderApprovalPage(w, approvalErrorStatus(err), page)
		return
	}
	s.renderApprovalPage(w, http.StatusOK, page)
}

func parseProposedActions(raw string) ([]proposedAction, error) {
	if raw == "" {
		return nil, errors.New("the link has no actions to approve")
	}
	var actions []proposedAction
	if err := json.Unmarshal([]byte(raw), &actions); err != nil {
		return nil, fmt.Errorf("the link's actions are not valid JSON: %w", err)
	}
	return actions, nil
}

func indentJSON(raw json.RawMessage) string {
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return string(raw)
	}
	return out.String()
}

func (s *Server) renderApprovalPage(w http.ResponseWriter, code int, page approvalPage) {
	var body bytes.Buffer
	if err := approvalPageTemplate.Execute(&body, page); err != nil {
		s.log.Error("render approval page", "error", err)
		http.Error(w, "failed to render approval page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	_, _ = body.WriteTo(w)
}
