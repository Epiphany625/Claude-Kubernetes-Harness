package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/humanapproval"
)

const maxApprovalBodyBytes = 64 << 10

type approvalResponse struct {
	EventID   string                 `json:"event_id"`
	SessionID string                 `json:"session_id"`
	Tool      string                 `json:"tool"`
	Decision  humanapproval.Decision `json:"decision"`
}

// handleHumanApproval receives a human's decision on a paused session's tool
// call. 4xx means the request will never succeed as sent; 5xx means a store or
// broker failure and the caller should retry, which is safe.
func (s *Server) handleHumanApproval(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxApprovalBodyBytes)

	var a humanapproval.HumanApproval
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&a); err != nil {
		s.log.Warn("rejected malformed human approval",
			"remote_addr", r.RemoteAddr, "error", err)
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	err := s.approver.Process(r.Context(), a)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, approvalResponse{
			EventID: a.EventID, SessionID: a.SessionID, Tool: a.Tool, Decision: a.Decision,
		})
	case errors.Is(err, humanapproval.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
	case errors.Is(err, humanapproval.ErrEventNotFound):
		writeJSON(w, http.StatusNotFound, errorResponse{Error: err.Error()})
	case errors.Is(err, humanapproval.ErrSessionMismatch):
		writeJSON(w, http.StatusConflict, errorResponse{Error: err.Error()})
	default:
		s.log.Error("human approval failed", "event_id", a.EventID, "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
	}
}
