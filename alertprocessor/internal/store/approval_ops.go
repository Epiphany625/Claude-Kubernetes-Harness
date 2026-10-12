package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrApprovalConflict means the session's approval row belongs to a different event.
var ErrApprovalConflict = errors.New("session already has an approval for a different event")

// approveToolSQL appends tool to the session's approval row, creating it on the
// first approval. Approving the same tool twice is a no-op. A row owned by a
// different event is left untouched and no row is returned.
const approveToolSQL = `
INSERT INTO approval (event_id, session_id, approved_tools)
VALUES ($1, $2, ARRAY[$3::text])
ON CONFLICT (session_id) DO UPDATE
    SET approved_tools = CASE
        WHEN $3::text = ANY (approval.approved_tools) THEN approval.approved_tools
        ELSE array_append(approval.approved_tools, $3::text)
    END
    WHERE approval.event_id = EXCLUDED.event_id
RETURNING id
`

// ApproveTool implements Store.
func (s *PostgresStore) ApproveTool(ctx context.Context, eventID, sessionID, tool string) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	var id string
	err := s.pool.QueryRow(ctx, approveToolSQL, eventID, sessionID, tool).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("approve tool %s for session %s on event %s: %w",
			tool, sessionID, eventID, ErrApprovalConflict)
	}
	if err != nil {
		return fmt.Errorf("approve tool %s for session %s on event %s: %w",
			tool, sessionID, eventID, err)
	}
	return nil
}
