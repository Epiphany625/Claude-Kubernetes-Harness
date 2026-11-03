package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrApprovalConflict means the session's approval row belongs to a different event.
var ErrApprovalConflict = errors.New("session already has an approval for a different event")

// approveToolsSQL appends tools to the session's approval row, creating it on
// the first approval. Tools already approved are skipped, so approving the same
// tool twice is a no-op. A row owned by a different event is left untouched and
// no row is returned. $3 must hold no duplicates.
const approveToolsSQL = `
INSERT INTO approval (event_id, session_id, approved_tools)
VALUES ($1, $2, $3::text[])
ON CONFLICT (session_id) DO UPDATE
    SET approved_tools = approval.approved_tools || ARRAY(
        SELECT t FROM unnest(EXCLUDED.approved_tools) WITH ORDINALITY AS u(t, n)
        WHERE t <> ALL (approval.approved_tools)
        ORDER BY n
    )
    WHERE approval.event_id = EXCLUDED.event_id
RETURNING id
`

// ApproveTools implements Store.
func (s *PostgresStore) ApproveTools(ctx context.Context, eventID, sessionID string, tools []string) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	var id string
	err := s.pool.QueryRow(ctx, approveToolsSQL, eventID, sessionID, dedupe(tools)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("approve tools %v for session %s on event %s: %w",
			tools, sessionID, eventID, ErrApprovalConflict)
	}
	if err != nil {
		return fmt.Errorf("approve tools %v for session %s on event %s: %w",
			tools, sessionID, eventID, err)
	}
	return nil
}

// dedupe drops repeated names, keeping the first occurrence's position.
func dedupe(names []string) []string {
	seen := make(map[string]bool, len(names))
	out := make([]string, 0, len(names))
	for _, name := range names {
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}
