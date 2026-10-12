-- The approval table: one row per human-in-the-loop decision on a paused session.
--
-- Every statement is idempotent, like 0001_event.sql, because Migrate has no
-- version table and re-running the files is the recovery path.

CREATE TABLE IF NOT EXISTS approval (
    -- gen_random_uuid() is built in from PostgreSQL 13 (and on Supabase).
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Not unique: one event may be associated with more than one session, though
    -- that is not the case today.
    event_id       UUID        NOT NULL REFERENCES event (event_id) ON DELETE CASCADE,

    -- The Claude conversation session the approval resumes. At most one approval
    -- per session.
    session_id     UUID        UNIQUE,

    -- Tool names the human approved, e.g. '{mcp__kubemcp__scale_deployment}'.
    approved_tools TEXT[]      NOT NULL DEFAULT '{}'
);

-- Approvals for one event. The foreign key does not index event_id by itself.
CREATE INDEX IF NOT EXISTS approval_event_id_idx
    ON approval (event_id);
