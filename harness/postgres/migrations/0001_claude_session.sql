-- The claude_session table: backing store for the Agent SDK's SessionStore
-- (postgres.ClaudeSessionStore). One row per transcript entry, append-only.
--
-- Every statement is idempotent, so re-running the file is the recovery path.

CREATE TABLE IF NOT EXISTS claude_session (
    -- Global insertion order; load() replays a transcript ORDER BY seq.
    seq         BIGSERIAL   PRIMARY KEY,

    project_key TEXT        NOT NULL,
    -- TEXT, not UUID: the SDK hands back session ids as plain strings.
    session_id  TEXT        NOT NULL,
    -- '' is the main transcript; anything else is a subagent's subpath.
    subpath     TEXT        NOT NULL DEFAULT '',

    entry       JSONB       NOT NULL,
    -- Epoch milliseconds, as SessionStoreListEntry.mtime expects.
    mtime       BIGINT      NOT NULL
);

-- load, delete and list_subkeys all filter on this prefix; seq keeps load's
-- ORDER BY an index scan.
CREATE INDEX IF NOT EXISTS claude_session_key_idx
    ON claude_session (project_key, session_id, subpath, seq);

-- list_sessions: main transcripts only, grouped by session with MAX(mtime).
CREATE INDEX IF NOT EXISTS claude_session_main_mtime_idx
    ON claude_session (project_key, session_id, mtime)
    WHERE subpath = '';
