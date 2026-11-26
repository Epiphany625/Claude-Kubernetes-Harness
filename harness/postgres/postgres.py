from typing import Final

import psycopg
from psycopg.types.json import Jsonb

from abc import abstractmethod, ABC

from config.config import PostgresConfig
from mq.event import Status

from claude_agent_sdk import (
    SessionKey,
    SessionListSubkeysKey,
    SessionStore,
    SessionStoreEntry,
    SessionStoreListEntry,
)
from logging import Logger

EVENT_TABLE_NAME: Final = "event"
APPROVAL_TABLE_NAME: Final = "approval"
SESSION_TABLE_NAME: Final = "claude_session"

# The event and approval tables are owned by alertprocessor's migrations.
INSERT_SESSION_ID_SQL: Final = """
UPDATE event SET session_id = %s::uuid, updated_at = now()
 WHERE event_id = %s::uuid"""

UPDATE_EVENT_STATE_SQL: Final = """
UPDATE event SET status = %s, updated_at = now()
 WHERE event_id = %s::uuid"""

GET_APPROVED_TOOLS_SQL: Final = """
SELECT approved_tools FROM approval WHERE session_id = %s::uuid"""

APPEND_SESSION_ENTRIES_SQL: Final = f"""
INSERT INTO {SESSION_TABLE_NAME} (project_key, session_id, subpath, entry, mtime)
SELECT %s, %s, %s, e,
       (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint
  FROM unnest(%s::jsonb[]) WITH ORDINALITY AS t(e, ord)
 ORDER BY ord"""

LOAD_SESSION_ENTRIES_SQL: Final = f"""
SELECT entry FROM {SESSION_TABLE_NAME}
 WHERE project_key = %s AND session_id = %s AND subpath = %s
 ORDER BY seq"""

LIST_SESSIONS_SQL: Final = f"""
SELECT session_id, MAX(mtime) AS mtime FROM {SESSION_TABLE_NAME}
 WHERE project_key = %s AND subpath = ''
 GROUP BY session_id"""

DELETE_SESSION_SUBPATH_SQL: Final = f"""
DELETE FROM {SESSION_TABLE_NAME}
 WHERE project_key = %s AND session_id = %s AND subpath = %s"""

DELETE_SESSION_SQL: Final = f"""
DELETE FROM {SESSION_TABLE_NAME}
 WHERE project_key = %s AND session_id = %s"""

LIST_SESSION_SUBKEYS_SQL: Final = f"""
SELECT DISTINCT subpath FROM {SESSION_TABLE_NAME}
 WHERE project_key = %s AND session_id = %s AND subpath <> ''"""
 
 
# make all stores mockable
class PostgresClientABC(ABC):
    @abstractmethod
    async def start(self) -> None:... 

class EventStoreABC(ABC):
    @abstractmethod
    async def insertSessionID(self, eventID: str, sessionID: str) -> None:...
    @abstractmethod
    async def updateEventState(self, eventID: str, newState: Status) -> None:...

class ApprovalStoreABC(ABC):
    @abstractmethod
    async def getApprovedTools(self, sessionID: str) -> set[str]:...


    

class PostgresClient(PostgresClientABC):
    def __init__(self, config: PostgresConfig):
        self.dsn = config.dsn
        self.connectTimeout = config.connectTimeout
        self.logger = config.logger
        self.connRef: psycopg.AsyncConnection = None
        
        self.eventStore: EventStore = None
        self.approvalStore: ApprovalStore = None
        self.claudeSessionStore: ClaudeSessionStore = None

    # start the connection.
    async def start(self) -> None:
        # Autocommit: every statement here is a single-row write or read.
        self.connRef = await psycopg.AsyncConnection.connect(
            self.dsn, autocommit=True, connect_timeout=int(self.connectTimeout)
        )
        self.eventStore = EventStore(self.connRef, self.logger)
        self.approvalStore = ApprovalStore(self.connRef, self.logger)
        self.claudeSessionStore = ClaudeSessionStore(self.connRef, self.logger)
        self.logger.info("[postgres] connected")

class EventStore(EventStoreABC):
    def __init__(self, conn: psycopg.AsyncConnection, logger: Logger):
        self.logger = logger
        self.conn: psycopg.AsyncConnection = conn
    # insert session ID to an Event.
    async def insertSessionID(self, eventID: str, sessionID: str) -> None:
        cur = await self.conn.execute(INSERT_SESSION_ID_SQL, (sessionID, eventID))
        if cur.rowcount == 0:
            raise LookupError(f"set session {sessionID} on event {eventID}: no such event")

    # update the state of the event.
    async def updateEventState(self, eventID: str, newState: Status) -> None:
        cur = await self.conn.execute(UPDATE_EVENT_STATE_SQL, (str(newState), eventID))
        if cur.rowcount == 0:
            raise LookupError(f"set status {newState} on event {eventID}: no such event")

class ApprovalStore(ApprovalStoreABC):
    def __init__(self, conn: psycopg.AsyncConnection, logger: Logger):
        self.logger = logger
        self.conn: psycopg.AsyncConnection = conn
    # tools a human has approved for the session; empty when none are.
    async def getApprovedTools(self, sessionID: str) -> set[str]:
        cur = await self.conn.execute(GET_APPROVED_TOOLS_SQL, (sessionID,))
        row = await cur.fetchone()
        return set(row[0]) if row else set()

class ClaudeSessionStore(SessionStore):
    def __init__(self, conn: psycopg.AsyncConnection, logger: Logger):
        self.logger = logger
        self.conn: psycopg.AsyncConnection = conn

    async def append(self, key: SessionKey, entries: list[SessionStoreEntry]) -> None:
        if not entries:
            return
        subpath = key.get("subpath") or ""
        # Single round-trip multi-row INSERT: unnest() the jsonb[] payload so
        # the whole batch lands in one statement (atomic, ordered by array
        # position via WITH ORDINALITY, one bigserial draw per row).
        await self.conn.execute(
            APPEND_SESSION_ENTRIES_SQL,
            (key["project_key"], key["session_id"], subpath, [Jsonb(e) for e in entries]),
        )
        self.logger.info(f"[session-store] append {key['session_id']} subpath={subpath!r}: {len(entries)} entries")

    async def load(self, key: SessionKey) -> list[SessionStoreEntry] | None:
        subpath = key.get("subpath") or ""
        cur = await self.conn.execute(
            LOAD_SESSION_ENTRIES_SQL,
            (key["project_key"], key["session_id"], subpath),
        )
        rows = await cur.fetchall()
        self.logger.info(f"[session-store] load {key['session_id']} subpath={subpath!r}: {len(rows)} entries")
        if not rows:
            return None
        # psycopg decodes jsonb into Python objects already.
        return [row[0] for row in rows]

    async def list_sessions(self, project_key: str) -> list[SessionStoreListEntry]:
        cur = await self.conn.execute(LIST_SESSIONS_SQL, (project_key,))
        rows = await cur.fetchall()
        self.logger.info(f"[session-store] list_sessions {project_key}: {len(rows)} sessions")
        return [{"session_id": r[0], "mtime": int(r[1])} for r in rows]

    async def delete(self, key: SessionKey) -> None:
        subpath = key.get("subpath")
        if subpath:
            # Targeted: remove just this subpath's rows.
            cur = await self.conn.execute(
                DELETE_SESSION_SUBPATH_SQL,
                (key["project_key"], key["session_id"], subpath),
            )
            self.logger.info(f"[session-store] delete {key['session_id']} subpath={subpath!r}: {cur.rowcount} rows")
            return
        # Cascade: main + every subpath under this (project_key, session_id).
        cur = await self.conn.execute(
            DELETE_SESSION_SQL, (key["project_key"], key["session_id"])
        )
        self.logger.info(f"[session-store] delete {key['session_id']} (all subpaths): {cur.rowcount} rows")

    async def list_subkeys(self, key: SessionListSubkeysKey) -> list[str]:
        cur = await self.conn.execute(
            LIST_SESSION_SUBKEYS_SQL, (key["project_key"], key["session_id"])
        )
        rows = await cur.fetchall()
        self.logger.info(f"[session-store] list_subkeys {key['session_id']}: {len(rows)} subkeys")
        return [r[0] for r in rows]
