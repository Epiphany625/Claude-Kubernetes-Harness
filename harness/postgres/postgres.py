from typing import Final

import psycopg

from config.config import PostgresConfig
from mq.event import Status

# The event and approval tables are owned by alertprocessor's migrations.
INSERT_SESSION_ID_SQL: Final = """
UPDATE event SET session_id = %s::uuid, updated_at = now()
 WHERE event_id = %s::uuid"""

UPDATE_EVENT_STATE_SQL: Final = """
UPDATE event SET status = %s, updated_at = now()
 WHERE event_id = %s::uuid"""

GET_APPROVED_TOOLS_SQL: Final = """
SELECT approved_tools FROM approval WHERE session_id = %s::uuid"""


class PostgresClient:
    def __init__(self, config: PostgresConfig):
        self.dsn = config.dsn
        self.connectTimeout = config.connectTimeout
        self.logger = config.logger
        self.conn: psycopg.AsyncConnection | None = None

    # start the connection.
    async def start(self) -> None:
        # Autocommit: every statement here is a single-row write or read.
        self.conn = await psycopg.AsyncConnection.connect(
            self.dsn, autocommit=True, connect_timeout=int(self.connectTimeout)
        )
        self.logger.info("[postgres] connected")

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    def _connection(self) -> psycopg.AsyncConnection:
        if self.conn is None:
            raise RuntimeError("postgres client is not started")
        return self.conn

    # insert session ID to an Event.
    async def insertSessionID(self, eventID: str, sessionID: str) -> None:
        cur = await self._connection().execute(INSERT_SESSION_ID_SQL, (sessionID, eventID))
        if cur.rowcount == 0:
            raise LookupError(f"set session {sessionID} on event {eventID}: no such event")

    # update the state of the event.
    async def updateEventState(self, eventID: str, newState: Status) -> None:
        cur = await self._connection().execute(UPDATE_EVENT_STATE_SQL, (str(newState), eventID))
        if cur.rowcount == 0:
            raise LookupError(f"set status {newState} on event {eventID}: no such event")

    # tools a human has approved for the session; empty when none are.
    async def getApprovedTools(self, sessionID: str) -> set[str]:
        cur = await self._connection().execute(GET_APPROVED_TOOLS_SQL, (sessionID,))
        row = await cur.fetchone()
        return set(row[0]) if row else set()
