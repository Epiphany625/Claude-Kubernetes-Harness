from dataclasses import replace

from mq.event import Status, Event
from postgres.postgres import PostgresClientABC


class PostgresClientMock(PostgresClientABC):
    def __init__(
        self,
        events: list[Event] | None = None,
        approvedTools: dict[str, set[str]] | None = None,
    ):
        self.events: dict[str, Event] = {e.eventID: e for e in events or []}  # eventID: event
        self.approvedTools: dict[str, set[str]] = dict(approvedTools or {})  # sessionID: approved tools
        self.approvalTried: set[str] = set()

    async def insertEvent(self, event: Event) -> None:
        if event.eventID in self.events:
            raise ValueError(f"insert event {event.eventID}: already exists")
        self.events[event.eventID] = event

    async def insertSessionID(self, eventID: str, sessionID: str) -> None:
        if eventID not in self.events:
            raise LookupError(f"set session {sessionID} on event {eventID}: no such event")
        self.events[eventID] = replace(self.events[eventID], sessionID=sessionID)

    async def updateEventState(self, eventID: str, newState: Status) -> None:
        if eventID not in self.events:
            raise LookupError(f"set status {newState} on event {eventID}: no such event")
        self.events[eventID] = replace(self.events[eventID], status=newState)

    async def getApprovedTools(self, sessionID: str) -> set[str]:
        self.approvalTried.add(sessionID)
        return set(self.approvedTools.get(sessionID, set()))
