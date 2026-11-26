from dataclasses import replace

from mq.event import Status, Event
from postgres.postgres import PostgresClientABC, EventStoreABC, ApprovalStoreABC

class EventStoreMock(EventStoreABC):
    def __init__(self):
        self.events: dict[str, Event] = {}  # eventID: event
        
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


class ApprovalStoreMock(ApprovalStoreABC):
    def __init__(self):
        self.approvedTools: dict[str, set[str]] = {}  # sessionID: approved tools
        self.approvalTried: set[str] = set()
    async def getApprovedTools(self, sessionID: str) -> set[str]:
        self.approvalTried.add(sessionID)
        return set(self.approvedTools.get(sessionID, set()))
class PostgresClientMock(PostgresClientABC):
    def __init__(self):
        self.eventStore = EventStoreMock()
        self.approvalStore = ApprovalStoreMock()
        self.claudeSessionStore = None # disable claude session store because it's irrelevant for unit test.

    async def start(self) -> None:
        pass



