from enum import StrEnum
from typing import Any
class Status(StrEnum):
    """Lifecycle state of an event -- what the harness has done about it.

    alertprocessor only ever writes WAITING; every later transition belongs to
    this service. The spellings are a cross-service contract, constrained by the
    event_status_valid CHECK in the producer's 0001_event.sql.
    """
    WAITING = "waiting"
    SOLVING = "solving"
    WAITING_HUMAN_APPROVAL = "waiting_human_approval"
    RESOLVED = "resolved"
    FAILED_TO_RESOLVE = "failed_to_resolve"
    UNKNOWN = "unknown" # added a new UNKNOWN status. TODO. maybe we should update database schema? 


class AlertState(StrEnum):
    """Firing/resolved state as Alertmanager reported it -- what the world is
    doing, as opposed to Status, which is what we are doing about it. An alert
    can be resolved in Prometheus while its event is still SOLVING."""

    FIRING = "firing"
    RESOLVED = "resolved"

def parseEnum(enum: type[StrEnum], value: Any) -> Any:
    """Coerce to a known member, else hand back the raw string.
    A value we do not recognise is a vocabulary that grew without us. That is
    worth seeing in a log line, and not worth dropping the message over.
    """
    if not isinstance(value, str):
        return ""
    try:
        return enum(value)
    except ValueError:
        return value