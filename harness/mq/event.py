"""The consuming side of the event contract.

One RabbitMQ message body is one JSON-encoded event, produced by
alertprocessor/internal/event.Event. The JSON keys -- not the Go field names --
are the contract, and this module is the only place in the harness that spells
them out: a rename upstream is a change to `Event.fromDict` and nowhere else.

The distinction the producer draws is worth keeping here too. An *alert* is a
condition Prometheus observes and keeps one identity for as long as it fires;
an *event* is one notification about that alert. alertID and eventID are
therefore unrelated, and one alert yields several events over its life.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping


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


class AlertState(StrEnum):
    """Firing/resolved state as Alertmanager reported it -- what the world is
    doing, as opposed to Status, which is what we are doing about it. An alert
    can be resolved in Prometheus while its event is still SOLVING."""

    FIRING = "firing"
    RESOLVED = "resolved"


def _parseEnum(enum: type[StrEnum], value: Any) -> Any:
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


def _parseTime(value: Any) -> datetime | None:
    """Parse one RFC 3339 timestamp as Go's encoding/json writes it.

    Go emits up to nanosecond precision; datetime resolves to microseconds and
    truncates the rest, which is finer than anything here needs.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _parseText(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _parseMapping(value: Any) -> dict[str, str]:
    # A nil Go map marshals to null, not {}.
    if not isinstance(value, Mapping):
        return {}
    return {str(k): str(v) for k, v in value.items()}


@dataclass(frozen=True)
class Event:
    """One alert notification the pipeline has accepted responsibility for."""

    eventID: str = ""
    alertID: str = ""
    status: Status | str = ""
    alertState: AlertState | str = ""

    alertName: str = ""
    severity: str = ""
    namespace: str = ""

    # The most specific Kubernetes object the alert's labels name -- the pod,
    # else the deployment, else the node. A convenience projection; labels
    # remains the authority.
    resource: str = ""

    summary: str = ""
    description: str = ""
    runbookURL: str = ""
    generatorURL: str = ""

    startsAt: datetime | None = None
    endsAt: datetime | None = None

    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)

    # The alert exactly as Alertmanager sent it, so an event can be re-read
    # after the projection above changes.
    rawAlert: Any = None

    receivedAt: datetime | None = None

    @classmethod
    def fromDict(cls, payload: Mapping[str, Any]) -> "Event":
        """Project a decoded message body onto this class.

        Missing keys become empty values rather than errors: every optional
        field on the producer carries `omitempty`, so absence is normal and
        says only that the alert had no such label.
        """
        return cls(
            eventID=_parseText(payload.get("event_id")),
            alertID=_parseText(payload.get("alert_id")),
            status=_parseEnum(Status, payload.get("status")),
            # The wire name is alert_status; the producer's field is AlertState.
            alertState=_parseEnum(AlertState, payload.get("alert_status")),
            alertName=_parseText(payload.get("alert_name")),
            severity=_parseText(payload.get("severity")),
            namespace=_parseText(payload.get("namespace")),
            resource=_parseText(payload.get("resource")),
            summary=_parseText(payload.get("summary")),
            description=_parseText(payload.get("description")),
            runbookURL=_parseText(payload.get("runbook_url")),
            generatorURL=_parseText(payload.get("generator_url")),
            startsAt=_parseTime(payload.get("starts_at")),
            endsAt=_parseTime(payload.get("ends_at")),
            labels=_parseMapping(payload.get("labels")),
            annotations=_parseMapping(payload.get("annotations")),
            rawAlert=payload.get("raw_alert"),
            receivedAt=_parseTime(payload.get("received_at")),
        )

    @classmethod
    def fromMessage(cls, body: bytes) -> "Event":
        """Decode one AMQP message body. Raises ValueError on anything that is
        not a JSON object -- including invalid UTF-8, whose UnicodeDecodeError
        is itself a ValueError."""
        payload = json.loads(body)
        if not isinstance(payload, Mapping):
            raise ValueError(f"event body must be a JSON object, got {type(payload).__name__}")
        return cls.fromDict(payload)

    @property
    def isFiring(self) -> bool:
        return self.alertState == AlertState.FIRING

    def summaryLine(self) -> str:
        """One line: what fired, how bad, and where."""
        parts = [self.alertName or "(unnamed alert)"]
        if self.severity:
            parts.append(f"[{self.severity}]")
        if self.alertState:
            parts.append(f"{self.alertState}")
        where = "/".join(p for p in (self.namespace, self.resource) if p)
        if where:
            parts.append(f"on {where}")
        return " ".join(parts)

    def describe(self) -> str:
        """A readable block for the console, listing only what the event has."""
        rows: list[tuple[str, str]] = [
            ("event", self.eventID),
            ("alert", self.alertID),
            ("status", str(self.status)),
            ("state", str(self.alertState)),
            ("severity", self.severity),
            ("namespace", self.namespace),
            ("resource", self.resource),
            ("summary", self.summary),
            ("description", self.description),
            ("runbook", self.runbookURL),
            ("generator", self.generatorURL),
            ("starts", _formatTime(self.startsAt)),
            ("ends", _formatTime(self.endsAt)),
            ("received", _formatTime(self.receivedAt)),
            ("labels", _formatMapping(self.labels)),
            ("annotations", _formatMapping(self.annotations)),
        ]
        width = max(len(name) for name, _ in rows)
        lines = [self.summaryLine()]
        lines += [f"  {name:<{width}}  {value}" for name, value in rows if value]
        return "\n".join(lines)


def _formatTime(value: datetime | None) -> str:
    return value.isoformat() if value else ""


def _formatMapping(values: Mapping[str, str]) -> str:
    return ", ".join(f"{k}={v}" for k, v in sorted(values.items()))
