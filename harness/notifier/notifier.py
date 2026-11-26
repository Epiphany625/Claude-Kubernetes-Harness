from abc import ABC, abstractmethod
from typing import Any, Literal

from pydantic import BaseModel


# one cluster write awaiting approval; mirrors an item of ASK_HUMAN_APPROVAL_SCHEMA["actions"].
class Action(BaseModel):
    tool: str
    arguments: dict[str, Any]
    intent: str
    risk: Literal["low", "medium", "high"]


class Notifier(ABC):
    # a method to be overridden.
    @abstractmethod
    def notify(self, subject: str, actions: list[Action], event_id: str, session_id: str) -> None: ...


# stores a set of notifiers.
class NotifierSet:
    def __init__(self, notifiers: list[Notifier]):
        self.notifiers = notifiers

    def notify(self, subject: str, actions: list[Action], event_id: str, session_id: str) -> None:
        for notifier in self.notifiers:
            notifier.notify(subject, actions, event_id, session_id)

