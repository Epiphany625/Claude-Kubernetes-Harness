"""The object the server lifespan yields to every tool."""

from __future__ import annotations

from dataclasses import dataclass

from kubemcp.config import Settings
from kubemcp.k8s.client import KubeClient


@dataclass(slots=True)
class AppContext:
    """Shared state, built once at startup and reused by every tool call."""

    kube: KubeClient
    settings: Settings
