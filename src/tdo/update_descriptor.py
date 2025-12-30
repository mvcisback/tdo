from __future__ import annotations

from dataclasses import dataclass, field
from typing import FrozenSet


__all__ = ["UpdateDescriptor"]


@dataclass(frozen=True)
class UpdateDescriptor:
    description: str
    add_tags: FrozenSet[str]
    remove_tags: FrozenSet[str]
    project: str | None
    due: str | None
    wait: str | None
    priority: int | None = None
    status: str | None = None
    summary: str | None = None
    x_properties: dict[str, str] = field(default_factory=dict)
