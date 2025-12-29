from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet


__all__ = ["UpdateDescriptor"]


@dataclass(frozen=True)
class UpdateDescriptor:
    index: int | None
    add_tags: FrozenSet[str]
    remove_tags: FrozenSet[str]
    project: str | None
    due: str | None
    wait: str | None
