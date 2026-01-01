from __future__ import annotations

from dataclasses import dataclass, field

from .models import TaskData


__all__ = ["UpdateDescriptor"]


@dataclass(frozen=True)
class UpdateDescriptor:
    add_data: TaskData[str] = field(default_factory=lambda: TaskData[str]())
    remove_data: TaskData[str] = field(default_factory=lambda: TaskData[str]())
