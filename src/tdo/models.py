from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Generic, Optional, TypeVar


T = TypeVar("T")


@dataclass
class TaskData(Generic[T]):
    summary: str | None = None
    status: str | None = None
    due: T | None = None
    wait: T | None = None
    priority: Optional[int] = None
    x_properties: Dict[str, str] = field(default_factory=dict)
    categories: list[str] | None = None


@dataclass
class Task:
    uid: str
    data: TaskData[datetime]
    href: str | None = None
    task_index: int | None = None


TaskPayload = TaskData[datetime]
TaskPatch = TaskData[datetime]


@dataclass
class TaskFilter:
    project: str | None = None
    tags: list[str] = field(default_factory=list)
    indices: list[int] = field(default_factory=list)
