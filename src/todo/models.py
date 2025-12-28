from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional


@dataclass
class Task:
    uid: str
    summary: str
    status: str = "IN-PROCESS"
    due: Optional[datetime] = None
    priority: Optional[int] = None
    x_properties: Dict[str, str] = field(default_factory=dict)
    categories: list[str] = field(default_factory=list)


@dataclass
class TaskPayload:
    summary: str
    status: str = "IN-PROCESS"
    due: Optional[datetime] = None
    priority: Optional[int] = None
    x_properties: Dict[str, str] = field(default_factory=dict)
    categories: list[str] | None = None


@dataclass
class TaskPatch:
    summary: Optional[str] = None
    status: Optional[str] = None
    due: Optional[datetime] = None
    priority: Optional[int] = None
    x_properties: Dict[str, str] = field(default_factory=dict)
    categories: list[str] | None = None

    def has_changes(self) -> bool:
        return bool(
            self.summary
            or self.status
            or self.priority is not None
            or self.due is not None
            or self.x_properties
            or self.categories
        )
