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


@dataclass
class TaskPayload:
    summary: str
    status: str = "IN-PROCESS"
    due: Optional[datetime] = None
    priority: Optional[int] = None
    x_properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class TaskPatch:
    summary: Optional[str] = None
    status: Optional[str] = None
    due: Optional[datetime] = None
    priority: Optional[int] = None
    x_properties: Dict[str, str] = field(default_factory=dict)

    def has_changes(self) -> bool:
        return bool(
            self.summary
            or self.status
            or self.priority is not None
            or self.due is not None
            or self.x_properties
        )
