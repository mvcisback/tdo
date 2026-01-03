from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generic, Optional, TypeVar


T = TypeVar("T")


@dataclass
class Attachment:
    """Represents a CalDAV ATTACH property."""

    uri: str
    fmttype: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"uri": self.uri, "fmttype": self.fmttype}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Attachment:
        return cls(uri=data["uri"], fmttype=data.get("fmttype"))


@dataclass
class TaskData(Generic[T]):
    summary: str | None = None
    status: str | None = None
    due: T | None = None
    wait: T | None = None
    priority: Optional[int] = None
    x_properties: Dict[str, str] = field(default_factory=dict)
    categories: list[str] | None = None
    url: str | None = None
    attachments: list[Attachment] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize TaskData to a JSON-compatible dict."""

        def serialize_time(v: T | None) -> str | None:
            if v is None:
                return None
            if isinstance(v, datetime):
                return v.isoformat()
            return str(v)

        return {
            "summary": self.summary,
            "status": self.status,
            "due": serialize_time(self.due),
            "wait": serialize_time(self.wait),
            "priority": self.priority,
            "x_properties": dict(self.x_properties) if self.x_properties else {},
            "categories": list(self.categories) if self.categories else None,
            "url": self.url,
            "attachments": [a.to_dict() for a in self.attachments],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskData[datetime]:
        """Deserialize TaskData from a dict."""
        due = data.get("due")
        wait = data.get("wait")
        attachments_raw = data.get("attachments") or []
        return cls(
            summary=data.get("summary"),
            status=data.get("status"),
            due=datetime.fromisoformat(due) if due else None,
            wait=datetime.fromisoformat(wait) if wait else None,
            priority=data.get("priority"),
            x_properties=data.get("x_properties") or {},
            categories=data.get("categories"),
            url=data.get("url"),
            attachments=[Attachment.from_dict(a) for a in attachments_raw],
        )


@dataclass
class Task:
    uid: str
    data: TaskData[datetime]
    href: str | None = None
    task_index: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize Task to a JSON-compatible dict."""
        return {
            "uid": self.uid,
            "data": self.data.to_dict(),
            "href": self.href,
            "task_index": self.task_index,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Task:
        """Deserialize Task from a dict."""
        return cls(
            uid=data["uid"],
            data=TaskData.from_dict(data["data"]),
            href=data.get("href"),
            task_index=data.get("task_index"),
        )


TaskPayload = TaskData[datetime]
TaskPatch = TaskData[datetime]


@dataclass
class TaskFilter:
    project: str | None = None
    tags: list[str] = field(default_factory=list)
    indices: list[int] = field(default_factory=list)
