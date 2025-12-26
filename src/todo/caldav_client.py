from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict
from uuid import uuid4

from caldav import DAVClient, Calendar

from .config import CaldavConfig
from .models import Task, TaskPatch, TaskPayload


@dataclass
class CalDAVClient:
    config: CaldavConfig
    client: DAVClient | None = field(default=None, init=False)
    calendar: Calendar | None = field(default=None, init=False)

    def __enter__(self) -> CalDAVClient:
        self.client = DAVClient(
            url=self.config.calendar_url,
            username=self.config.username,
            password=self.config.password,
        )
        if self.config.token and self.client.session:
            self.client.session.headers["Authorization"] = f"Bearer {self.config.token}"
        calendar = None
        try:
            calendar = self.client.calendar(url=self.config.calendar_url)
        except Exception:
            pass
        if not calendar:
            principal = self.client.principal()
            calendars = principal.calendars()
            if not calendars:
                raise RuntimeError("no calendars found for the configured user")
            calendar = calendars[0]
        self.calendar = calendar
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.client and self.client.session:
            self.client.session.close()
        self.client = None
        self.calendar = None

    def list_tasks(self) -> list[Task]:
        calendar = self._ensure_calendar()
        return [self._task_from_data(todo.data) for todo in calendar.todos()]

    def create_task(self, payload: TaskPayload) -> Task:
        calendar = self._ensure_calendar()
        uid = self._uid_from_summary(payload.summary)
        body = self._build_ics(
            payload.summary,
            payload.due,
            payload.priority,
            payload.x_properties,
            uid,
            payload.status,
        )
        todo = calendar.add_todo(body)
        return self._task_from_data(todo.data)

    def modify_task(self, uid: str, patch: TaskPatch) -> Task:
        calendar = self._ensure_calendar()
        todo = calendar.todo_by_uid(uid)
        if todo is None:
            raise KeyError(f"todo {uid} not found")
        existing = self._task_from_data(todo.data)
        summary = patch.summary or existing.summary or uid
        due = patch.due if patch.due is not None else existing.due
        priority = patch.priority if patch.priority is not None else existing.priority
        status = patch.status or existing.status
        x_properties = dict(existing.x_properties)
        x_properties.update(patch.x_properties)
        body = self._build_ics(summary, due, priority, x_properties, uid, status)
        todo.data = body
        todo.save()
        return Task(uid=uid, summary=summary, status=status, due=due, priority=priority, x_properties=x_properties)

    def delete_task(self, uid: str) -> str:
        calendar = self._ensure_calendar()
        todo = calendar.todo_by_uid(uid)
        if todo is None:
            raise KeyError(f"todo {uid} not found")
        todo.delete()
        return uid

    def _ensure_calendar(self) -> Calendar:
        if self.calendar is None:
            raise RuntimeError("caldav client is not initialized")
        return self.calendar

    def _build_ics(
        self,
        summary: str,
        due: datetime | None,
        priority: int | None,
        x_properties: Dict[str, str],
        uid: str,
        status: str | None,
    ) -> str:
        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//todo-cli//EN",
            "BEGIN:VTODO",
            f"UID:{uid}",
            f"SUMMARY:{summary}",
        ]
        if status:
            lines.append(f"STATUS:{status}")
        if priority is not None:
            lines.append(f"PRIORITY:{priority}")
        if due is not None:
            lines.append(f"DUE:{self._format_due(due)}")
        for name, value in x_properties.items():
            lines.append(f"{name}:{value}")
        lines.extend(["END:VTODO", "END:VCALENDAR"])
        return "\r\n".join(lines) + "\r\n"

    def _format_due(self, value: datetime) -> str:
        return value.strftime("%Y%m%dT%H%M%SZ")

    def _task_from_data(self, data: str) -> Task:
        summary = ""
        due = None
        priority = None
        status: str | None = None
        x_properties: Dict[str, str] = {}
        uid = ""
        for raw in data.splitlines():
            line = raw.strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            if key == "SUMMARY":
                summary = value
            elif key == "UID":
                uid = value
            elif key == "DUE":
                due = self._parse_due(value)
            elif key == "PRIORITY":
                try:
                    priority = int(value)
                except ValueError:
                    pass
            elif key == "STATUS":
                status = value
            elif key.startswith("X-"):
                x_properties[key] = value
        return Task(
            uid=uid,
            summary=summary,
            status=status or "IN-PROCESS",
            due=due,
            priority=priority,
            x_properties=x_properties,
        )

    def _parse_due(self, raw: str) -> datetime | None:
        try:
            if raw.endswith("Z"):
                return datetime.strptime(raw, "%Y%m%dT%H%M%SZ")
            return datetime.strptime(raw, "%Y%m%dT%H%M%S")
        except ValueError:
            return None

    def _extract_summary(self, data: str) -> str | None:
        for raw in data.splitlines():
            if raw.strip().startswith("SUMMARY:"):
                return raw.split(":", 1)[1]
        return None

    def _uid_from_summary(self, summary: str) -> str:
        return f"{summary.replace(' ', '_')}-{uuid4()}"
