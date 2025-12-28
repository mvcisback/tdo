from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, Sequence
from uuid import uuid4

from caldav import DAVClient, Calendar
from caldav.objects import CalendarObjectResource

from .config import CaldavConfig
from .models import Task, TaskPatch, TaskPayload


@dataclass
class CalDAVClient:
    config: CaldavConfig
    cache_path: Path | None = field(default=None)
    client: DAVClient | None = field(default=None, init=False)
    calendar: Calendar | None = field(default=None, init=False)
    _cached_tasks: list[Task] | None = field(default=None, init=False)

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

    def list_tasks(self, force_refresh: bool = False) -> list[Task]:
        if not force_refresh and self._cached_tasks is not None:
            return list(self._cached_tasks)
        if not force_refresh:
            cached = self._read_cache()
            if cached is not None:
                self._cached_tasks = cached
                return list(cached)
        tasks = self._fetch_tasks()
        self._cached_tasks = tasks
        self._write_cache(tasks)
        return list(tasks)

    def create_task(self, payload: TaskPayload) -> Task:
        calendar = self._ensure_calendar()
        uid = self._uid_from_summary(payload.summary)
        body = self._build_ics(
            payload.summary,
            payload.due,
            payload.priority,
            payload.x_properties,
            payload.categories,
            uid,
            payload.status,
        )
        todo = calendar.add_todo(body)
        created = self._task_from_resource(todo)
        self._invalidate_cache()
        return created

    def modify_task(self, task: Task, patch: TaskPatch) -> Task:
        calendar = self._ensure_calendar()
        if not task.href:
            raise KeyError(f"task {task.uid} missing href")
        summary = patch.summary or task.summary or task.uid
        due = patch.due if patch.due is not None else task.due
        priority = patch.priority if patch.priority is not None else task.priority
        status = patch.status or task.status
        x_properties = dict(task.x_properties)
        x_properties.update(patch.x_properties)
        categories = patch.categories if patch.categories is not None else task.categories
        categories = list(categories)
        body = self._build_ics(summary, due, priority, x_properties, categories, task.uid, status)
        resource = CalendarObjectResource(
            client=self.client,
            url=task.href,
            parent=calendar,
        )
        resource.id = task.uid
        resource.data = body
        resource.save()
        updated = Task(
            uid=task.uid,
            summary=summary,
            status=status,
            due=due,
            priority=priority,
            x_properties=x_properties,
            categories=categories,
            href=task.href,
        )
        self._invalidate_cache()
        return updated

    def delete_task(self, uid: str) -> str:
        calendar = self._ensure_calendar()
        todo = calendar.todo_by_uid(uid)
        if todo is None:
            raise KeyError(f"todo {uid} not found")
        todo.delete()
        self._invalidate_cache()
        return uid

    def _ensure_calendar(self) -> Calendar:
        if self.calendar is None:
            raise RuntimeError("caldav client is not initialized")
        return self.calendar

    def _fetch_tasks(self) -> list[Task]:
        calendar = self._ensure_calendar()
        return [self._task_from_resource(todo) for todo in calendar.todos()]

    def _task_from_resource(self, resource: CalendarObjectResource) -> Task:
        task = self._task_from_data(resource.data or "")
        if resource.url:
            task.href = str(resource.url)
        return task

    def _build_ics(
        self,
        summary: str,
        due: datetime | None,
        priority: int | None,
        x_properties: Dict[str, str],
        categories: list[str] | None,
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
        if categories:
            lines.append(f"CATEGORIES:{','.join(categories)}")
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
        categories: list[str] = []
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
            elif key == "CATEGORIES":
                categories.extend(self._split_categories(value))
            elif key.startswith("X-"):
                x_properties[key] = value
        return Task(
            uid=uid,
            summary=summary,
            status=status or "IN-PROCESS",
            due=due,
            priority=priority,
            x_properties=x_properties,
            categories=categories,
        )

    def _parse_due(self, raw: str) -> datetime | None:
        try:
            if raw.endswith("Z"):
                return datetime.strptime(raw, "%Y%m%dT%H%M%SZ")
            return datetime.strptime(raw, "%Y%m%dT%H%M%S")
        except ValueError:
            return None

    def _split_categories(self, raw: str) -> list[str]:
        return [candidate.strip() for candidate in raw.split(",") if candidate.strip()]

    def _extract_summary(self, data: str) -> str | None:
        for raw in data.splitlines():
            if raw.strip().startswith("SUMMARY:"):
                return raw.split(":", 1)[1]
        return None

    def _uid_from_summary(self, summary: str) -> str:
        return f"{summary.replace(' ', '_')}-{uuid4()}"

    def _cache_file_path(self) -> Path | None:
        if self.cache_path:
            return self.cache_path
        override = os.environ.get("TODO_TASK_CACHE_FILE")
        if override:
            return Path(override)
        try:
            home = Path.home()
        except OSError:
            return None
        return home / ".cache" / "todo" / "tasks.json"

    def _read_cache(self) -> list[Task] | None:
        path = self._cache_file_path()
        if path is None or not path.exists():
            return None
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None
        entries = payload.get("tasks")
        if not isinstance(entries, list):
            return None
        tasks: list[Task] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            tasks.append(self._deserialize_task(entry))
        return tasks if tasks else None

    def _write_cache(self, tasks: Sequence[Task]) -> None:
        path = self._cache_file_path()
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"tasks": [self._serialize_task(task) for task in tasks]}
            path.write_text(json.dumps(payload, indent=2))
        except OSError:
            return

    def _serialize_task(self, task: Task) -> dict[str, Any]:
        return {
            "uid": task.uid,
            "summary": task.summary,
            "status": task.status,
            "due": task.due.isoformat() if task.due else None,
            "priority": task.priority,
            "x_properties": task.x_properties,
            "categories": task.categories,
            "href": task.href,
        }

    def _deserialize_task(self, entry: dict[str, Any]) -> Task:
        due_value = entry.get("due")
        due: datetime | None = None
        if isinstance(due_value, str):
            try:
                due = datetime.fromisoformat(due_value)
            except ValueError:
                pass
        return Task(
            uid=str(entry.get("uid") or ""),
            summary=str(entry.get("summary") or ""),
            status=str(entry.get("status") or "IN-PROCESS"),
            due=due,
            priority=entry.get("priority"),
            x_properties=dict(entry.get("x_properties") or {}),
            categories=list(entry.get("categories") or []),
            href=entry.get("href"),
        )

    def _invalidate_cache(self) -> None:
        self._cached_tasks = None
