from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import time
from pathlib import Path
from time import perf_counter
from typing import Dict
from uuid import uuid4

from caldav import DAVClient, Calendar
from caldav.objects import CalendarObjectResource

from .config import CaldavConfig
from .models import Task, TaskPatch, TaskPayload
from .sqlite_cache import DirtyTask, SqliteTaskCache


def _debug_log(stage: str, duration: float, info: str | None = None) -> None:
    suffix = f" {info}" if info else ""
    print(f"[timing] {stage}: {duration:.3f}s{suffix}")


@dataclass
class PullResult:
    fetched: int


@dataclass
class PushResult:
    created: int = 0
    updated: int = 0
    deleted: int = 0


@dataclass
class SyncResult:
    pulled: PullResult
    pushed: PushResult


@dataclass
class CalDAVClient:
    config: CaldavConfig
    cache_path: Path | None = field(default=None)
    client: DAVClient | None = field(default=None, init=False)
    calendar: Calendar | None = field(default=None, init=False)
    cache: SqliteTaskCache = field(init=False)

    def __post_init__(self) -> None:
        self.cache = SqliteTaskCache(self.cache_path)

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
        return self.cache.list_tasks()

    def create_task(self, payload: TaskPayload) -> Task:
        uid = self._uid_from_summary(payload.summary)
        categories = list(payload.categories) if payload.categories else []
        task = Task(
            uid=uid,
            summary=payload.summary,
            status=payload.status or "IN-PROCESS",
            due=payload.due,
            priority=payload.priority,
            x_properties=dict(payload.x_properties),
            categories=categories,
        )
        self.cache.upsert_task(task, pending_action="create")
        return task

    def modify_task(self, task: Task, patch: TaskPatch) -> Task:
        updated = self._apply_patch(task, patch)
        pending_action = self.cache.get_pending_action(task.uid)
        action = "create" if pending_action == "create" else "update"
        self.cache.upsert_task(updated, pending_action=action)
        return updated

    def delete_task(self, uid: str) -> str:
        task = self.cache.get_task(uid)
        if task is None:
            raise KeyError(f"task {uid} not found")
        pending_action = self.cache.get_pending_action(uid)
        if pending_action == "create":
            self.cache.delete_task(uid)
            return uid
        self.cache.upsert_task(task, pending_action="delete", deleted=True)
        return uid

    def _apply_patch(self, task: Task, patch: TaskPatch) -> Task:
        summary = patch.summary or task.summary or task.uid
        due = patch.due if patch.due is not None else task.due
        priority = patch.priority if patch.priority is not None else task.priority
        status = patch.status or task.status
        x_properties = dict(task.x_properties)
        x_properties.update(patch.x_properties)
        categories = patch.categories if patch.categories is not None else task.categories
        categories = list(categories)
        return Task(
            uid=task.uid,
            summary=summary,
            status=status,
            due=due,
            priority=priority,
            x_properties=x_properties,
            categories=categories,
            href=task.href,
        )

    def pull(self) -> PullResult:
        start = perf_counter()
        calendar = self._ensure_calendar()
        resources = calendar.todos()
        tasks = [self._task_from_resource(todo) for todo in resources]
        self.cache.replace_remote_tasks(tasks)
        elapsed = perf_counter() - start
        _debug_log("pull", elapsed, f"count={len(tasks)}")
        return PullResult(fetched=len(tasks))

    def push(self) -> PushResult:
        start = perf_counter()
        pending = self.cache.dirty_tasks()
        created = 0
        updated = 0
        deleted = 0
        if pending:
            calendar = self._ensure_calendar()
            for entry in pending:
                if entry.action == "create":
                    created += 1
                    synced = self._push_create(entry.task, calendar)
                    self.cache.upsert_task(
                        synced,
                        last_synced=time.time(),
                        clear_pending=True,
                    )
                    continue
                if entry.action == "update":
                    updated += 1
                    synced = self._push_update(entry.task, calendar)
                    self.cache.upsert_task(
                        synced,
                        last_synced=time.time(),
                        clear_pending=True,
                    )
                    continue
                deleted += 1
                self._push_delete(entry.task, calendar)
                self.cache.delete_task(entry.task.uid)
        elapsed = perf_counter() - start
        _debug_log("push", elapsed, f"pending={len(pending)}")
        return PushResult(created=created, updated=updated, deleted=deleted)

    def sync(self) -> SyncResult:
        pulled = self.pull()
        pushed = self.push()
        return SyncResult(pulled=pulled, pushed=pushed)

    def _ensure_calendar(self) -> Calendar:
        if self.calendar is None:
            raise RuntimeError("caldav client is not initialized")
        return self.calendar


    def _push_create(self, task: Task, calendar: Calendar) -> Task:
        body = self._build_ics(
            task.summary,
            task.due,
            task.priority,
            task.x_properties,
            task.categories,
            task.uid,
            task.status,
        )
        todo = calendar.add_todo(body)
        return self._task_from_resource(todo)

    def _push_update(self, task: Task, calendar: Calendar) -> Task:
        summary = task.summary or task.uid
        body = self._build_ics(summary, task.due, task.priority, task.x_properties, task.categories, task.uid, task.status)
        resource = self._resource_for_update(task, calendar)
        resource.id = task.uid
        resource.data = body
        resource.save()
        return self._task_from_resource(resource)

    def _push_delete(self, task: Task, calendar: Calendar) -> None:
        todo = calendar.todo_by_uid(task.uid)
        if todo:
            todo.delete()

    def _resource_for_update(self, task: Task, calendar: Calendar) -> CalendarObjectResource:
        if task.href:
            return CalendarObjectResource(client=self.client, url=task.href, parent=calendar)
        resource = calendar.todo_by_uid(task.uid)
        if resource is None:
            raise KeyError(f"task {task.uid} missing href")
        return resource

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

    def _uid_from_summary(self, summary: str) -> str:
        return f"{summary.replace(' ', '_')}-{uuid4()}"
