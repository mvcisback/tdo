from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Dict, TYPE_CHECKING
from uuid import uuid4

from .config import CaldavConfig
from .diff import TaskDiff, TaskSetDiff
from .models import Task, TaskData, TaskPatch, TaskPayload
from .sqlite_cache import DirtyTask, SqliteTaskCache

if TYPE_CHECKING:
    from caldav import DAVClient, Calendar
    from caldav.objects import CalendarObjectResource


# Sentinel value to indicate a datetime field should be explicitly unset
_UNSET_DATETIME = datetime(1, 1, 1, 0, 0, 0)


def _debug_log(stage: str, duration: float, info: str | None = None) -> None:
    suffix = f" {info}" if info else ""
    print(f"[timing] {stage}: {duration:.3f}s{suffix}")


@dataclass
class PullResult:
    tasks: list[Task]
    diff: TaskSetDiff[int]

    @property
    def fetched(self) -> int:
        return len(self.tasks)


@dataclass
class PushResult:
    diff: TaskSetDiff[int]

    @property
    def created(self) -> int:
        return self.diff.created_count

    @property
    def updated(self) -> int:
        return self.diff.updated_count

    @property
    def deleted(self) -> int:
        return self.diff.deleted_count


@dataclass
class SyncResult:
    pulled: PullResult
    pushed: PushResult


@dataclass
class CalDAVClient:
    config: CaldavConfig
    cache_path: Path | None = field(default=None)
    client: "DAVClient" | None = field(default=None, init=False)
    calendar: "Calendar" | None = field(default=None, init=False)
    cache: SqliteTaskCache | None = field(default=None, init=False)

    @classmethod
    async def create(cls, config: CaldavConfig, cache_path: Path | None = None) -> CalDAVClient:
        instance = cls(config=config, cache_path=cache_path)
        await instance._init_cache()
        return instance

    async def _init_cache(self) -> None:
        env = self.config.env or "default"
        self.cache = await SqliteTaskCache.create(self.cache_path, env=env)

    def __enter__(self) -> CalDAVClient:
        from caldav import DAVClient

        self.client = DAVClient(
            url=self.config.calendar_url,
            username=self.config.username,
            password=self.config.getpass(),
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

    async def close(self) -> None:
        if self.cache:
            await self.cache.close()
            self.cache = None

    def _ensure_cache(self) -> SqliteTaskCache:
        if self.cache is None:
            raise RuntimeError("cache is not initialized; use CalDAVClient.create()")
        return self.cache

    async def list_tasks(self, force_refresh: bool = False) -> list[Task]:
        return await self._ensure_cache().list_tasks()

    async def list_tasks_filtered(self, task_filter: "TaskFilter | None" = None) -> list[Task]:
        from .models import TaskFilter
        return await self._ensure_cache().list_tasks_filtered(task_filter)

    async def create_task(self, payload: TaskPayload) -> Task:
        uid = self._uid_from_summary(payload.summary)
        categories = list(payload.categories) if payload.categories else []
        task = Task(
            uid=uid,
            data=TaskData(
                summary=payload.summary,
                status=payload.status or "IN-PROCESS",
                due=payload.due,
                wait=payload.wait,
                priority=payload.priority,
                x_properties=dict(payload.x_properties),
                categories=categories,
            ),
        )
        cache = self._ensure_cache()
        await cache.upsert_task(task, pending_action="create")
        # Assign a stable index to the new task
        task_index = await cache.assign_index(uid)
        task.task_index = task_index
        return task

    async def modify_task(self, task: Task, patch: TaskPatch) -> Task:
        updated = self._apply_patch(task, patch)
        pending_action = await self._ensure_cache().get_pending_action(task.uid)
        action = "create" if pending_action == "create" else "update"
        await self._ensure_cache().upsert_task(updated, pending_action=action)
        return updated

    async def delete_task(self, uid: str) -> str:
        cache = self._ensure_cache()
        task = await cache.get_task(uid)
        if task is None:
            raise KeyError(f"task {uid} not found")
        pending_action = await cache.get_pending_action(uid)
        if pending_action == "create":
            await cache.delete_task(uid)
            return uid
        await cache.upsert_task(task, pending_action="delete", deleted=True)
        return uid

    def _apply_patch(self, task: Task, patch: TaskPatch) -> Task:
        summary = patch.summary or task.data.summary or task.uid
        # Handle sentinel values for "unset"
        if patch.due == _UNSET_DATETIME:
            due = None
        elif patch.due is not None:
            due = patch.due
        else:
            due = task.data.due
        if patch.wait == _UNSET_DATETIME:
            wait = None
        elif patch.wait is not None:
            wait = patch.wait
        else:
            wait = task.data.wait
        if patch.priority == 0:
            priority = None  # 0 means unset priority
        elif patch.priority is not None:
            priority = patch.priority
        else:
            priority = task.data.priority
        status = patch.status or task.data.status
        x_properties = dict(task.data.x_properties)
        x_properties.update(patch.x_properties)
        # Remove properties with empty values (signals deletion)
        x_properties = {k: v for k, v in x_properties.items() if v}
        categories = patch.categories if patch.categories is not None else task.data.categories
        categories = list(categories or [])
        return Task(
            uid=task.uid,
            data=TaskData(
                summary=summary,
                status=status,
                due=due,
                wait=wait,
                priority=priority,
                x_properties=x_properties,
                categories=categories,
            ),
            href=task.href,
            task_index=task.task_index,
        )

    async def pull(self) -> PullResult:
        start = perf_counter()
        cache = self._ensure_cache()

        # Get cached state before pull
        before = await cache.list_tasks()
        before_by_uid = {t.uid: t for t in before}

        # Fetch remote tasks
        calendar = self._ensure_calendar()
        resources = calendar.todos()
        remote_tasks = [self._task_from_resource(todo) for todo in resources]

        # Replace cache with remote tasks
        await cache.replace_remote_tasks(remote_tasks)

        # Get cached state after pull (with assigned indices)
        after = await cache.list_tasks()
        after_by_uid = {t.uid: t for t in after}

        # Build diff keyed by task_index
        diffs: dict[int, TaskDiff] = {}
        all_uids = set(before_by_uid.keys()) | set(after_by_uid.keys())

        for uid in all_uids:
            before_task = before_by_uid.get(uid)
            after_task = after_by_uid.get(uid)

            pre = before_task.data if before_task else None
            post = after_task.data if after_task else None

            if pre == post:
                continue

            # Use after's index for creates/updates, before's index for deletes
            if after_task is not None:
                key = after_task.task_index
            else:
                key = before_task.task_index

            diffs[key] = TaskDiff(pre=pre, post=post)

        diff: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)

        elapsed = perf_counter() - start
        _debug_log("pull", elapsed, f"count={len(remote_tasks)}")
        return PullResult(tasks=after, diff=diff)

    async def push(self) -> PushResult:
        start = perf_counter()
        cache = self._ensure_cache()
        pending = await cache.dirty_tasks()
        diffs: dict[int, TaskDiff] = {}

        if pending:
            calendar = self._ensure_calendar()
            for entry in pending:
                task = entry.task
                index = task.task_index

                if entry.action == "create":
                    synced = self._push_create(task, calendar)
                    await cache.upsert_task(
                        synced,
                        last_synced=time.time(),
                        clear_pending=True,
                    )
                    diffs[index] = TaskDiff(pre=None, post=task.data)
                elif entry.action == "update":
                    synced = self._push_update(task, calendar)
                    await cache.upsert_task(
                        synced,
                        last_synced=time.time(),
                        clear_pending=True,
                    )
                    # Use empty TaskData as synthetic pre to trigger is_update
                    diffs[index] = TaskDiff(pre=TaskData(), post=task.data)
                else:  # delete
                    self._push_delete(task, calendar)
                    await cache.delete_task(task.uid)
                    diffs[index] = TaskDiff(pre=task.data, post=None)

        diff: TaskSetDiff[int] = TaskSetDiff(diffs=diffs)
        elapsed = perf_counter() - start
        _debug_log("push", elapsed, f"pending={len(pending)}")
        return PushResult(diff=diff)

    async def sync(self) -> SyncResult:
        pulled = await self.pull()
        pushed = await self.push()
        return SyncResult(pulled=pulled, pushed=pushed)

    def _ensure_calendar(self) -> "Calendar":
        if self.calendar is None:
            raise RuntimeError("caldav client is not initialized")
        return self.calendar


    def _push_create(self, task: Task, calendar: "Calendar") -> Task:
        body = self._build_ics(
            task.data.summary,
            task.data.due,
            task.data.wait,
            task.data.priority,
            task.data.x_properties,
            task.data.categories,
            task.uid,
            task.data.status,
        )
        todo = calendar.add_todo(body)
        return self._task_from_resource(todo)

    def _push_update(self, task: Task, calendar: "Calendar") -> Task:
        summary = task.data.summary or task.uid
        body = self._build_ics(summary, task.data.due, task.data.wait, task.data.priority, task.data.x_properties, task.data.categories, task.uid, task.data.status)
        resource = self._resource_for_update(task, calendar)
        resource.id = task.uid
        resource.data = body
        resource.save()
        return self._task_from_resource(resource)

    def _push_delete(self, task: Task, calendar: "Calendar") -> None:
        todo = calendar.todo_by_uid(task.uid)
        if todo:
            todo.delete()

    def _resource_for_update(self, task: Task, calendar: "Calendar") -> "CalendarObjectResource":
        from caldav.objects import CalendarObjectResource

        if task.href:
            return CalendarObjectResource(client=self.client, url=task.href, parent=calendar)
        resource = calendar.todo_by_uid(task.uid)
        if resource is None:
            raise KeyError(f"task {task.uid} missing href")
        return resource

    def _task_from_resource(self, resource: "CalendarObjectResource") -> Task:
        task = self._task_from_data(resource.data or "")
        if resource.url:
            task.href = str(resource.url)
        return task

    def _build_ics(
        self,
        summary: str,
        due: datetime | None,
        wait: datetime | None,
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
        if wait is not None:
            lines.append(f"DTSTART:{self._format_due(wait)}")
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
        wait = None
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
            elif key == "DTSTART":
                wait = self._parse_due(value)
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
            data=TaskData(
                summary=summary,
                status=status or "IN-PROCESS",
                due=due,
                wait=wait,
                priority=priority,
                x_properties=x_properties,
                categories=categories,
            ),
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
