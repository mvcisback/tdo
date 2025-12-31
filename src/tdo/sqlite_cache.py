from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

import aiosqlite

from .models import Task, TaskFilter


@dataclass
class DirtyTask:
    task: Task
    action: str
    deleted: bool


def _serialize_properties(value: Sequence[str] | None) -> str:
    return json.dumps(list(value or []))


def _serialize_map(value: dict[str, str] | None) -> str:
    return json.dumps(value or {})


def _parse_json(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return {str(key): str(value) for key, value in payload.items()}
    return {}


def _parse_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if isinstance(payload, list):
        return [str(item) for item in payload if item is not None]
    return []


class SqliteTaskCache:
    def __init__(self, path: Path | None = None, *, env: str = "default"):
        resolved = self._resolve_path(path, env)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        self.path = resolved
        self._conn: aiosqlite.Connection | None = None

    @classmethod
    async def create(cls, path: Path | None = None, *, env: str = "default") -> SqliteTaskCache:
        instance = cls(path, env=env)
        await instance._connect()
        return instance

    async def _connect(self) -> None:
        self._conn = await aiosqlite.connect(str(self.path))
        self._conn.row_factory = aiosqlite.Row
        await self._ensure_schema()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @staticmethod
    def _resolve_path(path: Path | None, env: str) -> Path:
        if path:
            return path.expanduser()
        override = os.environ.get("TDO_TASK_CACHE_FILE")
        if override:
            return Path(override).expanduser()
        try:
            base = Path.home()
        except OSError:
            base = Path.cwd()
        safe_env = SqliteTaskCache._normalize_env(env)
        return base / ".cache" / "tdo" / safe_env / "tasks.db"

    @staticmethod
    def _normalize_env(env: str | None) -> str:
        candidate = (env or "").strip()
        if not candidate:
            return "default"
        normalized = Path(candidate).name
        if not normalized:
            return "default"
        return normalized

    async def _ensure_schema(self) -> None:
        script = """
        CREATE TABLE IF NOT EXISTS tasks (
            uid TEXT PRIMARY KEY,
            summary TEXT NOT NULL,
            status TEXT NOT NULL,
            due TEXT,
            wait TEXT,
            priority INTEGER,
            x_properties TEXT,
            categories TEXT,
            href TEXT,
            pending_action TEXT,
            deleted INTEGER NOT NULL DEFAULT 0,
            last_synced REAL,
            updated_at REAL NOT NULL,
            task_index INTEGER UNIQUE
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due);
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_dirty ON tasks(pending_action);
        CREATE INDEX IF NOT EXISTS idx_tasks_index ON tasks(task_index);
        """
        assert self._conn is not None
        await self._conn.executescript(script)
        await self._conn.commit()
        await self._migrate_schema()

    async def _migrate_schema(self) -> None:
        assert self._conn is not None
        cursor = await self._conn.execute("PRAGMA table_info(tasks)")
        columns = {row[1] for row in await cursor.fetchall()}
        if "task_index" not in columns:
            await self._conn.execute(
                "ALTER TABLE tasks ADD COLUMN task_index INTEGER UNIQUE"
            )
            await self._conn.commit()
            await self._assign_indices_to_existing_tasks()
        if "wait" not in columns:
            await self._conn.execute("ALTER TABLE tasks ADD COLUMN wait TEXT")
            await self._conn.commit()

    async def _assign_indices_to_existing_tasks(self) -> None:
        assert self._conn is not None
        cursor = await self._conn.execute(
            "SELECT uid FROM tasks WHERE deleted = 0 ORDER BY due IS NULL, due, summary"
        )
        rows = await cursor.fetchall()
        for idx, row in enumerate(rows, start=1):
            await self._conn.execute(
                "UPDATE tasks SET task_index = ? WHERE uid = ?",
                (idx, row[0])
            )
        await self._conn.commit()

    async def _next_available_index(self) -> int:
        """Find smallest hole or increment max."""
        assert self._conn is not None
        cursor = await self._conn.execute(
            "SELECT task_index FROM tasks WHERE task_index IS NOT NULL ORDER BY task_index"
        )
        rows = await cursor.fetchall()
        indices = [row[0] for row in rows]

        if not indices:
            return 1

        # Find first hole
        expected = 1
        for idx in indices:
            if idx > expected:
                return expected
            expected = idx + 1

        # No holes, return max + 1
        return indices[-1] + 1

    async def assign_index(self, uid: str) -> int:
        """Assign next available index to a task."""
        index = await self._next_available_index()
        assert self._conn is not None
        await self._conn.execute(
            "UPDATE tasks SET task_index = ? WHERE uid = ?",
            (index, uid)
        )
        await self._conn.commit()
        return index

    async def get_task_by_index(self, index: int) -> Task | None:
        """Get task by its stable index."""
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM tasks WHERE task_index = ? AND deleted = 0",
            (index,)
        ) as cursor:
            row = await cursor.fetchone()
        return self._build_task(row) if row else None

    async def list_tasks(self) -> list[Task]:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM tasks WHERE deleted = 0 ORDER BY due IS NULL, due"
        ) as cursor:
            rows = await cursor.fetchall()
        return [self._build_task(row) for row in rows]

    async def list_tasks_filtered(self, task_filter: TaskFilter | None = None) -> list[Task]:
        assert self._conn is not None
        conditions = ["deleted = 0"]
        params: list[str] = []

        if task_filter:
            if task_filter.project:
                conditions.append("json_extract(x_properties, '$.X-PROJECT') = ?")
                params.append(task_filter.project)
            for tag in task_filter.tags:
                conditions.append("categories LIKE ?")
                params.append(f'%"{tag}"%')
            if task_filter.indices:
                placeholders = ",".join("?" for _ in task_filter.indices)
                conditions.append(f"task_index IN ({placeholders})")
                params.extend(str(i) for i in task_filter.indices)

        where = " AND ".join(conditions)
        query = f"SELECT * FROM tasks WHERE {where} ORDER BY due IS NULL, due"

        async with self._conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return [self._build_task(row) for row in rows]

    async def dirty_tasks(self) -> list[DirtyTask]:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM tasks WHERE pending_action IS NOT NULL ORDER BY updated_at"
        ) as cursor:
            rows = await cursor.fetchall()
        return [DirtyTask(task=self._build_task(row), action=row["pending_action"], deleted=bool(row["deleted"])) for row in rows]

    async def replace_remote_tasks(self, tasks: Sequence[Task]) -> None:
        timestamp = time.time()
        assert self._conn is not None

        # Preserve existing indices for tasks we're updating
        cursor = await self._conn.execute(
            "SELECT uid, task_index FROM tasks WHERE task_index IS NOT NULL"
        )
        existing_indices = {row[0]: row[1] for row in await cursor.fetchall()}

        await self._conn.execute("DELETE FROM tasks WHERE pending_action IS NULL")
        await self._conn.commit()

        # Track which tasks need new indices
        tasks_needing_indices: list[str] = []

        for task in tasks:
            # Preserve existing index if task was already in cache
            preserved_index = existing_indices.get(task.uid)
            await self._insert_or_update(
                task,
                pending_action=None,
                deleted=False,
                last_synced=timestamp,
                clear_pending=True,
                task_index=preserved_index,
            )
            if preserved_index is None:
                tasks_needing_indices.append(task.uid)

        # Assign indices to new tasks
        for uid in tasks_needing_indices:
            await self.assign_index(uid)

    async def upsert_task(
        self,
        task: Task,
        *,
        pending_action: str | None = None,
        deleted: bool = False,
        last_synced: float | None = None,
        clear_pending: bool = False,
        task_index: int | None = None,
    ) -> None:
        await self._insert_or_update(
            task,
            pending_action=pending_action,
            deleted=deleted,
            last_synced=last_synced,
            clear_pending=clear_pending,
            task_index=task_index,
        )

    async def delete_task(self, uid: str) -> None:
        assert self._conn is not None
        await self._conn.execute("DELETE FROM tasks WHERE uid = ?", (uid,))
        await self._conn.commit()

    async def get_task(self, uid: str) -> Task | None:
        assert self._conn is not None
        async with self._conn.execute("SELECT * FROM tasks WHERE uid = ?", (uid,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        return self._build_task(row)

    async def get_pending_action(self, uid: str) -> str | None:
        assert self._conn is not None
        async with self._conn.execute("SELECT pending_action FROM tasks WHERE uid = ?", (uid,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        return row["pending_action"]

    async def _insert_or_update(
        self,
        task: Task,
        *,
        pending_action: str | None,
        deleted: bool,
        last_synced: float | None,
        clear_pending: bool,
        task_index: int | None = None,
    ) -> None:
        summary = task.summary or task.uid
        status = task.status or "IN-PROCESS"
        due_value = task.due.isoformat() if task.due else None
        wait_value = task.wait.isoformat() if task.wait else None
        priority = task.priority
        x_props = _serialize_map(task.x_properties)
        categories = _serialize_properties(task.categories)
        href = task.href
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT pending_action, last_synced, task_index FROM tasks WHERE uid = ?",
            (task.uid,)
        ) as cursor:
            existing = await cursor.fetchone()
        if clear_pending:
            resolved_pending = None
        elif pending_action is not None:
            resolved_pending = pending_action
        else:
            resolved_pending = existing["pending_action"] if existing else None
        resolved_last_synced = last_synced if last_synced is not None else (existing["last_synced"] if existing else None)
        # Preserve existing index if not explicitly provided
        resolved_index = task_index if task_index is not None else (existing["task_index"] if existing else None)
        now = time.time()
        await self._conn.execute(
            """
            INSERT INTO tasks (
                uid,
                summary,
                status,
                due,
                wait,
                priority,
                x_properties,
                categories,
                href,
                pending_action,
                deleted,
                last_synced,
                updated_at,
                task_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                summary = excluded.summary,
                status = excluded.status,
                due = excluded.due,
                wait = excluded.wait,
                priority = excluded.priority,
                x_properties = excluded.x_properties,
                categories = excluded.categories,
                href = excluded.href,
                pending_action = ?,
                deleted = ?,
                last_synced = ?,
                updated_at = excluded.updated_at,
                task_index = COALESCE(excluded.task_index, task_index)
            """,
            (
                task.uid,
                summary,
                status,
                due_value,
                wait_value,
                priority,
                x_props,
                categories,
                href,
                resolved_pending,
                1 if deleted else 0,
                resolved_last_synced,
                now,
                resolved_index,
                resolved_pending,
                1 if deleted else 0,
                resolved_last_synced,
            ),
        )
        await self._conn.commit()

    def _build_task(self, row: aiosqlite.Row) -> Task:
        due = None
        due_value = row["due"]
        if due_value:
            try:
                due = datetime.fromisoformat(due_value)
            except ValueError:
                due = None
        wait = None
        wait_value = row["wait"]
        if wait_value:
            try:
                wait = datetime.fromisoformat(wait_value)
            except ValueError:
                wait = None
        return Task(
            uid=row["uid"],
            summary=row["summary"],
            status=row["status"],
            due=due,
            wait=wait,
            priority=row["priority"],
            x_properties=_parse_json(row["x_properties"]),
            categories=_parse_list(row["categories"]),
            href=row["href"],
            task_index=row["task_index"],
        )
