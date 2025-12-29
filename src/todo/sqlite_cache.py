from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence

from .models import Task


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
    def __init__(self, path: Path | None = None):
        resolved = self._resolve_path(path)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        self.path = resolved
        self._conn = sqlite3.connect(
            str(self.path),
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._ensure_schema()

    @staticmethod
    def _resolve_path(path: Path | None) -> Path:
        if path:
            return path.expanduser()
        override = os.environ.get("TODO_TASK_CACHE_FILE")
        if override:
            return Path(override).expanduser()
        try:
            base = Path.home()
        except OSError:
            base = Path.cwd()
        return base / ".cache" / "todo" / "tasks.db"

    def _ensure_schema(self) -> None:
        script = """
        CREATE TABLE IF NOT EXISTS tasks (
            uid TEXT PRIMARY KEY,
            summary TEXT NOT NULL,
            status TEXT NOT NULL,
            due TEXT,
            priority INTEGER,
            x_properties TEXT,
            categories TEXT,
            href TEXT,
            pending_action TEXT,
            deleted INTEGER NOT NULL DEFAULT 0,
            last_synced REAL,
            updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due);
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_dirty ON tasks(pending_action);
        """
        with self._lock:
            self._conn.executescript(script)
            self._conn.commit()

    def list_tasks(self) -> list[Task]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE deleted = 0 ORDER BY due IS NULL, due"
            ).fetchall()
        return [self._build_task(row) for row in rows]

    async def list_tasks_async(self) -> list[Task]:
        return await asyncio.to_thread(self.list_tasks)

    def dirty_tasks(self) -> list[DirtyTask]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE pending_action IS NOT NULL ORDER BY updated_at"
            ).fetchall()
        return [DirtyTask(task=self._build_task(row), action=row["pending_action"], deleted=bool(row["deleted"])) for row in rows]

    async def dirty_tasks_async(self) -> list[DirtyTask]:
        return await asyncio.to_thread(self.dirty_tasks)

    def replace_remote_tasks(self, tasks: Sequence[Task]) -> None:
        timestamp = time.time()
        with self._lock:
            self._conn.execute("DELETE FROM tasks WHERE pending_action IS NULL")
            self._conn.commit()
        for task in tasks:
            self._insert_or_update(
                task,
                pending_action=None,
                deleted=False,
                last_synced=timestamp,
                clear_pending=True,
            )

    def upsert_task(
        self,
        task: Task,
        *,
        pending_action: str | None = None,
        deleted: bool = False,
        last_synced: float | None = None,
        clear_pending: bool = False,
    ) -> None:
        self._insert_or_update(
            task,
            pending_action=pending_action,
            deleted=deleted,
            last_synced=last_synced,
            clear_pending=clear_pending,
        )

    def delete_task(self, uid: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM tasks WHERE uid = ?", (uid,))
            self._conn.commit()

    def get_task(self, uid: str) -> Task | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM tasks WHERE uid = ?", (uid,)).fetchone()
        if not row:
            return None
        return self._build_task(row)

    def get_pending_action(self, uid: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT pending_action FROM tasks WHERE uid = ?", (uid,)).fetchone()
        if not row:
            return None
        return row["pending_action"]

    def _insert_or_update(
        self,
        task: Task,
        *,
        pending_action: str | None,
        deleted: bool,
        last_synced: float | None,
        clear_pending: bool,
    ) -> None:
        summary = task.summary or task.uid
        status = task.status or "IN-PROCESS"
        due_value = task.due.isoformat() if task.due else None
        priority = task.priority
        x_props = _serialize_map(task.x_properties)
        categories = _serialize_properties(task.categories)
        href = task.href
        with self._lock:
            existing = self._conn.execute(
                "SELECT pending_action, last_synced FROM tasks WHERE uid = ?",
                (task.uid,)
            ).fetchone()
            if clear_pending:
                resolved_pending = None
            elif pending_action is not None:
                resolved_pending = pending_action
            else:
                resolved_pending = existing["pending_action"] if existing else None
            resolved_last_synced = last_synced if last_synced is not None else (existing["last_synced"] if existing else None)
            now = time.time()
            self._conn.execute(
                """
                INSERT INTO tasks (
                    uid,
                    summary,
                    status,
                    due,
                    priority,
                    x_properties,
                    categories,
                    href,
                    pending_action,
                    deleted,
                    last_synced,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uid) DO UPDATE SET
                    summary = excluded.summary,
                    status = excluded.status,
                    due = excluded.due,
                    priority = excluded.priority,
                    x_properties = excluded.x_properties,
                    categories = excluded.categories,
                    href = excluded.href,
                    pending_action = ?,
                    deleted = ?,
                    last_synced = ?,
                    updated_at = excluded.updated_at
                """,
                (
                    task.uid,
                    summary,
                    status,
                    due_value,
                    priority,
                    x_props,
                    categories,
                    href,
                    resolved_pending,
                    1 if deleted else 0,
                    resolved_last_synced,
                    now,
                    resolved_pending,
                    1 if deleted else 0,
                    resolved_last_synced,
                ),
            )
            self._conn.commit()

    def _build_task(self, row: sqlite3.Row) -> Task:
        due = None
        due_value = row["due"]
        if due_value:
            try:
                due = datetime.fromisoformat(due_value)
            except ValueError:
                due = None
        return Task(
            uid=row["uid"],
            summary=row["summary"],
            status=row["status"],
            due=due,
            priority=row["priority"],
            x_properties=_parse_json(row["x_properties"]),
            categories=_parse_list(row["categories"]),
            href=row["href"],
        )
