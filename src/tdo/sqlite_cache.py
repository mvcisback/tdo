from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

import aiosqlite

from .models import Task, TaskData, TaskFilter

if TYPE_CHECKING:
    from .diff import TaskSetDiff


@dataclass
class DirtyTask:
    task: Task
    action: str
    deleted: bool


@dataclass
class TransactionLogEntry:
    id: int
    diff_json: str
    operation: str | None
    created_at: float


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
            last_synced REAL,
            updated_at REAL NOT NULL,
            task_index INTEGER UNIQUE
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due);
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_dirty ON tasks(pending_action);
        CREATE INDEX IF NOT EXISTS idx_tasks_index ON tasks(task_index);

        CREATE TABLE IF NOT EXISTS completed_tasks (
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
            last_synced REAL,
            updated_at REAL NOT NULL,
            completed_at REAL NOT NULL,
            task_index INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_completed_tasks_completed_at ON completed_tasks(completed_at);

        CREATE TABLE IF NOT EXISTS deleted_tasks (
            uid TEXT PRIMARY KEY,
            summary TEXT NOT NULL,
            status TEXT NOT NULL,
            due TEXT,
            wait TEXT,
            priority INTEGER,
            x_properties TEXT,
            categories TEXT,
            href TEXT,
            last_synced REAL,
            deleted_at REAL NOT NULL,
            task_index INTEGER
        );

        CREATE TABLE IF NOT EXISTS transaction_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            diff_json TEXT NOT NULL,
            operation TEXT,
            created_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_transaction_log_created ON transaction_log(created_at);
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

        # Migration: move deleted=1 rows to deleted_tasks, completed to completed_tasks
        if "deleted" in columns:
            await self._migrate_to_three_tables()

    async def _migrate_to_three_tables(self) -> None:
        """Migrate from single tasks table with deleted flag to three tables."""
        assert self._conn is not None
        now = time.time()

        # Move deleted tasks to deleted_tasks table
        await self._conn.execute(
            """
            INSERT OR IGNORE INTO deleted_tasks (
                uid, summary, status, due, wait, priority, x_properties,
                categories, href, last_synced, deleted_at, task_index
            )
            SELECT
                uid, summary, status, due, wait, priority, x_properties,
                categories, href, last_synced, ?, task_index
            FROM tasks WHERE deleted = 1
            """,
            (now,),
        )

        # Move completed tasks to completed_tasks table
        await self._conn.execute(
            """
            INSERT OR IGNORE INTO completed_tasks (
                uid, summary, status, due, wait, priority, x_properties,
                categories, href, pending_action, last_synced, updated_at,
                completed_at, task_index
            )
            SELECT
                uid, summary, status, due, wait, priority, x_properties,
                categories, href, pending_action, last_synced, updated_at,
                ?, task_index
            FROM tasks WHERE deleted = 0 AND status = 'COMPLETED'
            """,
            (now,),
        )

        # Remove migrated rows from tasks
        await self._conn.execute("DELETE FROM tasks WHERE deleted = 1")
        await self._conn.execute(
            "DELETE FROM tasks WHERE status = 'COMPLETED'"
        )

        # Remove the deleted column by recreating the table
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks_new (
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
                last_synced REAL,
                updated_at REAL NOT NULL,
                task_index INTEGER UNIQUE
            )
            """
        )
        await self._conn.execute(
            """
            INSERT INTO tasks_new (
                uid, summary, status, due, wait, priority, x_properties,
                categories, href, pending_action, last_synced, updated_at, task_index
            )
            SELECT
                uid, summary, status, due, wait, priority, x_properties,
                categories, href, pending_action, last_synced, updated_at, task_index
            FROM tasks
            """
        )
        await self._conn.execute("DROP TABLE tasks")
        await self._conn.execute("ALTER TABLE tasks_new RENAME TO tasks")

        # Recreate indices
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_dirty ON tasks(pending_action)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_index ON tasks(task_index)")

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
        """Get active task by its stable index."""
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM tasks WHERE task_index = ?",
            (index,)
        ) as cursor:
            row = await cursor.fetchone()
        return self._build_task(row) if row else None

    async def list_tasks(self) -> list[Task]:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM tasks ORDER BY due IS NULL, due"
        ) as cursor:
            rows = await cursor.fetchall()
        return [self._build_task(row) for row in rows]

    async def list_tasks_filtered(self, task_filter: TaskFilter | None = None) -> list[Task]:
        assert self._conn is not None
        conditions: list[str] = []
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

        if conditions:
            where = " WHERE " + " AND ".join(conditions)
        else:
            where = ""
        query = f"SELECT * FROM tasks{where} ORDER BY due IS NULL, due"

        async with self._conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return [self._build_task(row) for row in rows]

    async def dirty_tasks(self) -> list[DirtyTask]:
        """Return all tasks with pending changes to sync.

        Includes:
        - tasks with pending_action (create/update)
        - completed_tasks with pending_action (update)
        - all deleted_tasks (being in the table means pending delete)
        """
        assert self._conn is not None
        result: list[DirtyTask] = []

        # Pending creates/updates from active tasks
        async with self._conn.execute(
            "SELECT * FROM tasks WHERE pending_action IS NOT NULL ORDER BY updated_at"
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            result.append(DirtyTask(
                task=self._build_task(row),
                action=row["pending_action"],
                deleted=False,
            ))

        # Pending updates from completed tasks
        async with self._conn.execute(
            "SELECT * FROM completed_tasks WHERE pending_action IS NOT NULL ORDER BY updated_at"
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            result.append(DirtyTask(
                task=self._build_completed_task(row),
                action=row["pending_action"],
                deleted=False,
            ))

        # All deleted tasks are pending deletion
        async with self._conn.execute(
            "SELECT * FROM deleted_tasks ORDER BY deleted_at"
        ) as cursor:
            rows = await cursor.fetchall()
        for row in rows:
            result.append(DirtyTask(
                task=self._build_deleted_task(row),
                action="delete",
                deleted=True,
            ))

        return result

    async def replace_remote_tasks(self, tasks: Sequence[Task]) -> None:
        """Replace local cache with remote tasks.

        Routes tasks to appropriate tables based on status:
        - COMPLETED -> completed_tasks
        - Other statuses -> tasks (active)
        """
        timestamp = time.time()
        assert self._conn is not None

        # Preserve existing indices for tasks we're updating (from both tables)
        cursor = await self._conn.execute(
            "SELECT uid, task_index FROM tasks WHERE task_index IS NOT NULL"
        )
        existing_indices = {row[0]: row[1] for row in await cursor.fetchall()}

        # Also check completed_tasks for preserved indices
        cursor = await self._conn.execute(
            "SELECT uid, task_index FROM completed_tasks WHERE task_index IS NOT NULL"
        )
        existing_indices.update({row[0]: row[1] for row in await cursor.fetchall()})

        # Delete non-pending tasks from both tables
        await self._conn.execute("DELETE FROM tasks WHERE pending_action IS NULL")
        await self._conn.execute("DELETE FROM completed_tasks WHERE pending_action IS NULL")
        await self._conn.commit()

        # Track which active tasks need new indices
        tasks_needing_indices: list[str] = []

        for task in tasks:
            preserved_index = existing_indices.get(task.uid)

            if task.data.status == "COMPLETED":
                # Insert into completed_tasks
                await self._insert_completed_task(
                    task,
                    pending_action=None,
                    last_synced=timestamp,
                    completed_at=timestamp,
                    task_index=preserved_index,
                )
            else:
                # Insert into active tasks
                await self._insert_or_update(
                    task,
                    pending_action=None,
                    last_synced=timestamp,
                    clear_pending=True,
                    task_index=preserved_index,
                )
                if preserved_index is None:
                    tasks_needing_indices.append(task.uid)

        # Assign indices to new active tasks
        for uid in tasks_needing_indices:
            await self.assign_index(uid)

    async def upsert_task(
        self,
        task: Task,
        *,
        pending_action: str | None = None,
        last_synced: float | None = None,
        clear_pending: bool = False,
        task_index: int | None = None,
    ) -> None:
        await self._insert_or_update(
            task,
            pending_action=pending_action,
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
        last_synced: float | None,
        clear_pending: bool,
        task_index: int | None = None,
    ) -> None:
        """Insert or update a task in the active tasks table."""
        summary = task.data.summary or task.uid
        status = task.data.status or "IN-PROCESS"
        due_value = task.data.due.isoformat() if task.data.due else None
        wait_value = task.data.wait.isoformat() if task.data.wait else None
        priority = task.data.priority
        x_props = _serialize_map(task.data.x_properties)
        categories = _serialize_properties(task.data.categories)
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
                last_synced,
                updated_at,
                task_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                resolved_last_synced,
                now,
                resolved_index,
                resolved_pending,
                resolved_last_synced,
            ),
        )
        await self._conn.commit()

    async def _insert_completed_task(
        self,
        task: Task,
        *,
        pending_action: str | None,
        last_synced: float | None,
        completed_at: float,
        task_index: int | None = None,
    ) -> None:
        """Insert or update a task in the completed_tasks table."""
        summary = task.data.summary or task.uid
        status = task.data.status or "COMPLETED"
        due_value = task.data.due.isoformat() if task.data.due else None
        wait_value = task.data.wait.isoformat() if task.data.wait else None
        priority = task.data.priority
        x_props = _serialize_map(task.data.x_properties)
        categories = _serialize_properties(task.data.categories)
        href = task.href
        now = time.time()
        assert self._conn is not None
        await self._conn.execute(
            """
            INSERT INTO completed_tasks (
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
                last_synced,
                updated_at,
                completed_at,
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
                pending_action = excluded.pending_action,
                last_synced = excluded.last_synced,
                updated_at = excluded.updated_at,
                completed_at = excluded.completed_at,
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
                pending_action,
                last_synced,
                now,
                completed_at,
                task_index,
            ),
        )
        await self._conn.commit()

    async def _insert_deleted_task(
        self,
        task: Task,
        *,
        deleted_at: float,
        task_index: int | None = None,
    ) -> None:
        """Insert a task into the deleted_tasks table (pending deletion)."""
        summary = task.data.summary or task.uid
        status = task.data.status or "IN-PROCESS"
        due_value = task.data.due.isoformat() if task.data.due else None
        wait_value = task.data.wait.isoformat() if task.data.wait else None
        priority = task.data.priority
        x_props = _serialize_map(task.data.x_properties)
        categories = _serialize_properties(task.data.categories)
        assert self._conn is not None
        await self._conn.execute(
            """
            INSERT INTO deleted_tasks (
                uid,
                summary,
                status,
                due,
                wait,
                priority,
                x_properties,
                categories,
                href,
                last_synced,
                deleted_at,
                task_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                summary = excluded.summary,
                status = excluded.status,
                due = excluded.due,
                wait = excluded.wait,
                priority = excluded.priority,
                x_properties = excluded.x_properties,
                categories = excluded.categories,
                href = excluded.href,
                last_synced = excluded.last_synced,
                deleted_at = excluded.deleted_at,
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
                task.href,
                None,  # last_synced
                deleted_at,
                task_index,
            ),
        )
        await self._conn.commit()

    async def complete_task(self, uid: str) -> None:
        """Move a task from tasks to completed_tasks.

        The task's original index is preserved in completed_tasks for undo.
        """
        assert self._conn is not None

        # Get the task from active table
        async with self._conn.execute(
            "SELECT * FROM tasks WHERE uid = ?", (uid,)
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            raise KeyError(f"task {uid} not found in active tasks")

        task = self._build_task(row)
        pending = await self.get_pending_action(uid)
        now = time.time()

        # Insert into completed_tasks with status COMPLETED
        completed_task = Task(
            uid=task.uid,
            data=TaskData(
                summary=task.data.summary,
                status="COMPLETED",
                due=task.data.due,
                wait=task.data.wait,
                priority=task.data.priority,
                x_properties=task.data.x_properties,
                categories=task.data.categories,
            ),
            href=task.href,
            task_index=task.task_index,
        )

        # If task was never synced (pending create), completion is also a create
        # Otherwise it's an update
        pending_action = "create" if pending == "create" else "update"

        await self._insert_completed_task(
            completed_task,
            pending_action=pending_action,
            last_synced=row["last_synced"],
            completed_at=now,
            task_index=task.task_index,  # Preserve original index
        )

        # Remove from active tasks
        await self._conn.execute("DELETE FROM tasks WHERE uid = ?", (uid,))
        await self._conn.commit()

    async def mark_for_deletion(self, uid: str) -> None:
        """Move a task to deleted_tasks (pending deletion).

        Works for both active tasks and completed tasks.
        The task's original index is preserved for undo.
        """
        assert self._conn is not None
        now = time.time()

        # Try to find in active tasks first
        async with self._conn.execute(
            "SELECT * FROM tasks WHERE uid = ?", (uid,)
        ) as cursor:
            row = await cursor.fetchone()

        if row is not None:
            task = self._build_task(row)
            pending = await self.get_pending_action(uid)

            # If task was never synced, just delete it entirely
            if pending == "create":
                await self._conn.execute("DELETE FROM tasks WHERE uid = ?", (uid,))
                await self._conn.commit()
                return

            # Move to deleted_tasks
            await self._insert_deleted_task(
                task,
                deleted_at=now,
                task_index=task.task_index,
            )
            await self._conn.execute("DELETE FROM tasks WHERE uid = ?", (uid,))
            await self._conn.commit()
            return

        # Try completed_tasks
        async with self._conn.execute(
            "SELECT * FROM completed_tasks WHERE uid = ?", (uid,)
        ) as cursor:
            row = await cursor.fetchone()

        if row is not None:
            task = self._build_completed_task(row)
            pending = row["pending_action"]

            # If completion was never synced, just delete it entirely
            if pending == "create":
                await self._conn.execute("DELETE FROM completed_tasks WHERE uid = ?", (uid,))
                await self._conn.commit()
                return

            # Move to deleted_tasks
            await self._insert_deleted_task(
                task,
                deleted_at=now,
                task_index=task.task_index,
            )
            await self._conn.execute("DELETE FROM completed_tasks WHERE uid = ?", (uid,))
            await self._conn.commit()
            return

        raise KeyError(f"task {uid} not found")

    async def flush_deleted_tasks(self) -> None:
        """Delete all rows from deleted_tasks table (called after push)."""
        assert self._conn is not None
        await self._conn.execute("DELETE FROM deleted_tasks")
        await self._conn.commit()

    async def list_completed_tasks(self) -> list[Task]:
        """List all completed tasks."""
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM completed_tasks ORDER BY completed_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
        return [self._build_completed_task(row) for row in rows]

    async def list_deleted_tasks(self) -> list[Task]:
        """List all tasks pending deletion."""
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM deleted_tasks ORDER BY deleted_at"
        ) as cursor:
            rows = await cursor.fetchall()
        return [self._build_deleted_task(row) for row in rows]

    async def restore_from_completed(self, uid: str, *, status: str = "IN-PROCESS") -> Task:
        """Move a task from completed_tasks back to tasks.

        Used for undo. Tries to restore original index, falls back to new index.

        Args:
            uid: Task UID
            status: Status to set (default IN-PROCESS)

        Returns:
            The restored task
        """
        assert self._conn is not None

        async with self._conn.execute(
            "SELECT * FROM completed_tasks WHERE uid = ?", (uid,)
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            raise KeyError(f"task {uid} not found in completed_tasks")

        task = self._build_completed_task(row)
        original_index = task.task_index

        # Check if original index is available
        resolved_index = await self._try_restore_index(original_index)

        # Create restored task with new status
        restored_task = Task(
            uid=task.uid,
            data=TaskData(
                summary=task.data.summary,
                status=status,
                due=task.data.due,
                wait=task.data.wait,
                priority=task.data.priority,
                x_properties=task.data.x_properties,
                categories=task.data.categories,
            ),
            href=task.href,
            task_index=resolved_index,
        )

        # Insert into active tasks
        await self._insert_or_update(
            restored_task,
            pending_action=row["pending_action"],
            last_synced=row["last_synced"],
            clear_pending=False,
            task_index=resolved_index,
        )

        # Assign new index if original was unavailable
        if resolved_index is None:
            new_index = await self.assign_index(uid)
            restored_task.task_index = new_index

        # Remove from completed_tasks
        await self._conn.execute("DELETE FROM completed_tasks WHERE uid = ?", (uid,))
        await self._conn.commit()

        return restored_task

    async def restore_from_deleted(self, uid: str) -> Task:
        """Move a task from deleted_tasks back to tasks.

        Used for undo. Tries to restore original index, falls back to new index.

        Returns:
            The restored task
        """
        assert self._conn is not None

        async with self._conn.execute(
            "SELECT * FROM deleted_tasks WHERE uid = ?", (uid,)
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            raise KeyError(f"task {uid} not found in deleted_tasks")

        task = self._build_deleted_task(row)
        original_index = task.task_index

        # Check if original index is available
        resolved_index = await self._try_restore_index(original_index)

        # Restore task
        restored_task = Task(
            uid=task.uid,
            data=task.data,
            href=task.href,
            task_index=resolved_index,
        )

        # Insert into active tasks (no pending action since we're undoing a delete)
        await self._insert_or_update(
            restored_task,
            pending_action=None,
            last_synced=row["last_synced"],
            clear_pending=True,
            task_index=resolved_index,
        )

        # Assign new index if original was unavailable
        if resolved_index is None:
            new_index = await self.assign_index(uid)
            restored_task.task_index = new_index

        # Remove from deleted_tasks
        await self._conn.execute("DELETE FROM deleted_tasks WHERE uid = ?", (uid,))
        await self._conn.commit()

        return restored_task

    async def _try_restore_index(self, original_index: int | None) -> int | None:
        """Try to restore an original index, returning None if unavailable.

        Best effort: if original index is taken, returns None to signal
        that a new index should be assigned.
        """
        if original_index is None:
            return None

        assert self._conn is not None
        async with self._conn.execute(
            "SELECT 1 FROM tasks WHERE task_index = ?", (original_index,)
        ) as cursor:
            exists = await cursor.fetchone()

        if exists:
            # Index is taken, will need to assign new one
            return None

        return original_index

    async def get_completed_task(self, uid: str) -> Task | None:
        """Get a completed task by UID."""
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM completed_tasks WHERE uid = ?", (uid,)
        ) as cursor:
            row = await cursor.fetchone()
        return self._build_completed_task(row) if row else None

    async def get_deleted_task(self, uid: str) -> Task | None:
        """Get a task pending deletion by UID."""
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM deleted_tasks WHERE uid = ?", (uid,)
        ) as cursor:
            row = await cursor.fetchone()
        return self._build_deleted_task(row) if row else None

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
            data=TaskData(
                summary=row["summary"],
                status=row["status"],
                due=due,
                wait=wait,
                priority=row["priority"],
                x_properties=_parse_json(row["x_properties"]),
                categories=_parse_list(row["categories"]),
            ),
            href=row["href"],
            task_index=row["task_index"],
        )

    def _build_completed_task(self, row: aiosqlite.Row) -> Task:
        """Build a Task from a completed_tasks row."""
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
            data=TaskData(
                summary=row["summary"],
                status=row["status"],
                due=due,
                wait=wait,
                priority=row["priority"],
                x_properties=_parse_json(row["x_properties"]),
                categories=_parse_list(row["categories"]),
            ),
            href=row["href"],
            task_index=row["task_index"],
        )

    def _build_deleted_task(self, row: aiosqlite.Row) -> Task:
        """Build a Task from a deleted_tasks row."""
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
            data=TaskData(
                summary=row["summary"],
                status=row["status"],
                due=due,
                wait=wait,
                priority=row["priority"],
                x_properties=_parse_json(row["x_properties"]),
                categories=_parse_list(row["categories"]),
            ),
            href=None,  # deleted_tasks doesn't have href
            task_index=row["task_index"],
        )

    async def log_transaction(
        self,
        diff: "TaskSetDiff[str]",
        *,
        operation: str | None = None,
        max_entries: int = 32,
    ) -> None:
        """Record a TaskSetDiff to the transaction log.

        Maintains a FIFO queue of max_entries. Oldest entries are dropped
        when the limit is exceeded.

        Args:
            diff: The diff to record (must be uid-keyed)
            operation: Optional operation type (e.g., "pull", "push", "add")
            max_entries: Maximum log entries to retain
        """
        assert self._conn is not None

        diff_json = diff.to_json()
        now = time.time()

        # Insert new entry
        await self._conn.execute(
            """
            INSERT INTO transaction_log (diff_json, operation, created_at)
            VALUES (?, ?, ?)
            """,
            (diff_json, operation, now),
        )

        # Prune oldest entries beyond max_entries
        await self._conn.execute(
            """
            DELETE FROM transaction_log
            WHERE id NOT IN (
                SELECT id FROM transaction_log
                ORDER BY id DESC
                LIMIT ?
            )
            """,
            (max_entries,),
        )

        await self._conn.commit()

    async def get_transaction_log(
        self,
        limit: int | None = None,
    ) -> list[TransactionLogEntry]:
        """Retrieve transaction log entries.

        Args:
            limit: Maximum entries to return (None for all)

        Returns:
            List of TransactionLogEntry, ordered by newest first.
        """
        assert self._conn is not None

        query = "SELECT id, diff_json, operation, created_at FROM transaction_log ORDER BY id DESC"
        if limit:
            query += f" LIMIT {limit}"

        async with self._conn.execute(query) as cursor:
            rows = await cursor.fetchall()

        return [
            TransactionLogEntry(
                id=row[0],
                diff_json=row[1],
                operation=row[2],
                created_at=row[3],
            )
            for row in rows
        ]

    async def clear_transaction_log(self) -> int:
        """Clear all transaction log entries.

        Returns:
            Number of entries deleted.
        """
        assert self._conn is not None

        async with self._conn.execute("SELECT COUNT(*) FROM transaction_log") as cursor:
            row = await cursor.fetchone()
            count = row[0] if row else 0

        await self._conn.execute("DELETE FROM transaction_log")
        await self._conn.commit()

        return count

    async def pop_transaction(self) -> TransactionLogEntry | None:
        """Pop the newest transaction log entry.

        Returns the entry and deletes it from the log.
        Returns None if the log is empty.
        """
        assert self._conn is not None

        # Get the newest entry
        async with self._conn.execute(
            "SELECT id, diff_json, operation, created_at FROM transaction_log ORDER BY id DESC LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            return None

        entry = TransactionLogEntry(
            id=row[0],
            diff_json=row[1],
            operation=row[2],
            created_at=row[3],
        )

        # Delete the entry
        await self._conn.execute("DELETE FROM transaction_log WHERE id = ?", (entry.id,))
        await self._conn.commit()

        return entry
