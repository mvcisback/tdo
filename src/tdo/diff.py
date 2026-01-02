from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Generic, Mapping, Sequence, TypeVar

from .models import Task, TaskData

if TYPE_CHECKING:
    from .sqlite_cache import SqliteTaskCache


__all__ = ["DiffMismatchError", "TaskDiff", "TaskSetDiff"]


class DiffMismatchError(Exception):
    """Raised when applying a diff to a task that doesn't match the expected pre-state."""

    pass


K = TypeVar("K")


@dataclass(frozen=True)
class TaskDiff:
    """Represents a change to a single task's data.

    - pre=None, post=TaskData: creation
    - pre=TaskData, post=None: deletion
    - pre=TaskData, post=TaskData: update
    - pre=None, post=None: invalid (noop)
    """

    pre: TaskData[datetime] | None = None
    post: TaskData[datetime] | None = None

    def __call__(self, task: TaskData[datetime]) -> TaskData[datetime] | None:
        """Apply this diff to a task. Returns new state or None for deletion.

        Raises DiffMismatchError if pre doesn't match the provided task.
        """
        if self.pre is not None and task != self.pre:
            raise DiffMismatchError(f"Expected {self.pre}, got {task}")
        return self.post

    def chain(self, other: TaskDiff) -> TaskDiff:
        """Compose two diffs: apply self, then other.

        Returns TaskDiff(self.pre, other.post).
        """
        return TaskDiff(pre=self.pre, post=other.post)

    def inv(self) -> TaskDiff:
        """Return the inverse diff: TaskDiff(self.post, self.pre)."""
        return TaskDiff(pre=self.post, post=self.pre)

    @property
    def is_create(self) -> bool:
        """True if this diff creates a new task."""
        return self.pre is None and self.post is not None

    @property
    def is_delete(self) -> bool:
        """True if this diff deletes an existing task."""
        return self.pre is not None and self.post is None

    @property
    def is_update(self) -> bool:
        """True if this diff updates an existing task (and changes it)."""
        return self.pre is not None and self.post is not None and self.pre != self.post

    @property
    def is_noop(self) -> bool:
        """True if this diff represents no change."""
        return self.pre == self.post

    def to_dict(self) -> dict[str, Any]:
        """Serialize TaskDiff to a JSON-compatible dict."""
        return {
            "pre": self.pre.to_dict() if self.pre else None,
            "post": self.post.to_dict() if self.post else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskDiff:
        """Deserialize TaskDiff from a dict."""
        pre_data = data.get("pre")
        post_data = data.get("post")
        return cls(
            pre=TaskData.from_dict(pre_data) if pre_data else None,
            post=TaskData.from_dict(post_data) if post_data else None,
        )


@dataclass(frozen=True)
class TaskSetDiff(Generic[K]):
    """Represents changes to a set of tasks, keyed by K (uid or task_index).

    Parametric over key type:
    - TaskSetDiff[str]: keyed by uid (for database operations)
    - TaskSetDiff[int]: keyed by task_index (for user-facing operations)
    """

    diffs: Mapping[K, TaskDiff] = field(default_factory=dict)

    def __call__(self, tasks: Sequence[Task], key_fn: Callable[[Task], K]) -> list[Task]:
        """Apply all diffs to a task list.

        Args:
            tasks: The list of tasks to transform.
            key_fn: Function to extract the key from a task.

        Returns:
            New list of tasks with diffs applied.

        Raises:
            DiffMismatchError if any diff's pre doesn't match.
        """
        # Index tasks by key
        task_by_key: dict[K, Task] = {key_fn(t): t for t in tasks}
        result: list[Task] = []

        # Track which keys we've processed
        processed_keys: set[K] = set()

        for key, diff in self.diffs.items():
            processed_keys.add(key)

            if diff.is_create:
                # Create: task shouldn't exist
                if key in task_by_key:
                    raise DiffMismatchError(f"Task with key {key} already exists for create")
                if diff.post is not None:
                    # We need uid to create Task, but we only have TaskData
                    # This requires the caller to provide uid separately
                    raise NotImplementedError(
                        "Creating tasks from TaskSetDiff requires uid; use from_task_lists instead"
                    )
            elif diff.is_delete:
                # Delete: task should exist and match pre
                if key not in task_by_key:
                    raise DiffMismatchError(f"Task with key {key} not found for delete")
                existing = task_by_key[key]
                diff(existing.data)  # Validates pre matches
                # Don't add to result (deleted)
            elif diff.is_update:
                # Update: task should exist and match pre
                if key not in task_by_key:
                    raise DiffMismatchError(f"Task with key {key} not found for update")
                existing = task_by_key[key]
                new_data = diff(existing.data)
                if new_data is not None:
                    result.append(
                        Task(
                            uid=existing.uid,
                            data=new_data,
                            href=existing.href,
                            task_index=existing.task_index,
                        )
                    )
            # noop: keep as-is
            elif diff.is_noop and key in task_by_key:
                result.append(task_by_key[key])

        # Add tasks that weren't in diffs
        for key, task in task_by_key.items():
            if key not in processed_keys:
                result.append(task)

        return result

    def chain(self, other: TaskSetDiff[K]) -> TaskSetDiff[K]:
        """Compose two TaskSetDiffs: apply self, then other.

        For keys in both: chain the individual diffs.
        For keys in only one: keep that diff.
        """
        all_keys = set(self.diffs.keys()) | set(other.diffs.keys())
        new_diffs: dict[K, TaskDiff] = {}

        for key in all_keys:
            self_diff = self.diffs.get(key)
            other_diff = other.diffs.get(key)

            if self_diff is not None and other_diff is not None:
                new_diffs[key] = self_diff.chain(other_diff)
            elif self_diff is not None:
                new_diffs[key] = self_diff
            elif other_diff is not None:
                new_diffs[key] = other_diff

        return TaskSetDiff(diffs=new_diffs)

    def inv(self) -> TaskSetDiff[K]:
        """Return the inverse of all diffs."""
        return TaskSetDiff(diffs={k: v.inv() for k, v in self.diffs.items()})

    def as_sql(self) -> list[tuple[str, tuple]]:
        """Convert to list of (sql, params) statements.

        Only works for TaskSetDiff[str] (uid-keyed).

        Note: With the three-table architecture (tasks, completed_tasks, deleted_tasks),
        this method handles simple cases. For complex undo operations that require
        moving between tables, use SqliteTaskCache methods directly.
        """
        statements: list[tuple[str, tuple]] = []

        for uid, diff in self.diffs.items():
            if not isinstance(uid, str):
                raise TypeError(f"as_sql requires str keys (uid), got {type(uid)}")

            if diff.is_noop:
                continue

            if diff.is_delete:
                # Delete from active tasks table
                statements.append(("DELETE FROM tasks WHERE uid = ?", (uid,)))

            elif diff.is_create and diff.post is not None:
                post = diff.post
                # Determine target table based on status
                if post.status == "COMPLETED":
                    sql = """
                        INSERT INTO completed_tasks (uid, summary, status, due, wait, priority, x_properties, categories, updated_at, completed_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(uid) DO UPDATE SET
                            summary = excluded.summary,
                            status = excluded.status,
                            due = excluded.due,
                            wait = excluded.wait,
                            priority = excluded.priority,
                            x_properties = excluded.x_properties,
                            categories = excluded.categories,
                            updated_at = excluded.updated_at,
                            completed_at = excluded.completed_at
                    """
                    now = datetime.now().timestamp()
                    params = (
                        uid,
                        post.summary or uid,
                        post.status,
                        post.due.isoformat() if post.due else None,
                        post.wait.isoformat() if post.wait else None,
                        post.priority,
                        _serialize_map(post.x_properties),
                        _serialize_list(post.categories),
                        now,
                        now,  # completed_at
                    )
                else:
                    sql = """
                        INSERT INTO tasks (uid, summary, status, due, wait, priority, x_properties, categories, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(uid) DO UPDATE SET
                            summary = excluded.summary,
                            status = excluded.status,
                            due = excluded.due,
                            wait = excluded.wait,
                            priority = excluded.priority,
                            x_properties = excluded.x_properties,
                            categories = excluded.categories,
                            updated_at = excluded.updated_at
                    """
                    params = (
                        uid,
                        post.summary or uid,
                        post.status or "IN-PROCESS",
                        post.due.isoformat() if post.due else None,
                        post.wait.isoformat() if post.wait else None,
                        post.priority,
                        _serialize_map(post.x_properties),
                        _serialize_list(post.categories),
                        datetime.now().timestamp(),
                    )
                statements.append((sql.strip(), params))

            elif diff.is_update and diff.post is not None:
                post = diff.post
                # Updates go to tasks table (completed tasks have their own update path)
                sql = """
                    UPDATE tasks SET
                        summary = ?,
                        status = ?,
                        due = ?,
                        wait = ?,
                        priority = ?,
                        x_properties = ?,
                        categories = ?,
                        updated_at = ?
                    WHERE uid = ?
                """
                params = (
                    post.summary or uid,
                    post.status or "IN-PROCESS",
                    post.due.isoformat() if post.due else None,
                    post.wait.isoformat() if post.wait else None,
                    post.priority,
                    _serialize_map(post.x_properties),
                    _serialize_list(post.categories),
                    datetime.now().timestamp(),
                    uid,
                )
                statements.append((sql.strip(), params))

        return statements

    def to_uid_keyed(self, resolver: Callable[[K], str]) -> TaskSetDiff[str]:
        """Convert to uid-keyed TaskSetDiff for database operations.

        Args:
            resolver: Function to convert key K to uid string.
        """
        return TaskSetDiff(diffs={resolver(k): v for k, v in self.diffs.items()})

    @classmethod
    def from_task_lists(
        cls, before: Sequence[Task], after: Sequence[Task]
    ) -> TaskSetDiff[str]:
        """Create a TaskSetDiff by comparing two task lists by uid.

        - Tasks in after but not before: creates
        - Tasks in before but not after: deletes
        - Tasks in both with different data: updates
        - Tasks in both with same data: noops (omitted)
        """
        before_by_uid = {t.uid: t for t in before}
        after_by_uid = {t.uid: t for t in after}

        all_uids = set(before_by_uid.keys()) | set(after_by_uid.keys())
        diffs: dict[str, TaskDiff] = {}

        for uid in all_uids:
            before_task = before_by_uid.get(uid)
            after_task = after_by_uid.get(uid)

            pre = before_task.data if before_task else None
            post = after_task.data if after_task else None

            # Skip noops
            if pre == post:
                continue

            diffs[uid] = TaskDiff(pre=pre, post=post)

        return cls(diffs=diffs)

    @classmethod
    async def from_cache_and_tasks(
        cls, cache: "SqliteTaskCache", tasks: Sequence[Task]
    ) -> TaskSetDiff[str]:
        """Create a TaskSetDiff by comparing cache state to new tasks.

        Delegates to from_task_lists after fetching cache contents.
        """
        cached_tasks = await cache.list_tasks()
        return cls.from_task_lists(cached_tasks, tasks)

    def pretty(self) -> str:
        """Return a human-readable summary of the changes."""
        creates = [(k, d) for k, d in self.diffs.items() if d.is_create]
        updates = [(k, d) for k, d in self.diffs.items() if d.is_update]
        deletes = [(k, d) for k, d in self.diffs.items() if d.is_delete]

        lines: list[str] = []

        if creates:
            lines.append(f"Created ({len(creates)}):")
            for key, diff in creates:
                summary = diff.post.summary if diff.post else "?"
                lines.append(f"  + [{key}] {summary}")

        if updates:
            lines.append(f"Updated ({len(updates)}):")
            for key, diff in updates:
                summary = diff.post.summary if diff.post else "?"
                lines.append(f"  ~ [{key}] {summary}")

        if deletes:
            lines.append(f"Deleted ({len(deletes)}):")
            for key, diff in deletes:
                summary = diff.pre.summary if diff.pre else "?"
                lines.append(f"  - [{key}] {summary}")

        if not lines:
            return "No changes"

        return "\n".join(lines)

    @property
    def created_count(self) -> int:
        """Number of tasks created."""
        return sum(1 for d in self.diffs.values() if d.is_create)

    @property
    def updated_count(self) -> int:
        """Number of tasks updated."""
        return sum(1 for d in self.diffs.values() if d.is_update)

    @property
    def deleted_count(self) -> int:
        """Number of tasks deleted."""
        return sum(1 for d in self.diffs.values() if d.is_delete)

    @property
    def is_empty(self) -> bool:
        """True if there are no changes."""
        return all(d.is_noop for d in self.diffs.values())

    def to_json(self) -> str:
        """Serialize TaskSetDiff to JSON string.

        Keys are converted to strings for JSON compatibility.
        """
        payload = {str(k): v.to_dict() for k, v in self.diffs.items()}
        return json.dumps(payload)

    @classmethod
    def from_json(cls, data: str) -> TaskSetDiff[str]:
        """Deserialize TaskSetDiff from JSON string.

        Returns a str-keyed TaskSetDiff since JSON keys are always strings.
        """
        payload = json.loads(data)
        diffs = {k: TaskDiff.from_dict(v) for k, v in payload.items()}
        return cls(diffs=diffs)


def _serialize_map(value: dict[str, str] | None) -> str:
    """Serialize a dict to JSON string."""
    return json.dumps(value or {})


def _serialize_list(value: list[str] | None) -> str:
    """Serialize a list to JSON string."""
    return json.dumps(list(value or []))
