"""Interface to the tdo-core Rust binary for fast database operations."""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any


class TdoCoreError(Exception):
    """Error from tdo-core binary."""

    pass


def _find_tdo_core() -> str:
    """Find the tdo-core binary path."""
    # Check if it's in PATH
    path = shutil.which("tdo-core")
    if path:
        return path
    # Fallback: assume it's installed alongside tdo
    raise TdoCoreError("tdo-core binary not found in PATH")


def _call_core(command: dict[str, Any], env: str = "default") -> Any:
    """Call tdo-core with a JSON command and return parsed result."""
    cmd = command.copy()
    cmd["env"] = env

    try:
        binary = _find_tdo_core()
        result = subprocess.run(
            [binary, json.dumps(cmd)],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            error_msg = result.stderr.strip() if result.stderr else f"tdo-core exited with code {result.returncode}"
            raise TdoCoreError(error_msg)

        if not result.stdout.strip():
            return None

        return json.loads(result.stdout)

    except FileNotFoundError:
        raise TdoCoreError("tdo-core binary not found")
    except json.JSONDecodeError as e:
        raise TdoCoreError(f"Invalid JSON response from tdo-core: {e}")


@dataclass
class CoreTask:
    """Task data from tdo-core."""

    uid: str
    index: int
    summary: str
    status: str
    due: str | None = None
    wait: str | None = None
    priority: int | None = None
    tags: list[str] | None = None
    project: str | None = None
    url: str | None = None
    attachments: list[str] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CoreTask":
        return cls(
            uid=data["uid"],
            index=data["index"],
            summary=data["summary"],
            status=data["status"],
            due=data.get("due"),
            wait=data.get("wait"),
            priority=data.get("priority"),
            tags=data.get("tags"),
            project=data.get("project"),
            url=data.get("url"),
            attachments=data.get("attachments"),
        )

    def to_task(self) -> "Task":
        """Convert to the Task model used by the CLI."""
        from datetime import datetime

        from .models import Attachment, Task, TaskData

        # Parse datetime strings
        def parse_dt(s: str | None) -> datetime | None:
            if not s:
                return None
            # Try ISO format with timezone
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except ValueError:
                pass
            # Try without timezone
            try:
                return datetime.fromisoformat(s)
            except ValueError:
                pass
            return None

        # Build x_properties
        x_properties: dict[str, str] = {}
        if self.project:
            x_properties["X-PROJECT"] = self.project

        # Build attachments
        attachments = [Attachment(uri=uri) for uri in (self.attachments or [])]

        data = TaskData(
            summary=self.summary,
            status=self.status,
            due=parse_dt(self.due),
            wait=parse_dt(self.wait),
            priority=self.priority,
            x_properties=x_properties,
            categories=self.tags,
            url=self.url,
            attachments=attachments,
        )

        return Task(
            uid=self.uid,
            data=data,
            href=None,
            task_index=self.index,
        )


def list_tasks(env: str = "default") -> list[CoreTask]:
    """List all active tasks."""
    result = _call_core({"command": "list"}, env)
    if not result:
        return []
    return [CoreTask.from_dict(t) for t in result]


def show_tasks(indices: list[int], env: str = "default") -> list[CoreTask]:
    """Get specific tasks by index."""
    result = _call_core({"command": "show", "indices": indices}, env)
    if not result:
        return []
    return [CoreTask.from_dict(t) for t in result]


def add_task(
    summary: str,
    *,
    status: str | None = None,
    due: str | None = None,
    wait: str | None = None,
    priority: int | None = None,
    project: str | None = None,
    tags: list[str] | None = None,
    url: str | None = None,
    env: str = "default",
) -> CoreTask | None:
    """Add a new task."""
    task_data: dict[str, Any] = {"summary": summary}
    if status:
        task_data["status"] = status
    if due:
        task_data["due"] = due
    if wait:
        task_data["wait"] = wait
    if priority is not None:
        task_data["priority"] = priority
    if project:
        task_data["project"] = project
    if tags:
        task_data["tags"] = tags
    if url:
        task_data["url"] = url

    result = _call_core({"command": "add", "task": task_data}, env)
    if result and result.get("task"):
        return CoreTask.from_dict(result["task"])
    return None


def modify_tasks(
    indices: list[int],
    *,
    summary: str | None = None,
    status: str | None = None,
    due: str | None = None,
    wait: str | None = None,
    priority: int | None = None,
    project: str | None = None,
    add_tags: list[str] | None = None,
    remove_tags: list[str] | None = None,
    url: str | None = None,
    env: str = "default",
) -> list[CoreTask]:
    """Modify tasks."""
    changes: dict[str, Any] = {}
    if summary is not None:
        changes["summary"] = summary
    if status is not None:
        changes["status"] = status
    if due is not None:
        changes["due"] = due
    if wait is not None:
        changes["wait"] = wait
    if priority is not None:
        changes["priority"] = priority
    if project is not None:
        changes["project"] = project
    if add_tags:
        changes["add_tags"] = add_tags
    if remove_tags:
        changes["remove_tags"] = remove_tags
    if url is not None:
        changes["url"] = url

    result = _call_core({"command": "modify", "indices": indices, "changes": changes}, env)
    if result and result.get("tasks"):
        return [CoreTask.from_dict(t) for t in result["tasks"]]
    return []


def complete_tasks(indices: list[int], env: str = "default") -> list[CoreTask]:
    """Mark tasks as complete."""
    result = _call_core({"command": "do", "indices": indices}, env)
    if result and result.get("tasks"):
        return [CoreTask.from_dict(t) for t in result["tasks"]]
    return []


def start_tasks(indices: list[int], env: str = "default") -> list[CoreTask]:
    """Start tasks (set status to IN-PROCESS)."""
    result = _call_core({"command": "start", "indices": indices}, env)
    if result and result.get("tasks"):
        return [CoreTask.from_dict(t) for t in result["tasks"]]
    return []


def stop_tasks(indices: list[int], env: str = "default") -> list[CoreTask]:
    """Stop tasks (set status to NEEDS-ACTION)."""
    result = _call_core({"command": "stop", "indices": indices}, env)
    if result and result.get("tasks"):
        return [CoreTask.from_dict(t) for t in result["tasks"]]
    return []


def delete_tasks(indices: list[int], env: str = "default") -> list[CoreTask]:
    """Delete tasks."""
    result = _call_core({"command": "delete", "indices": indices}, env)
    if result and result.get("tasks"):
        return [CoreTask.from_dict(t) for t in result["tasks"]]
    return []


def move_tasks(indices: list[int], dest_env: str, env: str = "default") -> list[CoreTask]:
    """Move tasks to another environment."""
    result = _call_core({"command": "move", "indices": indices, "dest_env": dest_env}, env)
    if result and result.get("tasks"):
        return [CoreTask.from_dict(t) for t in result["tasks"]]
    return []


def log_transaction(
    diff_json: str,
    operation: str,
    max_entries: int = 100,
    env: str = "default",
) -> bool:
    """Log a transaction for undo support."""
    result = _call_core(
        {
            "command": "log_transaction",
            "diff_json": diff_json,
            "operation": operation,
            "max_entries": max_entries,
        },
        env,
    )
    return result and result.get("success", False)


@dataclass
class TransactionEntry:
    """Transaction log entry from tdo-core."""

    id: int
    diff_json: str
    operation: str
    created_at: float


def pop_transaction(env: str = "default") -> TransactionEntry | None:
    """Get and remove the newest transaction entry."""
    result = _call_core({"command": "pop_transaction"}, env)
    if result and result.get("entry"):
        e = result["entry"]
        return TransactionEntry(
            id=e["id"],
            diff_json=e["diff_json"],
            operation=e["operation"],
            created_at=e["created_at"],
        )
    return None


def get_completions(completion_type: str, env: str = "default") -> list[tuple[int, str]] | list[str]:
    """Get shell completion data.

    For 'tasks', returns list of (index, summary) tuples.
    For other types, returns list of strings.
    """
    try:
        binary = _find_tdo_core()
        result = subprocess.run(
            [binary, "complete", completion_type, env],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            return []

        lines = result.stdout.strip().split("\n") if result.stdout.strip() else []

        if completion_type == "tasks":
            completions = []
            for line in lines:
                if "\t" in line:
                    idx, summary = line.split("\t", 1)
                    completions.append((int(idx), summary))
            return completions
        else:
            return lines

    except (FileNotFoundError, ValueError):
        return []
