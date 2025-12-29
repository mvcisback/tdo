from __future__ import annotations

import asyncio
from pathlib import Path

from tdo.models import Task
from tdo.sqlite_cache import SqliteTaskCache


def test_sqlite_cache_preserves_pending(tmp_path: Path) -> None:
    cache = SqliteTaskCache(tmp_path / "cache.db")
    pending = Task(uid="pending", summary="Pending")
    cache.upsert_task(pending, pending_action="create")
    remote = Task(uid="remote", summary="Remote")
    cache.replace_remote_tasks([remote])
    tasks = cache.list_tasks()
    assert {task.uid for task in tasks} == {"pending", "remote"}
    dirty = cache.dirty_tasks()
    assert len(dirty) == 1 and dirty[0].task.uid == "pending"
    async_tasks = asyncio.run(cache.list_tasks_async())
    assert len(async_tasks) == 2
