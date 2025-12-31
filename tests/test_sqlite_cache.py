from __future__ import annotations

from pathlib import Path

import pytest

from tdo.models import Task
from tdo.sqlite_cache import SqliteTaskCache


@pytest.mark.asyncio
async def test_sqlite_cache_preserves_pending(tmp_path: Path) -> None:
    cache = await SqliteTaskCache.create(tmp_path / "cache.db")
    try:
        pending = Task(uid="pending", summary="Pending")
        await cache.upsert_task(pending, pending_action="create")
        remote = Task(uid="remote", summary="Remote")
        await cache.replace_remote_tasks([remote])
        tasks = await cache.list_tasks()
        assert {task.uid for task in tasks} == {"pending", "remote"}
        dirty = await cache.dirty_tasks()
        assert len(dirty) == 1 and dirty[0].task.uid == "pending"
    finally:
        await cache.close()
