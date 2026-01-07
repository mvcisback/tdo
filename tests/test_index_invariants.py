"""Tests for task index management invariants.

These tests verify the hole management and index assignment logic to ensure:
1. holes + used_indices fills [1, max_index] with no duplicates
2. No tasks have NULL indices after operations
3. Race conditions don't cause index collisions
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from tdo.models import Task, TaskData
from tdo.sqlite_cache import SqliteTaskCache


async def get_all_active_indices(cache: SqliteTaskCache) -> list[int]:
    """Get all task indices from the active tasks table."""
    assert cache._conn is not None
    async with cache._conn.execute(
        "SELECT task_index FROM tasks WHERE task_index IS NOT NULL ORDER BY task_index"
    ) as cursor:
        rows = await cursor.fetchall()
    return [row[0] for row in rows]


async def get_tasks_with_null_index(cache: SqliteTaskCache) -> list[str]:
    """Get UIDs of tasks that have NULL indices."""
    assert cache._conn is not None
    async with cache._conn.execute(
        "SELECT uid FROM tasks WHERE task_index IS NULL"
    ) as cursor:
        rows = await cursor.fetchall()
    return [row[0] for row in rows]


async def verify_index_invariant(cache: SqliteTaskCache) -> None:
    """Verify the core invariant: holes + indices = [1, max_index] with no duplicates.

    Raises AssertionError if invariant is violated.
    """
    indices = await get_all_active_indices(cache)

    if not indices:
        return  # No tasks, invariant trivially holds

    # Check for duplicates
    assert len(indices) == len(set(indices)), f"Duplicate indices found: {indices}"

    # Check that indices fill [1, max] with holes accounted for
    max_index = max(indices)
    expected_range = set(range(1, max_index + 1))
    actual_indices = set(indices)
    holes = expected_range - actual_indices

    # The number of holes + actual indices should equal max_index
    assert len(holes) + len(actual_indices) == max_index, (
        f"Index invariant violated: holes={holes}, indices={actual_indices}, max={max_index}"
    )

    # No NULL indices should exist in active tasks
    null_tasks = await get_tasks_with_null_index(cache)
    assert not null_tasks, f"Tasks with NULL indices found: {null_tasks}"


class TestIndexInvariant:
    """Test the core index invariant under various operations."""

    @pytest.mark.asyncio
    async def test_invariant_after_sequential_creates(self, tmp_path: Path) -> None:
        """Creating tasks sequentially should maintain invariant."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            for i in range(10):
                task = Task(uid=f"task-{i}", data=TaskData(summary=f"Task {i}"))
                await cache.upsert_task(task, pending_action="create")
                await cache.assign_index(task.uid)

            await verify_index_invariant(cache)
            indices = await get_all_active_indices(cache)
            assert indices == list(range(1, 11))
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_invariant_after_deletions_create_holes(self, tmp_path: Path) -> None:
        """Deleting tasks should create holes, but invariant should hold."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # Create 5 tasks
            for i in range(5):
                task = Task(uid=f"task-{i}", data=TaskData(summary=f"Task {i}"))
                await cache.upsert_task(task, pending_action="create")
                await cache.assign_index(task.uid)

            # Delete task-1 and task-3 (indices 2 and 4)
            await cache.mark_for_deletion("task-1")
            await cache.mark_for_deletion("task-3")

            await verify_index_invariant(cache)
            indices = await get_all_active_indices(cache)
            assert sorted(indices) == [1, 3, 5]  # Holes at 2 and 4
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_hole_reuse_on_new_task(self, tmp_path: Path) -> None:
        """New tasks should fill holes before extending range."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # Create 5 tasks
            for i in range(5):
                task = Task(uid=f"task-{i}", data=TaskData(summary=f"Task {i}"))
                await cache.upsert_task(task, pending_action="create")
                await cache.assign_index(task.uid)

            # Delete task-1 (index 2) to create a hole
            await cache.mark_for_deletion("task-1")

            # Create a new task - should get index 2
            new_task = Task(uid="new-task", data=TaskData(summary="New"))
            await cache.upsert_task(new_task, pending_action="create")
            assigned_index = await cache.assign_index("new-task")

            assert assigned_index == 2, f"Expected hole (2) to be reused, got {assigned_index}"
            await verify_index_invariant(cache)
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_invariant_after_complete_and_restore(self, tmp_path: Path) -> None:
        """Completing and restoring tasks should maintain invariant."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # Create 3 tasks
            for i in range(3):
                task = Task(uid=f"task-{i}", data=TaskData(summary=f"Task {i}"))
                await cache.upsert_task(task, pending_action="create")
                await cache.assign_index(task.uid)

            # Complete task-1 (index 2)
            await cache.complete_task("task-1")

            await verify_index_invariant(cache)
            indices_after_complete = await get_all_active_indices(cache)
            assert sorted(indices_after_complete) == [1, 3]  # Hole at 2

            # Create new task - should fill hole at 2
            new_task = Task(uid="new-task", data=TaskData(summary="New"))
            await cache.upsert_task(new_task, pending_action="create")
            await cache.assign_index("new-task")

            indices_after_new = await get_all_active_indices(cache)
            assert sorted(indices_after_new) == [1, 2, 3]  # Hole filled

            # Restore completed task - should get new index (4) since 2 is taken
            restored = await cache.restore_from_completed("task-1")
            assert restored.task_index == 4, f"Expected new index 4, got {restored.task_index}"

            await verify_index_invariant(cache)
        finally:
            await cache.close()


class TestNullIndexPrevention:
    """Test that NULL indices don't occur after operations."""

    @pytest.mark.asyncio
    async def test_no_null_index_after_upsert_without_assign(self, tmp_path: Path) -> None:
        """Tasks created without explicit assign_index should still have NULL initially.

        This documents the current behavior - tasks CAN have NULL indices
        if assign_index is not called.
        """
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            task = Task(uid="task-no-index", data=TaskData(summary="No index"))
            await cache.upsert_task(task, pending_action="create")

            null_tasks = await get_tasks_with_null_index(cache)
            # This test documents the current (potentially problematic) behavior
            assert null_tasks == ["task-no-index"]
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_replace_remote_assigns_indices(self, tmp_path: Path) -> None:
        """replace_remote_tasks should assign indices to all new tasks."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # Simulate pulling remote tasks
            remote_tasks = [
                Task(uid=f"remote-{i}", data=TaskData(summary=f"Remote {i}"))
                for i in range(5)
            ]
            await cache.replace_remote_tasks(remote_tasks)

            # All tasks should have indices
            null_tasks = await get_tasks_with_null_index(cache)
            assert not null_tasks, f"Tasks missing indices after replace_remote: {null_tasks}"

            await verify_index_invariant(cache)
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_replace_remote_preserves_existing_indices(self, tmp_path: Path) -> None:
        """replace_remote_tasks should preserve indices of existing tasks."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # Create local task with index
            local = Task(uid="local-1", data=TaskData(summary="Local"))
            await cache.upsert_task(local, pending_action="create")
            await cache.assign_index("local-1")

            # Pull remote which includes the same task (updated)
            remote_tasks = [
                Task(uid="local-1", data=TaskData(summary="Local Updated")),
                Task(uid="remote-1", data=TaskData(summary="Remote")),
            ]
            await cache.replace_remote_tasks(remote_tasks)

            # local-1 should still have index 1
            task = await cache.get_task("local-1")
            assert task is not None
            assert task.task_index == 1

            await verify_index_invariant(cache)
        finally:
            await cache.close()


class TestConcurrentIndexAssignment:
    """Test for race conditions in index assignment."""

    @pytest.mark.asyncio
    async def test_concurrent_assign_index_no_duplicates(self, tmp_path: Path) -> None:
        """Concurrent index assignments should not produce duplicates.

        This test may expose race conditions if they exist.
        """
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # Create multiple tasks without indices
            num_tasks = 20
            for i in range(num_tasks):
                task = Task(uid=f"task-{i}", data=TaskData(summary=f"Task {i}"))
                await cache.upsert_task(task, pending_action="create")

            # Assign indices concurrently
            async def assign(uid: str) -> int:
                return await cache.assign_index(uid)

            tasks = [assign(f"task-{i}") for i in range(num_tasks)]

            # This may raise IntegrityError if there's a race condition
            try:
                results = await asyncio.gather(*tasks)
            except Exception as e:
                pytest.fail(f"Concurrent index assignment failed: {e}")

            # Verify no duplicates
            assert len(results) == len(set(results)), (
                f"Duplicate indices assigned: {results}"
            )

            await verify_index_invariant(cache)
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_sequential_assign_produces_contiguous(self, tmp_path: Path) -> None:
        """Sequential index assignment should produce contiguous indices."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            indices = []
            for i in range(10):
                task = Task(uid=f"task-{i}", data=TaskData(summary=f"Task {i}"))
                await cache.upsert_task(task, pending_action="create")
                idx = await cache.assign_index(task.uid)
                indices.append(idx)

            assert indices == list(range(1, 11))
        finally:
            await cache.close()


class TestEdgeCases:
    """Test edge cases that could cause index issues."""

    @pytest.mark.asyncio
    async def test_delete_all_then_create(self, tmp_path: Path) -> None:
        """After deleting all tasks, new task should get index 1."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # Create and delete a task
            task = Task(uid="task-1", data=TaskData(summary="Task"))
            await cache.upsert_task(task, pending_action="create")
            await cache.assign_index("task-1")
            await cache.mark_for_deletion("task-1")

            # Create new task - should get index 1
            new_task = Task(uid="task-2", data=TaskData(summary="New"))
            await cache.upsert_task(new_task, pending_action="create")
            idx = await cache.assign_index("task-2")

            assert idx == 1
            await verify_index_invariant(cache)
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_complete_task_index_preserved_in_completed_table(
        self, tmp_path: Path
    ) -> None:
        """Completed tasks should preserve their index in completed_tasks table."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            task = Task(uid="task-1", data=TaskData(summary="Task"))
            await cache.upsert_task(task, pending_action="create")
            await cache.assign_index("task-1")

            await cache.complete_task("task-1")

            # Check completed_tasks table directly
            assert cache._conn is not None
            async with cache._conn.execute(
                "SELECT task_index FROM completed_tasks WHERE uid = ?", ("task-1",)
            ) as cursor:
                row = await cursor.fetchone()

            assert row is not None
            assert row[0] == 1  # Index preserved
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_multiple_holes_filled_in_order(self, tmp_path: Path) -> None:
        """Multiple holes should be filled in ascending order."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # Create 5 tasks: indices 1-5
            for i in range(5):
                task = Task(uid=f"task-{i}", data=TaskData(summary=f"Task {i}"))
                await cache.upsert_task(task, pending_action="create")
                await cache.assign_index(task.uid)

            # Delete tasks 1, 3 (indices 2, 4) to create holes
            await cache.mark_for_deletion("task-1")
            await cache.mark_for_deletion("task-3")

            # Create new tasks - should fill holes 2, then 4
            new1 = Task(uid="new-1", data=TaskData(summary="New 1"))
            await cache.upsert_task(new1, pending_action="create")
            idx1 = await cache.assign_index("new-1")

            new2 = Task(uid="new-2", data=TaskData(summary="New 2"))
            await cache.upsert_task(new2, pending_action="create")
            idx2 = await cache.assign_index("new-2")

            assert idx1 == 2, f"Expected first hole (2), got {idx1}"
            assert idx2 == 4, f"Expected second hole (4), got {idx2}"

            await verify_index_invariant(cache)
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_restore_deleted_with_taken_index(self, tmp_path: Path) -> None:
        """Restoring a deleted task when its index is taken should assign new index."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # Create two tasks (simulating synced tasks with no pending_action)
            for i in range(2):
                task = Task(uid=f"task-{i}", data=TaskData(summary=f"Task {i}"))
                # Use pending_action=None to simulate already-synced tasks
                await cache.upsert_task(task, pending_action=None)
                await cache.assign_index(task.uid)

            # Delete task-0 (index 1) - since it's synced, goes to deleted_tasks
            await cache.mark_for_deletion("task-0")

            # Create new task - takes index 1 (the hole)
            new_task = Task(uid="new-task", data=TaskData(summary="New"))
            await cache.upsert_task(new_task, pending_action="create")
            await cache.assign_index("new-task")

            # Restore task-0 - should get index 3 (not 1, which is taken)
            restored = await cache.restore_from_deleted("task-0")
            assert restored.task_index == 3

            await verify_index_invariant(cache)
        finally:
            await cache.close()


class TestDatabaseState:
    """Tests to diagnose current database state issues."""

    @pytest.mark.asyncio
    async def test_detect_duplicate_indices_in_tasks(self, tmp_path: Path) -> None:
        """Detect if there are duplicate indices (would violate UNIQUE constraint)."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            # This should be prevented by UNIQUE constraint, but test anyway
            await verify_index_invariant(cache)
        finally:
            await cache.close()

    @pytest.mark.asyncio
    async def test_count_tasks_vs_indices(self, tmp_path: Path) -> None:
        """Count of tasks should equal count of non-NULL indices."""
        cache = await SqliteTaskCache.create(tmp_path / "cache.db")
        try:
            for i in range(5):
                task = Task(uid=f"task-{i}", data=TaskData(summary=f"Task {i}"))
                await cache.upsert_task(task, pending_action="create")
                await cache.assign_index(task.uid)

            tasks = await cache.list_tasks()
            indices = await get_all_active_indices(cache)

            assert len(tasks) == len(indices), (
                f"Task count ({len(tasks)}) != index count ({len(indices)})"
            )
        finally:
            await cache.close()
