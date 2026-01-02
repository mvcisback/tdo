from __future__ import annotations

from datetime import datetime

import pytest

from tdo.diff import DiffMismatchError, TaskDiff, TaskSetDiff
from tdo.models import Task, TaskData


def _make_task_data(
    summary: str = "test",
    status: str = "NEEDS-ACTION",
    due: datetime | None = None,
    priority: int | None = None,
) -> TaskData[datetime]:
    return TaskData(
        summary=summary,
        status=status,
        due=due,
        priority=priority,
        x_properties={},
        categories=[],
    )


def _make_task(
    uid: str,
    summary: str = "test",
    status: str = "NEEDS-ACTION",
    due: datetime | None = None,
    task_index: int | None = None,
) -> Task:
    return Task(
        uid=uid,
        data=_make_task_data(summary=summary, status=status, due=due),
        task_index=task_index,
    )


class TestTaskDiff:
    def test_is_create(self) -> None:
        diff = TaskDiff(pre=None, post=_make_task_data("new"))
        assert diff.is_create
        assert not diff.is_delete
        assert not diff.is_update
        assert not diff.is_noop

    def test_is_delete(self) -> None:
        diff = TaskDiff(pre=_make_task_data("old"), post=None)
        assert diff.is_delete
        assert not diff.is_create
        assert not diff.is_update
        assert not diff.is_noop

    def test_is_update(self) -> None:
        diff = TaskDiff(pre=_make_task_data("old"), post=_make_task_data("new"))
        assert diff.is_update
        assert not diff.is_create
        assert not diff.is_delete
        assert not diff.is_noop

    def test_is_noop(self) -> None:
        data = _make_task_data("same")
        diff = TaskDiff(pre=data, post=data)
        assert diff.is_noop
        assert not diff.is_create
        assert not diff.is_delete
        assert not diff.is_update

    def test_call_with_matching_pre(self) -> None:
        pre = _make_task_data("old")
        post = _make_task_data("new")
        diff = TaskDiff(pre=pre, post=post)

        result = diff(pre)
        assert result == post

    def test_call_with_mismatching_pre_raises(self) -> None:
        pre = _make_task_data("old")
        post = _make_task_data("new")
        diff = TaskDiff(pre=pre, post=post)

        wrong = _make_task_data("wrong")
        with pytest.raises(DiffMismatchError):
            diff(wrong)

    def test_call_create_accepts_any_task(self) -> None:
        post = _make_task_data("new")
        diff = TaskDiff(pre=None, post=post)

        # Create diff has pre=None, so it accepts any input
        result = diff(_make_task_data("anything"))
        assert result == post

    def test_call_delete_returns_none(self) -> None:
        pre = _make_task_data("old")
        diff = TaskDiff(pre=pre, post=None)

        result = diff(pre)
        assert result is None

    def test_chain_composition(self) -> None:
        a = _make_task_data("A")
        b = _make_task_data("B")
        c = _make_task_data("C")

        d1 = TaskDiff(pre=a, post=b)
        d2 = TaskDiff(pre=b, post=c)

        chained = d1.chain(d2)
        assert chained.pre == a
        assert chained.post == c

    def test_inverse(self) -> None:
        pre = _make_task_data("old")
        post = _make_task_data("new")
        diff = TaskDiff(pre=pre, post=post)

        inv = diff.inv()
        assert inv.pre == post
        assert inv.post == pre

    def test_chain_with_inverse_is_noop(self) -> None:
        pre = _make_task_data("old")
        post = _make_task_data("new")
        diff = TaskDiff(pre=pre, post=post)

        result = diff.chain(diff.inv())
        assert result.pre == pre
        assert result.post == pre
        assert result.is_noop


class TestTaskSetDiff:
    def test_from_task_lists_detects_create(self) -> None:
        before: list[Task] = []
        after = [_make_task("uid1", "new task")]

        diff = TaskSetDiff.from_task_lists(before, after)

        assert "uid1" in diff.diffs
        assert diff.diffs["uid1"].is_create
        assert diff.created_count == 1
        assert diff.updated_count == 0
        assert diff.deleted_count == 0

    def test_from_task_lists_detects_delete(self) -> None:
        before = [_make_task("uid1", "old task")]
        after: list[Task] = []

        diff = TaskSetDiff.from_task_lists(before, after)

        assert "uid1" in diff.diffs
        assert diff.diffs["uid1"].is_delete
        assert diff.deleted_count == 1

    def test_from_task_lists_detects_update(self) -> None:
        before = [_make_task("uid1", "old summary")]
        after = [_make_task("uid1", "new summary")]

        diff = TaskSetDiff.from_task_lists(before, after)

        assert "uid1" in diff.diffs
        assert diff.diffs["uid1"].is_update
        assert diff.updated_count == 1

    def test_from_task_lists_ignores_unchanged(self) -> None:
        task = _make_task("uid1", "same")
        before = [task]
        after = [task]

        diff = TaskSetDiff.from_task_lists(before, after)

        assert "uid1" not in diff.diffs
        assert diff.is_empty

    def test_chain_merges_diffs(self) -> None:
        a = _make_task_data("A")
        b = _make_task_data("B")
        c = _make_task_data("C")
        x = _make_task_data("X")
        y = _make_task_data("Y")

        d1 = TaskSetDiff(diffs={"1": TaskDiff(a, b), "2": TaskDiff(x, y)})
        d2 = TaskSetDiff(diffs={"1": TaskDiff(b, c), "3": TaskDiff(None, _make_task_data("new"))})

        chained = d1.chain(d2)

        # Key "1" should be chained: A -> C
        assert chained.diffs["1"].pre == a
        assert chained.diffs["1"].post == c

        # Key "2" should remain: X -> Y
        assert chained.diffs["2"].pre == x
        assert chained.diffs["2"].post == y

        # Key "3" should be from d2: None -> new
        assert chained.diffs["3"].is_create

    def test_inv_inverts_all_diffs(self) -> None:
        a = _make_task_data("A")
        b = _make_task_data("B")

        diff = TaskSetDiff(diffs={"1": TaskDiff(a, b)})
        inv = diff.inv()

        assert inv.diffs["1"].pre == b
        assert inv.diffs["1"].post == a

    def test_chain_with_inverse_is_noop(self) -> None:
        a = _make_task_data("A")
        b = _make_task_data("B")

        diff = TaskSetDiff(diffs={"1": TaskDiff(a, b)})
        result = diff.chain(diff.inv())

        assert result.diffs["1"].is_noop

    def test_as_sql_generates_delete(self) -> None:
        diff = TaskSetDiff(diffs={"uid1": TaskDiff(pre=_make_task_data("old"), post=None)})
        statements = diff.as_sql()

        assert len(statements) == 1
        sql, params = statements[0]
        assert "DELETE" in sql
        assert params == ("uid1",)

    def test_as_sql_generates_insert(self) -> None:
        post = _make_task_data("new task", status="NEEDS-ACTION")
        diff = TaskSetDiff(diffs={"uid1": TaskDiff(pre=None, post=post)})
        statements = diff.as_sql()

        assert len(statements) == 1
        sql, params = statements[0]
        assert "INSERT" in sql
        assert "uid1" in params

    def test_as_sql_generates_update(self) -> None:
        pre = _make_task_data("old")
        post = _make_task_data("new")
        diff = TaskSetDiff(diffs={"uid1": TaskDiff(pre=pre, post=post)})
        statements = diff.as_sql()

        assert len(statements) == 1
        sql, params = statements[0]
        assert "UPDATE" in sql
        assert "uid1" in params

    def test_as_sql_skips_noop(self) -> None:
        data = _make_task_data("same")
        diff = TaskSetDiff(diffs={"uid1": TaskDiff(pre=data, post=data)})
        statements = diff.as_sql()

        assert len(statements) == 0

    def test_to_uid_keyed(self) -> None:
        a = _make_task_data("A")
        b = _make_task_data("B")

        # TaskSetDiff keyed by int (task_index)
        diff: TaskSetDiff[int] = TaskSetDiff(diffs={1: TaskDiff(a, b), 2: TaskDiff(None, _make_task_data("new"))})

        # Resolver maps index to uid
        resolver = lambda idx: f"uid-{idx}"

        uid_keyed = diff.to_uid_keyed(resolver)

        assert "uid-1" in uid_keyed.diffs
        assert "uid-2" in uid_keyed.diffs
        assert uid_keyed.diffs["uid-1"].pre == a
        assert uid_keyed.diffs["uid-1"].post == b

    def test_pretty_output(self) -> None:
        diff = TaskSetDiff(
            diffs={
                "uid1": TaskDiff(pre=None, post=_make_task_data("created task")),
                "uid2": TaskDiff(pre=_make_task_data("old"), post=_make_task_data("updated task")),
                "uid3": TaskDiff(pre=_make_task_data("deleted task"), post=None),
            }
        )

        pretty = diff.pretty()

        assert "Created (1)" in pretty
        assert "created task" in pretty
        assert "Updated (1)" in pretty
        assert "updated task" in pretty
        assert "Deleted (1)" in pretty
        assert "deleted task" in pretty

    def test_pretty_empty(self) -> None:
        diff: TaskSetDiff[str] = TaskSetDiff(diffs={})
        assert diff.pretty() == "No changes"

    def test_apply_to_task_list_update(self) -> None:
        old_data = _make_task_data("old")
        new_data = _make_task_data("new")
        task = Task(uid="uid1", data=old_data, task_index=1)

        diff: TaskSetDiff[str] = TaskSetDiff(diffs={"uid1": TaskDiff(pre=old_data, post=new_data)})

        result = diff([task], key_fn=lambda t: t.uid)

        assert len(result) == 1
        assert result[0].data.summary == "new"

    def test_apply_to_task_list_delete(self) -> None:
        data = _make_task_data("to delete")
        task = Task(uid="uid1", data=data, task_index=1)

        diff: TaskSetDiff[str] = TaskSetDiff(diffs={"uid1": TaskDiff(pre=data, post=None)})

        result = diff([task], key_fn=lambda t: t.uid)

        assert len(result) == 0

    def test_apply_to_task_list_preserves_unaffected(self) -> None:
        data1 = _make_task_data("task1")
        data2 = _make_task_data("task2")
        task1 = Task(uid="uid1", data=data1, task_index=1)
        task2 = Task(uid="uid2", data=data2, task_index=2)

        # Only update task1
        new_data = _make_task_data("updated")
        diff: TaskSetDiff[str] = TaskSetDiff(diffs={"uid1": TaskDiff(pre=data1, post=new_data)})

        result = diff([task1, task2], key_fn=lambda t: t.uid)

        assert len(result) == 2
        uids = {t.uid for t in result}
        assert uids == {"uid1", "uid2"}

    def test_apply_raises_on_mismatch(self) -> None:
        pre = _make_task_data("expected")
        post = _make_task_data("new")
        wrong_data = _make_task_data("wrong")

        task = Task(uid="uid1", data=wrong_data, task_index=1)
        diff: TaskSetDiff[str] = TaskSetDiff(diffs={"uid1": TaskDiff(pre=pre, post=post)})

        with pytest.raises(DiffMismatchError):
            diff([task], key_fn=lambda t: t.uid)
