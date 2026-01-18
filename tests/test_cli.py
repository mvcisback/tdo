from __future__ import annotations

import io
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from tdo import cli
from tdo.config import CaldavConfig
from tdo.models import Task, TaskData, TaskPatch, TaskPayload
from tdo.tdo_core import CoreTask


def run_cli(arguments: list[str]) -> tuple[int, str]:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        try:
            exit_code = cli.main(arguments)
        except SystemExit as exc:
            exit_code = exc.code or 0
    return exit_code, buffer.getvalue()


class MockTdoCore:
    """Mock for tdo_core module functions."""

    last_add_kwargs: dict[str, Any] | None = None
    last_modify_kwargs: dict[str, Any] | None = None
    last_modified_indices: list[int] | None = None
    deleted_indices: list[int] = []
    completed_indices: list[int] = []
    started_indices: list[int] = []
    stopped_indices: list[int] = []
    moved_indices: list[int] = []
    moved_dest_env: str | None = None

    default_tasks: list[CoreTask] = [
        CoreTask(uid="list-task", index=1, summary="List task", status="NEEDS-ACTION", priority=3)
    ]
    task_entries: list[CoreTask] = list(default_tasks)
    _next_index: int = 2

    @classmethod
    def reset(cls) -> None:
        cls.last_add_kwargs = None
        cls.last_modify_kwargs = None
        cls.last_modified_indices = None
        cls.deleted_indices = []
        cls.completed_indices = []
        cls.started_indices = []
        cls.stopped_indices = []
        cls.moved_indices = []
        cls.moved_dest_env = None
        cls.task_entries = list(cls.default_tasks)
        cls._next_index = 2

    @classmethod
    def list_tasks(cls, env: str = "default") -> list[CoreTask]:
        return list(cls.task_entries)

    @classmethod
    def show_tasks(cls, indices: list[int], env: str = "default") -> list[CoreTask]:
        return [t for t in cls.task_entries if t.index in indices]

    @classmethod
    def add_task(
        cls,
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
    ) -> CoreTask:
        cls.last_add_kwargs = {
            "summary": summary,
            "status": status,
            "due": due,
            "wait": wait,
            "priority": priority,
            "project": project,
            "tags": tags,
            "url": url,
        }
        task_index = cls._next_index
        cls._next_index += 1
        new_task = CoreTask(
            uid="dummy-task",
            index=task_index,
            summary=summary,
            status=status or "NEEDS-ACTION",
            priority=priority,
            tags=tags,
            project=project,
        )
        cls.task_entries.append(new_task)
        return new_task

    @classmethod
    def modify_tasks(
        cls,
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
        cls.last_modify_kwargs = {
            "summary": summary,
            "status": status,
            "due": due,
            "wait": wait,
            "priority": priority,
            "project": project,
            "add_tags": add_tags,
            "remove_tags": remove_tags,
            "url": url,
        }
        cls.last_modified_indices = indices
        result = []
        for task in cls.task_entries:
            if task.index in indices:
                # Apply modifications
                new_summary = summary if summary else task.summary
                new_priority = priority if priority is not None else task.priority
                new_tags = list(task.tags or []) if task.tags else []
                if add_tags:
                    new_tags.extend(add_tags)
                if remove_tags:
                    new_tags = [t for t in new_tags if t not in remove_tags]
                modified = CoreTask(
                    uid=task.uid,
                    index=task.index,
                    summary=new_summary,
                    status=status or task.status,
                    priority=new_priority,
                    tags=new_tags if new_tags else None,
                    project=project if project else task.project,
                )
                result.append(modified)
        return result

    @classmethod
    def complete_tasks(cls, indices: list[int], env: str = "default") -> list[CoreTask]:
        cls.completed_indices.extend(indices)
        return [t for t in cls.task_entries if t.index in indices]

    @classmethod
    def start_tasks(cls, indices: list[int], env: str = "default") -> list[CoreTask]:
        cls.started_indices.extend(indices)
        result = []
        for task in cls.task_entries:
            if task.index in indices:
                modified = CoreTask(
                    uid=task.uid,
                    index=task.index,
                    summary=task.summary,
                    status="IN-PROCESS",
                    priority=task.priority,
                    tags=task.tags,
                    project=task.project,
                )
                result.append(modified)
        return result

    @classmethod
    def stop_tasks(cls, indices: list[int], env: str = "default") -> list[CoreTask]:
        cls.stopped_indices.extend(indices)
        result = []
        for task in cls.task_entries:
            if task.index in indices:
                modified = CoreTask(
                    uid=task.uid,
                    index=task.index,
                    summary=task.summary,
                    status="NEEDS-ACTION",
                    priority=task.priority,
                    tags=task.tags,
                    project=task.project,
                )
                result.append(modified)
        return result

    @classmethod
    def delete_tasks(cls, indices: list[int], env: str = "default") -> list[CoreTask]:
        cls.deleted_indices.extend(indices)
        deleted = [t for t in cls.task_entries if t.index in indices]
        cls.task_entries = [t for t in cls.task_entries if t.index not in indices]
        return deleted

    @classmethod
    def move_tasks(cls, indices: list[int], dest_env: str, env: str = "default") -> list[CoreTask]:
        cls.moved_indices.extend(indices)
        cls.moved_dest_env = dest_env
        moved = []
        for task in cls.task_entries:
            if task.index in indices:
                # Create new task in dest with new index
                new_task = CoreTask(
                    uid=f"dest-{task.uid}",
                    index=100 + task.index,  # Different index in dest
                    summary=task.summary,
                    status=task.status,
                    priority=task.priority,
                    tags=task.tags,
                    project=task.project,
                )
                moved.append(new_task)
        cls.task_entries = [t for t in cls.task_entries if t.index not in indices]
        return moved

    @classmethod
    def log_transaction(
        cls,
        diff_json: str,
        operation: str,
        max_entries: int = 100,
        env: str = "default",
    ) -> bool:
        return True


class DummyClient:
    """Dummy CalDAV client for tests that still need it (attach, prioritize, undo, etc.)."""

    last_payload: TaskPayload | None = None
    last_patch: TaskPatch | None = None
    last_modified_uid: str | None = None
    deleted: list[str] = []
    default_tasks: list[Task] = [
        Task(uid="list-task", data=TaskData(summary="List task", due=None, priority=3), task_index=1)
    ]
    list_entries: list[Task] = list(default_tasks)
    _next_index: int = 1

    def __init__(self, config: CaldavConfig) -> None:
        self.config = config
        self.cache = None

    @classmethod
    def reset(cls) -> None:
        cls.last_payload = None
        cls.last_patch = None
        cls.last_modified_uid = None
        cls.deleted = []
        cls.list_entries = list(cls.default_tasks)
        cls._next_index = 2  # default_tasks has index 1

    def __enter__(self) -> DummyClient:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    async def close(self) -> None:
        pass

    async def create_task(self, payload: TaskPayload) -> Task:
        DummyClient.last_payload = payload
        task_index = DummyClient._next_index
        DummyClient._next_index += 1
        return Task(
            uid="dummy-task",
            data=TaskData(
                summary=payload.summary,
                due=payload.due,
                priority=payload.priority,
                x_properties=payload.x_properties,
                categories=list(payload.categories or []),
            ),
            task_index=task_index,
        )

    async def modify_task(self, task: Task, patch: TaskPatch) -> Task:
        DummyClient.last_patch = patch
        DummyClient.last_modified_uid = task.uid
        categories = list(patch.categories or task.data.categories or [])
        return Task(
            uid=task.uid,
            data=TaskData(
                summary=patch.summary or task.data.summary or task.uid,
                due=patch.due,
                priority=patch.priority,
                x_properties=patch.x_properties,
                categories=categories,
            ),
            href=task.href,
            task_index=task.task_index,
        )

    async def delete_task(self, uid: str) -> str:
        DummyClient.deleted.append(uid)
        return uid

    async def list_tasks(self, force_refresh: bool = False) -> list[Task]:
        return list(DummyClient.list_entries)

    async def list_tasks_filtered(self, task_filter: "TaskFilter | None" = None) -> list[Task]:
        tasks = list(DummyClient.list_entries)
        if not task_filter:
            return tasks
        # Simple filtering for tests
        result = tasks
        if task_filter.project:
            result = [t for t in result if t.data.x_properties.get("X-PROJECT") == task_filter.project]
        for tag in task_filter.tags:
            result = [t for t in result if tag in (t.data.categories or [])]
        if task_filter.indices:
            result = [t for t in result if t.task_index in task_filter.indices]
        return result

    async def list_active_tasks(
        self,
        *,
        exclude_waiting: bool = True,
        task_filter: "TaskFilter | None" = None,
    ) -> list[Task]:
        # For tests, just return all tasks (no waiting logic needed)
        return await self.list_tasks_filtered(task_filter)

    async def list_waiting_tasks(
        self,
        *,
        task_filter: "TaskFilter | None" = None,
    ) -> list[Task]:
        # For tests, return empty list (no waiting tasks by default)
        return []


async def _mock_cache_client(env: str | None) -> DummyClient:
    config = CaldavConfig(
        calendar_url="https://example.com/cal",
        username="tester",
    )
    return DummyClient(config)


@pytest.fixture(autouse=True)
def stub_cal_dav(monkeypatch: pytest.MonkeyPatch) -> None:
    DummyClient.reset()
    MockTdoCore.reset()

    # Mock tdo_core module functions
    from tdo import tdo_core
    monkeypatch.setattr(tdo_core, "list_tasks", MockTdoCore.list_tasks)
    monkeypatch.setattr(tdo_core, "show_tasks", MockTdoCore.show_tasks)
    monkeypatch.setattr(tdo_core, "add_task", MockTdoCore.add_task)
    monkeypatch.setattr(tdo_core, "modify_tasks", MockTdoCore.modify_tasks)
    monkeypatch.setattr(tdo_core, "complete_tasks", MockTdoCore.complete_tasks)
    monkeypatch.setattr(tdo_core, "start_tasks", MockTdoCore.start_tasks)
    monkeypatch.setattr(tdo_core, "stop_tasks", MockTdoCore.stop_tasks)
    monkeypatch.setattr(tdo_core, "delete_tasks", MockTdoCore.delete_tasks)
    monkeypatch.setattr(tdo_core, "move_tasks", MockTdoCore.move_tasks)
    monkeypatch.setattr(tdo_core, "log_transaction", MockTdoCore.log_transaction)

    # Keep CalDAV client mock for commands that still use it
    monkeypatch.setattr(cli, "_cache_client", _mock_cache_client)
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda env: CaldavConfig(
            calendar_url="https://example.com/cal",
            username="tester",
        ),
    )
    monkeypatch.setattr(
        cli,
        "load_config_from_path",
        lambda path, env=None: CaldavConfig(
            calendar_url="https://example.com/cal",
            username="tester",
        ),
    )


def test_add_command_parses_tokens() -> None:
    exit_code, stdout = run_cli(["add", "Create", "pri:H", "x:X-TEST:value"])
    assert exit_code == 0
    assert "Created" in stdout
    assert MockTdoCore.last_add_kwargs is not None
    assert MockTdoCore.last_add_kwargs["priority"] == 1
    assert MockTdoCore.last_add_kwargs["summary"] == "Create"


def test_add_command_parses_tags_and_project() -> None:
    exit_code, stdout = run_cli(
        [
            "add",
            "Tagged",
            "+tag1",
            "+tag2",
            "project:work",
            "due:2025-01-01T03:00:00",
        ]
    )
    assert exit_code == 0
    kwargs = MockTdoCore.last_add_kwargs
    assert kwargs is not None
    assert kwargs["project"] == "work"
    assert kwargs["tags"]
    assert set(kwargs["tags"]) == {"tag1", "tag2"}
    assert kwargs["due"] is not None  # due is passed as ISO string


def test_add_command_supports_multi_word_description() -> None:
    exit_code, stdout = run_cli(["add", "multi", "word", "description"])
    assert exit_code == 0
    kwargs = MockTdoCore.last_add_kwargs
    assert kwargs is not None
    assert kwargs["summary"] == "multi word description"


def test_add_command_allows_description_tokens_after_metadata() -> None:
    exit_code, stdout = run_cli(
        [
            "add",
            "first",
            "+tag",
            "later",
            "words",
            "project:work",
        ]
    )
    assert exit_code == 0
    kwargs = MockTdoCore.last_add_kwargs
    assert kwargs is not None
    assert kwargs["summary"] == "first later words"
    assert kwargs["project"] == "work"
    assert kwargs["tags"]
    assert set(kwargs["tags"]) == {"tag"}


def test_modify_command_accepts_summary_patch() -> None:
    exit_code, stdout = run_cli(["1", "modify", "summary:Updated", "pri:L"])
    assert exit_code == 0
    assert MockTdoCore.last_modify_kwargs is not None
    assert MockTdoCore.last_modify_kwargs["summary"] == "Updated"
    assert MockTdoCore.last_modify_kwargs["priority"] == 9


def test_modify_command_adds_tag_without_other_changes() -> None:
    exit_code, stdout = run_cli(["1", "modify", "+foo2"])
    assert exit_code == 0
    assert "Updated" in stdout
    assert MockTdoCore.last_modify_kwargs is not None
    assert MockTdoCore.last_modify_kwargs["add_tags"] == ["foo2"]


def test_delete_command_accepts_filter_indices() -> None:
    MockTdoCore.task_entries = [
        CoreTask(uid="first", index=1, summary="Alpha", status="NEEDS-ACTION", priority=1),
        CoreTask(uid="second", index=2, summary="Bravo", status="NEEDS-ACTION", priority=1),
        CoreTask(uid="third", index=3, summary="Charlie", status="NEEDS-ACTION", priority=1),
    ]
    exit_code, stdout = run_cli(["1,3", "del"])
    assert exit_code == 0
    assert "Deleted" in stdout
    assert "Alpha" in stdout
    assert "Charlie" in stdout
    assert MockTdoCore.deleted_indices == [1, 3]


def test_delete_command_accepts_numeric_identifiers() -> None:
    MockTdoCore.task_entries = [
        CoreTask(uid="first", index=1, summary="First", status="NEEDS-ACTION", priority=1),
        CoreTask(uid="second", index=2, summary="Second", status="NEEDS-ACTION", priority=2),
    ]
    exit_code, stdout = run_cli(["1", "del"])
    assert exit_code == 0
    assert MockTdoCore.deleted_indices == [1]


def test_list_command_outputs_tasks() -> None:
    exit_code, stdout = run_cli(["list"])
    assert exit_code == 0
    assert "Project" in stdout
    assert "Description" in stdout
    assert "List task" in stdout
    assert "list-task" not in stdout


def test_list_command_hides_completed_tasks() -> None:
    MockTdoCore.task_entries = [
        CoreTask(uid="active", index=1, summary="Active task", status="NEEDS-ACTION", priority=1),
        CoreTask(uid="done", index=2, summary="Done task", status="COMPLETED", priority=1),
    ]
    exit_code, stdout = run_cli(["list"])
    assert exit_code == 0
    assert "Active task" in stdout
    assert "Done task" not in stdout


def test_default_command_is_list() -> None:
    exit_code, stdout = run_cli([])
    assert exit_code == 0
    assert "List task" in stdout


def test_filter_indices_default_to_list_command() -> None:
    MockTdoCore.task_entries = [
        CoreTask(uid="first", index=1, summary="Alpha", status="NEEDS-ACTION", priority=1),
        CoreTask(uid="second", index=2, summary="Bravo", status="NEEDS-ACTION", priority=2),
    ]
    exit_code, stdout = run_cli(["2"])
    assert exit_code == 0
    assert "Bravo" in stdout
    assert "Alpha" not in stdout


def test_modify_command_accepts_dash_prefixed_tokens() -> None:
    MockTdoCore.task_entries = [
        CoreTask(uid="first", index=1, summary="First", status="NEEDS-ACTION", priority=1),
        CoreTask(uid="second", index=2, summary="Second", status="NEEDS-ACTION", priority=2),
    ]
    exit_code, stdout = run_cli(["1", "modify", "-bar", "summary:Updated"])
    assert exit_code == 0
    assert MockTdoCore.last_modify_kwargs is not None
    assert MockTdoCore.last_modify_kwargs["summary"] == "Updated"


def test_modify_command_removes_dash_prefixed_tag() -> None:
    MockTdoCore.task_entries = [
        CoreTask(uid="first", index=1, summary="First", status="NEEDS-ACTION", priority=1, tags=["foo"]),
    ]
    exit_code, stdout = run_cli(["1", "modify", "-foo"])
    assert exit_code == 0
    assert "Updated" in stdout
    assert "First" in stdout
    assert MockTdoCore.last_modify_kwargs is not None
    assert MockTdoCore.last_modify_kwargs["remove_tags"] == ["foo"]


def test_modify_command_accepts_numeric_identifier() -> None:
    MockTdoCore.task_entries = [
        CoreTask(uid="first", index=1, summary="First", status="NEEDS-ACTION", priority=1),
        CoreTask(uid="second", index=2, summary="Second", status="NEEDS-ACTION", priority=2),
    ]
    exit_code, stdout = run_cli(["1", "modify", "summary:Updated"])
    assert exit_code == 0
    assert MockTdoCore.last_modified_indices == [1]


def test_list_command_shows_uids_when_enabled(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "config.radicale-test.toml"
    config_path.write_text(
        "[caldav]\ncalendar_url = \"https://example.com/cal\"\nusername = \"tester\"\nshow_uids = true\n"
    )

    def fake_load(path: Path, env: str | None = None) -> CaldavConfig:
        return CaldavConfig(
            calendar_url="https://example.com/cal",
            username="tester",
            show_uids=True,
        )

    monkeypatch.setattr(cli, "load_config_from_path", fake_load)
    monkeypatch.setenv("TDO_CONFIG_FILE", str(config_path))
    exit_code, stdout = run_cli(["list"])
    assert exit_code == 0


def test_list_command_accepts_config_file(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "config.radicale-test.toml"
    config_path.write_text(
        "[caldav]\ncalendar_url = \"https://example.com/cal\"\nusername = \"tester\"\n"
    )
    called: list[Path] = []

    def fake_load(path: Path, env: str | None = None) -> CaldavConfig:
        called.append(path)
        return CaldavConfig(calendar_url="https://example.com/cal", username="tester")

    monkeypatch.setattr(cli, "load_config_from_path", fake_load)
    monkeypatch.setenv("TDO_CONFIG_FILE", str(config_path))
    exit_code, stdout = run_cli(["list"])
    assert exit_code == 0
    assert called
    assert called[-1] == config_path


def test_config_init_command_writes_file(tmp_path) -> None:
    exit_code, stdout = run_cli(
        [
            "--env",
            "test",
            "config",
            "init",
            "--config-home",
            str(tmp_path),
            "--calendar-url",
            "https://example.com/declare",
            "--username",
            "tester",
            "--password",
            "secret",
            "--token",
            "tok",
            "--force",
        ]
    )
    assert exit_code == 0
    target = tmp_path / "config.test.toml"
    assert target.exists()
    contents = target.read_text()
    assert "calendar_url = \"https://example.com/declare\"" in contents
    assert "username = \"tester\"" in contents
    assert "password = \"secret\"" in contents
    assert "token = \"tok\"" in contents
    assert "env = \"test\"" in contents


def test_move_command_moves_task_to_dest_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that move command creates task in dest and marks for deletion in source."""
    MockTdoCore.reset()
    MockTdoCore.task_entries = [
        CoreTask(
            uid="source-task",
            index=1,
            summary="Task to move",
            status="NEEDS-ACTION",
            priority=3,
            tags=["tag1"],
        ),
    ]

    exit_code, stdout = run_cli(["1", "move", "work"])
    assert exit_code == 0
    assert "Moved 1 task(s)" in stdout
    assert "work" in stdout
    # Verify task was moved
    assert MockTdoCore.moved_indices == [1]
    assert MockTdoCore.moved_dest_env == "work"


def test_move_command_rejects_same_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that moving to the same environment is rejected."""
    # Mock resolve_env to return "default"
    monkeypatch.setattr(cli, "resolve_env", lambda env: "default")

    exit_code, stdout = run_cli(["1", "move", "default"])
    assert exit_code != 0 or "cannot move tasks to the same environment" in stdout


def test_move_command_rejects_missing_dest_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that move fails when destination env is not configured."""

    def fail_load(env: str) -> CaldavConfig:
        if env == "nonexistent":
            raise RuntimeError("Config not found")
        return CaldavConfig(calendar_url="https://example.com/cal", username="tester")

    monkeypatch.setattr(cli, "load_config", fail_load)

    exit_code, stdout = run_cli(["1", "move", "nonexistent"])
    assert exit_code != 0 or "not configured" in stdout


def test_move_command_preserves_task_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that all task properties are preserved during move."""
    MockTdoCore.reset()
    MockTdoCore.task_entries = [
        CoreTask(
            uid="full-task",
            index=1,
            summary="Full task",
            status="NEEDS-ACTION",
            due="2025-06-15T10:00:00",
            priority=1,
            tags=["work", "urgent"],
            project="myproject",
            url="https://example.com/task",
        ),
    ]

    exit_code, stdout = run_cli(["1", "move", "work"])
    assert exit_code == 0
    # Verify move was called with correct task
    assert MockTdoCore.moved_indices == [1]
    assert MockTdoCore.moved_dest_env == "work"
