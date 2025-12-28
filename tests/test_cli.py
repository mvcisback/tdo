from __future__ import annotations

import io
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path

import pytest

from todo import cli
from todo.config import CaldavConfig
from todo.models import Task, TaskPatch, TaskPayload


def run_cli(arguments: list[str]) -> tuple[int, str]:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        try:
            exit_code = cli.main(arguments)
        except SystemExit as exc:
            exit_code = exc.code or 0
    return exit_code, buffer.getvalue()


class DummyClient:
    last_payload: TaskPayload | None = None
    last_patch: TaskPatch | None = None
    last_modified_uid: str | None = None
    deleted: list[str] = []
    default_tasks: list[Task] = [
        Task(uid="list-task", summary="List task", due=None, priority=3)
    ]
    list_entries: list[Task] = list(default_tasks)

    def __init__(self, config: CaldavConfig) -> None:
        self.config = config

    @classmethod
    def reset(cls) -> None:
        cls.last_payload = None
        cls.last_patch = None
        cls.last_modified_uid = None
        cls.deleted = []
        cls.list_entries = list(cls.default_tasks)

    def __enter__(self) -> DummyClient:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def create_task(self, payload: TaskPayload) -> Task:
        DummyClient.last_payload = payload
        return Task(
            uid="dummy-task",
            summary=payload.summary,
            due=payload.due,
            priority=payload.priority,
            x_properties=payload.x_properties,
            categories=list(payload.categories or []),
        )

    def modify_task(self, uid: str, patch: TaskPatch) -> Task:
        DummyClient.last_patch = patch
        DummyClient.last_modified_uid = uid
        return Task(
            uid=uid,
            summary=patch.summary or uid,
            due=patch.due,
            priority=patch.priority,
            x_properties=patch.x_properties,
            categories=list(patch.categories or []),
        )

    def delete_task(self, uid: str) -> str:
        DummyClient.deleted.append(uid)
        return uid

    def list_tasks(self) -> list[Task]:
        return list(DummyClient.list_entries)


@pytest.fixture(autouse=True)
def stub_cal_dav(monkeypatch: pytest.MonkeyPatch) -> None:
    DummyClient.reset()
    monkeypatch.setattr(cli, "_CLIENT_FACTORY", DummyClient)
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
        lambda path: CaldavConfig(
            calendar_url="https://example.com/cal",
            username="tester",
        ),
    )


def test_add_command_parses_tokens() -> None:
    exit_code, stdout = run_cli(["add", "Create", "pri:H", "x:X-TEST:value"])
    assert exit_code == 0
    assert "dummy-task" in stdout
    assert DummyClient.last_payload is not None
    assert DummyClient.last_payload.priority == 1
    assert DummyClient.last_payload.x_properties == {"X-TEST": "value"}


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
    payload = DummyClient.last_payload
    assert payload is not None
    assert payload.x_properties.get("X-PROJECT") == "work"
    assert payload.categories
    assert set(payload.categories) == {"tag1", "tag2"}
    assert payload.due == datetime(2025, 1, 1, 3, 0, 0)


def test_modify_command_accepts_summary_patch() -> None:
    exit_code, stdout = run_cli(["modify", "existing", "summary:Updated", "pri:L"])
    assert exit_code == 0
    assert DummyClient.last_patch is not None
    assert DummyClient.last_patch.summary == "Updated"
    assert DummyClient.last_patch.priority == 9


def test_modify_command_adds_tag_without_other_changes() -> None:
    exit_code, stdout = run_cli(["1", "modify", "+foo2"])
    assert exit_code == 0
    assert "modified 1 tasks" in stdout
    assert DummyClient.last_patch is not None
    assert DummyClient.last_patch.categories == ["foo2"]


def test_delete_command_accepts_filter_indices() -> None:
    DummyClient.list_entries = [
        Task(uid="first", summary="Alpha", due=None, priority=1),
        Task(uid="second", summary="Bravo", due=None, priority=1),
        Task(uid="third", summary="Charlie", due=None, priority=1),
    ]
    exit_code, stdout = run_cli(["1,3", "del"])
    assert exit_code == 0
    assert "deleted 2 tasks" in stdout
    assert DummyClient.deleted == ["first", "third"]


def test_delete_command_accepts_numeric_identifiers() -> None:
    DummyClient.list_entries = [
        Task(uid="first", summary="First", due=None, priority=1),
        Task(uid="second", summary="Second", due=None, priority=2),
    ]
    exit_code, stdout = run_cli(["1", "del"])
    assert exit_code == 0
    assert DummyClient.deleted == ["first"]


def test_list_command_outputs_tasks() -> None:
    exit_code, stdout = run_cli(["list"])
    assert exit_code == 0
    assert "Project" in stdout
    assert "Description" in stdout
    assert "List task" in stdout
    assert "list-task" not in stdout


def test_default_command_is_list() -> None:
    exit_code, stdout = run_cli([])
    assert exit_code == 0
    assert "List task" in stdout


def test_filter_indices_default_to_list_command() -> None:
    DummyClient.list_entries = [
        Task(uid="first", summary="Alpha", due=None, priority=1),
        Task(uid="second", summary="Bravo", due=None, priority=2),
    ]
    exit_code, stdout = run_cli(["2"])
    assert exit_code == 0
    assert "Bravo" in stdout
    assert "Alpha" not in stdout


def test_modify_command_accepts_numeric_identifier() -> None:
    DummyClient.list_entries = [
        Task(uid="first", summary="First", due=None, priority=1),
        Task(uid="second", summary="Second", due=None, priority=2),
    ]
    exit_code, stdout = run_cli(["1", "modify", "summary:Updated"])
    assert exit_code == 0
    assert DummyClient.last_modified_uid == "first"


def test_list_command_shows_uids_when_enabled(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "config.radicale-test.toml"
    config_path.write_text(
        "[caldav]\ncalendar_url = \"https://example.com/cal\"\nusername = \"tester\"\nshow_uids = true\n"
    )

    def fake_load(path: Path) -> CaldavConfig:
        return CaldavConfig(
            calendar_url="https://example.com/cal",
            username="tester",
            show_uids=True,
        )

    monkeypatch.setattr(cli, "load_config_from_path", fake_load)
    monkeypatch.setenv("TODO_CONFIG_FILE", str(config_path))
    exit_code, stdout = run_cli(["list"])
    assert exit_code == 0


def test_list_command_accepts_config_file(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_path = tmp_path / "config.radicale-test.toml"
    config_path.write_text(
        "[caldav]\ncalendar_url = \"https://example.com/cal\"\nusername = \"tester\"\n"
    )
    called: list[Path] = []

    def fake_load(path: Path) -> CaldavConfig:
        called.append(path)
        return CaldavConfig(calendar_url="https://example.com/cal", username="tester")

    monkeypatch.setattr(cli, "load_config_from_path", fake_load)
    monkeypatch.setenv("TODO_CONFIG_FILE", str(config_path))
    exit_code, stdout = run_cli(["list"])
    assert exit_code == 0
    assert called
    assert called[-1] == config_path


def test_config_init_command_writes_file(tmp_path) -> None:
    exit_code, stdout = run_cli(
        [
            "config",
            "init",
            "--env",
            "test",
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
