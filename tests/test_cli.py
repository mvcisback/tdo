from __future__ import annotations

from datetime import datetime

import pytest
from typer.testing import CliRunner

from todo import cli
from todo.config import CaldavConfig
from todo.models import Task, TaskPatch, TaskPayload


runner = CliRunner()


class DummyClient:
    last_payload: TaskPayload | None = None
    last_patch: TaskPatch | None = None
    deleted: list[str] = []

    def __init__(self, config: CaldavConfig) -> None:
        self.config = config

    @classmethod
    def reset(cls) -> None:
        cls.last_payload = None
        cls.last_patch = None
        cls.deleted = []

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
        )

    def modify_task(self, uid: str, patch: TaskPatch) -> Task:
        DummyClient.last_patch = patch
        return Task(
            uid=uid,
            summary=patch.summary or uid,
            due=patch.due,
            priority=patch.priority,
            x_properties=patch.x_properties,
        )

    def delete_task(self, uid: str) -> str:
        DummyClient.deleted.append(uid)
        return uid

    def list_tasks(self) -> list[Task]:
        return [
            Task(
                uid="list-task",
                summary="List task",
                due=datetime(2025, 1, 1, 12, 0, 0),
                priority=3,
            )
        ]


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


def test_add_command_parses_tokens() -> None:
    result = runner.invoke(cli.app, ["add", "Create", "pri:H", "x:X-TEST:value"])
    assert result.exit_code == 0
    assert "dummy-task" in result.stdout
    assert DummyClient.last_payload is not None
    assert DummyClient.last_payload.priority == 1
    assert DummyClient.last_payload.x_properties == {"X-TEST": "value"}


def test_modify_command_accepts_summary_patch() -> None:
    result = runner.invoke(cli.app, ["modify", "existing", "summary:Updated", "pri:L"])
    assert result.exit_code == 0
    assert DummyClient.last_patch is not None
    assert DummyClient.last_patch.summary == "Updated"
    assert DummyClient.last_patch.priority == 9


def test_delete_command_accepts_multiple() -> None:
    result = runner.invoke(cli.app, ["del", "one,two"])
    assert result.exit_code == 0
    assert "deleted 2 tasks" in result.stdout
    assert DummyClient.deleted == ["one", "two"]


def test_list_command_outputs_tasks() -> None:
    result = runner.invoke(cli.app, ["list"])
    assert result.exit_code == 0
    assert "list-task" in result.stdout


def test_config_init_command_writes_file(tmp_path) -> None:
    result = runner.invoke(
        cli.app,
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
        ],
    )
    assert result.exit_code == 0
    target = tmp_path / "config.test.toml"
    assert target.exists()
    contents = target.read_text()
    assert "calendar_url = \"https://example.com/declare\"" in contents
    assert "username = \"tester\"" in contents
    assert "password = \"secret\"" in contents
    assert "token = \"tok\"" in contents
