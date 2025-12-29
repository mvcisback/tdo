from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from todo.caldav_client import CalDAVClient
from todo.config import CaldavConfig
from todo.models import Task, TaskPatch, TaskPayload


CALENDAR_CONFIG = CaldavConfig(calendar_url="https://example.com/calendars/main", username="alice")


@pytest.fixture
def client(tmp_path: Path) -> CalDAVClient:
    return CalDAVClient(CALENDAR_CONFIG, cache_path=tmp_path / "cache.db")


def test_build_ics_includes_priority_due_and_x_props(client: CalDAVClient) -> None:
    due = datetime(2025, 1, 2, 3, 4, 5)
    ics = client._build_ics(
        "Inspect",
        due,
        2,
        {"X-TEST": "value"},
        None,
        "uid-42",
        None,
    )
    assert "SUMMARY:Inspect" in ics
    assert "PRIORITY:2" in ics
    assert "DUE:20250102T030405Z" in ics
    assert "X-TEST:value" in ics
    assert ics.strip().endswith("END:VCALENDAR")


def test_format_due_serializes_to_utc_timestamp(client: CalDAVClient) -> None:
    due = datetime(2025, 6, 1, 0, 0, 0)
    assert client._format_due(due) == "20250601T000000Z"


def test_task_from_data_parses_fields(client: CalDAVClient) -> None:
    body = client._build_ics(
        "Review",
        datetime(2025, 2, 3, 4, 5, 6),
        4,
        {"X-ORG": "dev"},
        ["plan", "review"],
        "task-100",
        None,
    )
    task = client._task_from_data(body)
    assert task.uid == "task-100"
    assert task.summary == "Review"
    assert task.priority == 4
    assert task.x_properties.get("X-ORG") == "dev"
    assert task.due == datetime(2025, 2, 3, 4, 5, 6)
    assert task.categories == ["plan", "review"]


def test_ensure_calendar_raises_when_not_initialized(client: CalDAVClient) -> None:
    with pytest.raises(RuntimeError):
        client._ensure_calendar()


def test_ensure_calendar_returns_calendar_when_initialized(client: CalDAVClient) -> None:
    sentinel = object()
    client.calendar = sentinel
    assert client._ensure_calendar() is sentinel


def test_create_task_persists_in_cache(client: CalDAVClient) -> None:
    payload = TaskPayload(summary="Persist")
    created = client.create_task(payload)
    cached = client.cache.get_task(created.uid)
    assert cached is not None
    assert cached.summary == payload.summary
    assert client.cache.get_pending_action(created.uid) == "create"


def test_modify_task_marks_update_for_remote_task(client: CalDAVClient) -> None:
    base = Task(uid="remote", summary="Remote")
    client.cache.upsert_task(base)
    patch = TaskPatch(summary="Remote v2")
    updated = client.modify_task(base, patch)
    assert updated.summary == "Remote v2"
    assert client.cache.get_pending_action(base.uid) == "update"


def test_delete_unsynced_create_removes_cache_entry(client: CalDAVClient) -> None:
    payload = TaskPayload(summary="Transient")
    created = client.create_task(payload)
    client.delete_task(created.uid)
    assert client.cache.get_task(created.uid) is None


def test_delete_marks_remote_task_as_pending(client: CalDAVClient) -> None:
    existing = Task(uid="remote", summary="Remote")
    client.cache.upsert_task(existing)
    client.delete_task(existing.uid)
    assert client.cache.get_pending_action(existing.uid) == "delete"
