from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from tdo.caldav_client import CalDAVClient
from tdo.config import CaldavConfig
from tdo.models import Task, TaskData, TaskPatch, TaskPayload


CALENDAR_CONFIG = CaldavConfig(calendar_url="https://example.com/calendars/main", username="alice")


@pytest.fixture
async def client(tmp_path: Path) -> CalDAVClient:
    client = await CalDAVClient.create(CALENDAR_CONFIG, cache_path=tmp_path / "cache.db")
    yield client
    await client.close()


def test_build_ics_includes_priority_due_and_x_props() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    due = datetime(2025, 1, 2, 3, 4, 5)
    ics = client._build_ics(
        "Inspect",
        due,
        None,  # wait
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


def test_format_due_serializes_to_utc_timestamp() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    due = datetime(2025, 6, 1, 0, 0, 0)
    assert client._format_due(due) == "20250601T000000Z"


def test_task_from_data_parses_fields() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    body = client._build_ics(
        "Review",
        datetime(2025, 2, 3, 4, 5, 6),
        datetime(2025, 1, 15, 0, 0, 0),  # wait (DTSTART)
        4,
        {"X-ORG": "dev"},
        ["plan", "review"],
        "task-100",
        None,
    )
    task = client._task_from_data(body)
    assert task.uid == "task-100"
    assert task.data.summary == "Review"
    assert task.data.priority == 4
    assert task.data.x_properties.get("X-ORG") == "dev"
    assert task.data.due == datetime(2025, 2, 3, 4, 5, 6)
    assert task.data.wait == datetime(2025, 1, 15, 0, 0, 0)
    assert task.data.categories == ["plan", "review"]


def test_ensure_calendar_raises_when_not_initialized() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    with pytest.raises(RuntimeError):
        client._ensure_calendar()


def test_ensure_calendar_returns_calendar_when_initialized() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    sentinel = object()
    client.calendar = sentinel
    assert client._ensure_calendar() is sentinel


async def test_create_task_persists_in_cache(client: CalDAVClient) -> None:
    payload = TaskPayload(summary="Persist")
    created = await client.create_task(payload)
    cached = await client.cache.get_task(created.uid)
    assert cached is not None
    assert cached.data.summary == payload.summary
    assert await client.cache.get_pending_action(created.uid) == "create"


async def test_modify_task_marks_update_for_remote_task(client: CalDAVClient) -> None:
    base = Task(uid="remote", data=TaskData(summary="Remote"))
    await client.cache.upsert_task(base)
    patch = TaskPatch(summary="Remote v2")
    updated = await client.modify_task(base, patch)
    assert updated.data.summary == "Remote v2"
    assert await client.cache.get_pending_action(base.uid) == "update"


async def test_delete_unsynced_create_removes_cache_entry(client: CalDAVClient) -> None:
    payload = TaskPayload(summary="Transient")
    created = await client.create_task(payload)
    await client.delete_task(created.uid)
    assert await client.cache.get_task(created.uid) is None


async def test_delete_marks_remote_task_as_pending(client: CalDAVClient) -> None:
    existing = Task(uid="remote", data=TaskData(summary="Remote"))
    await client.cache.upsert_task(existing)
    await client.delete_task(existing.uid)
    assert await client.cache.get_pending_action(existing.uid) == "delete"
