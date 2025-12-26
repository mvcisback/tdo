from __future__ import annotations

from datetime import datetime

import pytest

from todo.caldav_client import CalDAVClient
from todo.config import CaldavConfig


CALENDAR_CONFIG = CaldavConfig(calendar_url="https://example.com/calendars/main", username="alice")


def test_build_ics_includes_priority_due_and_x_props() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    due = datetime(2025, 1, 2, 3, 4, 5)
    ics = client._build_ics(
        "Inspect",
        due,
        2,
        {"X-TEST": "value"},
        "uid-42",
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
        4,
        {"X-ORG": "dev"},
        "task-100",
    )
    task = client._task_from_data(body)
    assert task.uid == "task-100"
    assert task.summary == "Review"
    assert task.priority == 4
    assert task.x_properties.get("X-ORG") == "dev"
    assert task.due == datetime(2025, 2, 3, 4, 5, 6)


def test_ensure_calendar_raises_when_not_initialized() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    with pytest.raises(RuntimeError):
        client._ensure_calendar()


def test_ensure_calendar_returns_calendar_when_initialized() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    sentinel = object()
    client.calendar = sentinel
    assert client._ensure_calendar() is sentinel
