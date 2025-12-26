from __future__ import annotations

from datetime import datetime

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


def test_resource_url_without_duplicate_slashes() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    assert client._resource_url("task-abc") == "https://example.com/calendars/main/task-abc.ics"


def test_trade_report_contains_calendar_query() -> None:
    client = CalDAVClient(CALENDAR_CONFIG)
    report = client._trade_report()
    assert "c:calendar-query" in report
    assert "getetag" in report
