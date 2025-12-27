from __future__ import annotations

from datetime import timedelta

import arrow
import pytest

from todo.time_parser import parse_due_value, parse_wait_value


REFERENCE = arrow.get("2025-05-15T10:30:00")
REFERENCE_VARIANTS = [
    arrow.get("2024-01-01T05:00:00"),
    arrow.get("2025-05-15T10:30:00"),
    arrow.get("2026-12-31T23:59:59"),
]


def test_parse_due_special_tokens() -> None:
    assert parse_due_value("now", REFERENCE) == REFERENCE
    assert parse_due_value("today", REFERENCE) == REFERENCE.floor("day")
    assert parse_due_value("tomorrow", REFERENCE) == REFERENCE.shift(days=1).floor("day")
    assert parse_due_value("yesterday", REFERENCE) == REFERENCE.shift(days=-1).floor("day")
    assert parse_due_value("sod", REFERENCE) == REFERENCE.floor("day")
    assert parse_due_value("eod", REFERENCE) == REFERENCE.floor("day").shift(days=1).shift(seconds=-1)
    assert parse_due_value("later", REFERENCE) == arrow.get("2038-01-18T00:00:00")


def test_parse_due_special_tokens_multiple_references() -> None:
    for reference in REFERENCE_VARIANTS:
        today = reference.floor("day")
        assert parse_due_value("today", reference) == today
        assert parse_due_value("eod", reference) == today.shift(days=1).shift(seconds=-1)
        assert parse_due_value("tomorrow", reference) == reference.shift(days=1).floor("day")
        assert parse_due_value("yesterday", reference) == reference.shift(days=-1).floor("day")
        week_start = reference.shift(days=-reference.weekday()).floor("day")
        previous_week = week_start.shift(weeks=-1)
        assert parse_due_value("sow", reference) == previous_week
        assert parse_due_value("eow", reference) == previous_week.shift(days=7).shift(seconds=-1)
        assert parse_due_value("socw", reference) == week_start
        assert parse_due_value("eocw", reference) == week_start.shift(days=7).shift(seconds=-1)
        assert parse_due_value("soww", reference) == previous_week
        assert parse_due_value("eoww", reference) == previous_week.shift(days=5).shift(seconds=-1)

def test_parse_due_week_boundaries() -> None:
    assert parse_due_value("sow", REFERENCE) == arrow.get("2025-05-05T00:00:00")
    assert parse_due_value("eow", REFERENCE) == arrow.get("2025-05-11T23:59:59")
    assert parse_due_value("socw", REFERENCE) == arrow.get("2025-05-12T00:00:00")
    assert parse_due_value("eocw", REFERENCE) == arrow.get("2025-05-18T23:59:59")
    assert parse_due_value("soww", REFERENCE) == arrow.get("2025-05-05T00:00:00")
    assert parse_due_value("eoww", REFERENCE) == arrow.get("2025-05-09T23:59:59")


def test_parse_due_month_year_quarter_tokens() -> None:
    assert parse_due_value("soy", REFERENCE) == arrow.get("2024-01-01T00:00:00")
    assert parse_due_value("eoy", REFERENCE) == arrow.get("2024-12-31T23:59:59")
    assert parse_due_value("socy", REFERENCE) == arrow.get("2025-01-01T00:00:00")
    assert parse_due_value("eocy", REFERENCE) == arrow.get("2025-12-31T23:59:59")
    assert parse_due_value("som", REFERENCE) == arrow.get("2025-04-01T00:00:00")
    assert parse_due_value("eom", REFERENCE) == arrow.get("2025-04-30T23:59:59")
    assert parse_due_value("socm", REFERENCE) == arrow.get("2025-05-01T00:00:00")
    assert parse_due_value("eocm", REFERENCE) == arrow.get("2025-05-31T23:59:59")
    assert parse_due_value("soq", REFERENCE) == arrow.get("2025-01-01T00:00:00")
    assert parse_due_value("eoq", REFERENCE) == arrow.get("2025-03-31T23:59:59")
    assert parse_due_value("socq", REFERENCE) == arrow.get("2025-04-01T00:00:00")
    assert parse_due_value("eocq", REFERENCE) == arrow.get("2025-06-30T23:59:59")


def test_parse_due_month_names_and_ordinals() -> None:
    assert parse_due_value("march", REFERENCE) == arrow.get("2025-03-01T00:00:00")
    assert parse_due_value("december", REFERENCE) == arrow.get("2024-12-01T00:00:00")
    assert parse_due_value("may", REFERENCE) == arrow.get("2025-05-01T00:00:00")
    assert parse_due_value("7th", REFERENCE) == arrow.get("2025-05-07T00:00:00")
    assert parse_due_value("1st", arrow.get("2025-01-02T12:00:00")) == arrow.get("2025-01-01T00:00:00")


def test_parse_due_physical_datetimes() -> None:
    assert parse_due_value("2025-12-01", REFERENCE) == arrow.get("2025-12-01T00:00:00")
    assert parse_due_value("1700000000", REFERENCE) == arrow.get(1700000000)
    assert parse_due_value("5pm", REFERENCE).hour == 17


def test_parse_wait_value_variants() -> None:
    assert parse_wait_value("2d") == timedelta(days=2)
    assert parse_wait_value("90m") == timedelta(minutes=90)
    assert parse_wait_value("P1DT2H") == timedelta(days=1, hours=2)
    assert parse_wait_value("P1Y2M") == timedelta(days=365 + 60)
    assert parse_wait_value("invalid") is None


@pytest.mark.parametrize(
    "token,expected",
    [
        ("monday", arrow.get("2025-05-12T00:00:00")),
        ("sun", arrow.get("2025-05-11T00:00:00")),
        ("thu", arrow.get("2025-05-08T00:00:00")),
    ],
)
def test_parse_due_weekday_names(token: str, expected: arrow.Arrow) -> None:
    assert parse_due_value(token, REFERENCE) == expected


@pytest.mark.parametrize(
    "token,expected",
    [
        ("march", arrow.get("2025-03-01T00:00:00")),
        ("december", arrow.get("2024-12-01T00:00:00")),
        ("january", arrow.get("2025-01-01T00:00:00")),
        ("february", arrow.get("2025-02-01T00:00:00")),
    ],
)
def test_parse_due_month_names(token: str, expected: arrow.Arrow) -> None:
    assert parse_due_value(token, REFERENCE) == expected


@pytest.mark.parametrize(
    "token,expected",
    [
        ("31st", arrow.get("2025-03-31T00:00:00")),
        ("30th", arrow.get("2025-04-30T00:00:00")),
        ("29th", arrow.get("2025-04-29T00:00:00")),
        ("2nd", arrow.get("2025-05-02T00:00:00")),
    ],
)
def test_parse_due_ordinals_additional(token: str, expected: arrow.Arrow) -> None:
    assert parse_due_value(token, REFERENCE) == expected


@pytest.mark.parametrize(
    "value,expected_hour",
    [
        ("12am", 0),
        ("12pm", 12),
        ("1:30pm", 13),
        ("11:59pm", 23),
        ("0:15", 0),
    ],
)
def test_parse_due_time_of_day(value: str, expected_hour: int) -> None:
    result = parse_due_value(value, REFERENCE)
    assert result is not None
    assert result.hour == expected_hour


def test_parse_due_iso_and_invalid_formats() -> None:
    assert parse_due_value("2025-12-31T23:59:59Z", REFERENCE) == arrow.get("2025-12-31T23:59:59+00:00")
    assert parse_due_value("2025-12-31 23:59", REFERENCE) == arrow.get("2025-12-31T23:59:00")
    assert parse_due_value("not-a-date", REFERENCE) is None


@pytest.mark.parametrize(
    "value,expected",
    [
        ("30s", timedelta(seconds=30)),
        ("1.5h", timedelta(hours=1, minutes=30)),
        ("P1DT2H", timedelta(days=1, hours=2)),
        ("P2.5W", timedelta(days=17, hours=12)),
        ("P1Y2M3DT4H5M6S", timedelta(days=428, hours=4, minutes=5, seconds=6)),
        ("PT90M", timedelta(minutes=90)),
    ],
)
def test_parse_wait_value_various_formats(value: str, expected: timedelta) -> None:
    assert parse_wait_value(value) == expected


def test_parse_wait_value_empty_and_zero() -> None:
    assert parse_wait_value("") is None
    assert parse_wait_value("PT0S") is None


def test_parse_wait_value_rejects_bad_input() -> None:
    assert parse_wait_value("invalid string") is None
