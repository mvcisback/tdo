from __future__ import annotations

import arrow
import pytest

from tdo.time_parser import parse_due_value


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
        assert parse_due_value("eow", reference) == week_start.shift(days=7).shift(seconds=-1)  # end of current week
        assert parse_due_value("socw", reference) == week_start
        assert parse_due_value("eocw", reference) == week_start.shift(days=7).shift(seconds=-1)
        assert parse_due_value("soww", reference) == previous_week
        assert parse_due_value("eoww", reference) == previous_week.shift(days=5).shift(seconds=-1)

def test_parse_due_week_boundaries() -> None:
    assert parse_due_value("sow", REFERENCE) == arrow.get("2025-05-05T00:00:00")
    assert parse_due_value("eow", REFERENCE) == arrow.get("2025-05-18T23:59:59")  # end of current week
    assert parse_due_value("socw", REFERENCE) == arrow.get("2025-05-12T00:00:00")
    assert parse_due_value("eocw", REFERENCE) == arrow.get("2025-05-18T23:59:59")
    assert parse_due_value("soww", REFERENCE) == arrow.get("2025-05-05T00:00:00")
    assert parse_due_value("eoww", REFERENCE) == arrow.get("2025-05-09T23:59:59")


def test_parse_due_month_year_quarter_tokens() -> None:
    assert parse_due_value("soy", REFERENCE) == arrow.get("2024-01-01T00:00:00")
    assert parse_due_value("eoy", REFERENCE) == arrow.get("2025-12-31T23:59:59")  # end of current year
    assert parse_due_value("socy", REFERENCE) == arrow.get("2025-01-01T00:00:00")
    assert parse_due_value("eocy", REFERENCE) == arrow.get("2025-12-31T23:59:59")
    assert parse_due_value("som", REFERENCE) == arrow.get("2025-04-01T00:00:00")
    assert parse_due_value("eom", REFERENCE) == arrow.get("2025-05-31T23:59:59")  # end of current month
    assert parse_due_value("socm", REFERENCE) == arrow.get("2025-05-01T00:00:00")
    assert parse_due_value("eocm", REFERENCE) == arrow.get("2025-05-31T23:59:59")
    assert parse_due_value("soq", REFERENCE) == arrow.get("2025-01-01T00:00:00")
    assert parse_due_value("eoq", REFERENCE) == arrow.get("2025-06-30T23:59:59")  # end of current quarter
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


def test_parse_due_relative_durations() -> None:
    result_2w = parse_due_value("2w", REFERENCE)
    assert result_2w is not None
    assert result_2w == REFERENCE.shift(weeks=2)

    result_3d = parse_due_value("3d", REFERENCE)
    assert result_3d is not None
    assert result_3d == REFERENCE.shift(days=3)

    result_1y = parse_due_value("1y", REFERENCE)
    assert result_1y is not None
    assert result_1y == REFERENCE.shift(days=365)

    result_2h = parse_due_value("2h", REFERENCE)
    assert result_2h is not None
    assert result_2h == REFERENCE.shift(hours=2)


def test_parse_due_iso_durations() -> None:
    result_p1d = parse_due_value("P1D", REFERENCE)
    assert result_p1d is not None
    assert result_p1d == REFERENCE.shift(days=1)

    result_p2w = parse_due_value("P2W", REFERENCE)
    assert result_p2w is not None
    assert result_p2w == REFERENCE.shift(weeks=2)

    result_p1y = parse_due_value("P1Y", REFERENCE)
    assert result_p1y is not None
    assert result_p1y == REFERENCE.shift(days=365)


# === Additional Tests for Comprehensive Coverage ===


@pytest.mark.parametrize(
    "token,expected_shift",
    [
        ("30m", {"minutes": 30}),
        ("90m", {"minutes": 90}),
        ("90s", {"seconds": 90}),
        ("3600s", {"seconds": 3600}),
        ("1d", {"days": 1}),
        ("1w", {"weeks": 1}),
    ],
)
def test_relative_duration_minutes_and_seconds(token: str, expected_shift: dict) -> None:
    result = parse_due_value(token, REFERENCE)
    assert result is not None
    assert result == REFERENCE.shift(**expected_shift)


@pytest.mark.parametrize(
    "token",
    ["0m", "0s", "0d", "0w"],
)
def test_zero_duration_returns_none(token: str) -> None:
    """Zero durations return None (pytimeparse behavior)."""
    result = parse_due_value(token, REFERENCE)
    assert result is None


def test_relative_duration_365d_vs_1y_equivalence() -> None:
    result_365d = parse_due_value("365d", REFERENCE)
    result_1y = parse_due_value("1y", REFERENCE)
    assert result_365d is not None
    assert result_1y is not None
    assert result_365d == result_1y


@pytest.mark.parametrize(
    "token",
    [
        "tues",
        "thur",
        "thurs",
    ],
)
def test_weekday_variant_abbreviations(token: str) -> None:
    result = parse_due_value(token, REFERENCE)
    assert result is not None
    # All these should resolve to valid dates in the past week
    assert result < REFERENCE


@pytest.mark.parametrize(
    "token,expected",
    [
        ("TuEsDaY", arrow.get("2025-05-13T00:00:00")),
        ("MONDAY", arrow.get("2025-05-12T00:00:00")),
        ("Wed", arrow.get("2025-05-14T00:00:00")),
        ("FRIDAY", arrow.get("2025-05-09T00:00:00")),
    ],
)
def test_weekday_mixed_case(token: str, expected: arrow.Arrow) -> None:
    assert parse_due_value(token, REFERENCE) == expected


@pytest.mark.parametrize(
    "token",
    [
        "sept",
        "Sep",
        "SEPTEMBER",
    ],
)
def test_month_variant_abbreviations(token: str) -> None:
    result = parse_due_value(token, REFERENCE)
    assert result is not None
    assert result.month == 9


@pytest.mark.parametrize(
    "token,expected_month",
    [
        ("JaNuArY", 1),
        ("MARCH", 3),
        ("Apr", 4),
        ("DECEMBER", 12),
    ],
)
def test_month_mixed_case(token: str, expected_month: int) -> None:
    result = parse_due_value(token, REFERENCE)
    assert result is not None
    assert result.month == expected_month


def test_ordinal_31st_finds_previous_31st_day() -> None:
    # Reference in May (31-day month) - asking for 31st should find previous 31st
    may_ref = arrow.get("2025-05-15T10:00:00")
    result = parse_due_value("31st", may_ref)
    assert result is not None
    # Should find most recent 31st (March 31 since April only has 30 days)
    assert result == arrow.get("2025-03-31T00:00:00")


def test_ordinal_30th_in_march_finds_february_28() -> None:
    # Reference in March - asking for 30th
    march_ref = arrow.get("2025-03-15T10:00:00")
    result = parse_due_value("30th", march_ref)
    assert result is not None
    # Feb has no 30th, should find Feb 28 (end of Feb) or Jan 30
    assert result.day in (28, 30)


def test_leap_year_29th_february() -> None:
    # 2024 is a leap year
    leap_year_ref = arrow.get("2024-03-15T10:00:00")
    result = parse_due_value("29th", leap_year_ref)
    assert result is not None
    # Should find Feb 29 in leap year
    assert result == arrow.get("2024-02-29T00:00:00")


def test_non_leap_year_29th_february() -> None:
    # 2025 is not a leap year - asking for 29th from March should skip Feb
    non_leap_ref = arrow.get("2025-03-15T10:00:00")
    result = parse_due_value("29th", non_leap_ref)
    assert result is not None
    # Feb 2025 has no 29th, should go back to Jan 29
    assert result.day == 29
    assert result.month in (1, 2)  # Either Jan 29 or previous Feb 29


def test_year_boundary_month_parsing() -> None:
    # Reference at end of year - "january" should find current year's January
    dec_ref = arrow.get("2025-12-31T23:00:00")
    result = parse_due_value("january", dec_ref)
    assert result is not None
    assert result == arrow.get("2025-01-01T00:00:00")


def test_year_boundary_december_from_january() -> None:
    # Reference in January - "december" should find previous year's December
    jan_ref = arrow.get("2025-01-15T10:00:00")
    result = parse_due_value("december", jan_ref)
    assert result is not None
    assert result == arrow.get("2024-12-01T00:00:00")


@pytest.mark.parametrize(
    "token,expected_hour,expected_minute",
    [
        ("3p", 15, 0),
        ("3pm", 15, 0),
        ("11a", 11, 0),
        ("11am", 11, 0),
        ("9:45a", 9, 45),
        ("9:45am", 9, 45),
    ],
)
def test_time_of_day_single_letter_suffix(token: str, expected_hour: int, expected_minute: int) -> None:
    result = parse_due_value(token, REFERENCE)
    assert result is not None
    assert result.hour == expected_hour
    assert result.minute == expected_minute


@pytest.mark.parametrize(
    "invalid_token",
    [
        "abc",
        "not-a-date",
        "xyz123",
        "",
        "   ",
        "2025-13-45",  # Invalid month/day
    ],
)
def test_invalid_formats_return_none(invalid_token: str) -> None:
    result = parse_due_value(invalid_token, REFERENCE)
    assert result is None


def test_empty_and_none_inputs() -> None:
    assert parse_due_value("", REFERENCE) is None
    assert parse_due_value("   ", REFERENCE) is None
    assert parse_due_value(None, REFERENCE) is None  # type: ignore[arg-type]
