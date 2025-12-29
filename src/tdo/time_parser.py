from __future__ import annotations

import re
from datetime import timedelta
from typing import Callable

import arrow
from arrow.parser import ParserError
from pytimeparse import parse as parse_duration

__all__ = ["parse_due_value", "parse_wait_value"]

_LATER = arrow.get("2038-01-18T00:00:00")
_WEEKDAY_MAP = {
    "monday": 0,
    "mon": 0,
    "tuesday": 1,
    "tue": 1,
    "tues": 1,
    "wednesday": 2,
    "wed": 2,
    "thursday": 3,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "friday": 4,
    "fri": 4,
    "saturday": 5,
    "sat": 5,
    "sunday": 6,
    "sun": 6,
}
_MONTH_MAP = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}
_ORDINAL_RE = re.compile(r"^(\d+)(?:st|nd|rd|th)$", re.IGNORECASE)
_TIME_OF_DAY_RE = re.compile(
    r"^(?P<h>\d{1,2})(?::(?P<m>\d{1,2}))?(?::(?P<s>\d{1,2}))?\s*(?P<suffix>a|am|p|pm)?$",
    re.IGNORECASE,
)


def parse_due_value(raw: str, reference: arrow.Arrow | None = None) -> arrow.Arrow | None:
    candidate = (raw or "").strip()
    if not candidate:
        return None
    now = reference or arrow.now()
    lowered = candidate.lower()
    if lowered in _SPECIAL_DUE_MAPPINGS:
        return _SPECIAL_DUE_MAPPINGS[lowered](now)
    if lowered in _WEEKDAY_MAP:
        return _previous_weekday(now, _WEEKDAY_MAP[lowered])
    if match := _ORDINAL_RE.fullmatch(lowered):
        return _previous_ordinal_day(now, int(match.group(1)))
    if lowered in _MONTH_MAP:
        return _previous_month_start(now, _MONTH_MAP[lowered])
    if lowered.isdigit():
        return arrow.get(int(lowered))
    try:
        return arrow.get(candidate)
    except (ParserError, ValueError):
        if parsed := _parse_time_of_day(candidate, now):
            return parsed
        return None


def parse_wait_value(raw: str) -> timedelta | None:
    candidate = (raw or "").strip()
    if not candidate:
        return None
    upper = candidate.upper()
    if upper.startswith("P"):
        duration = _parse_iso_duration(upper)
        if duration is not None:
            return duration
    seconds = parse_duration(candidate)
    if seconds is None:
        return None
    return timedelta(seconds=seconds)


def _start_of_day(value: arrow.Arrow) -> arrow.Arrow:
    return value.floor("day")


def _end_of_day(value: arrow.Arrow) -> arrow.Arrow:
    return value.floor("day").shift(days=1).shift(seconds=-1)


def _start_of_week(value: arrow.Arrow) -> arrow.Arrow:
    return value.shift(days=-value.weekday()).floor("day")


def _week_end(start: arrow.Arrow) -> arrow.Arrow:
    return start.shift(days=7).shift(seconds=-1)


def _work_week_end(start: arrow.Arrow) -> arrow.Arrow:
    return start.shift(days=5).shift(seconds=-1)


def _start_of_month(value: arrow.Arrow) -> arrow.Arrow:
    return value.floor("month")


def _previous_month_start(value: arrow.Arrow, target_month: int) -> arrow.Arrow:
    candidate_year = value.year
    candidate = arrow.get(year=candidate_year, month=target_month, day=1)
    if candidate >= value:
        candidate = candidate.shift(years=-1)
    return candidate.floor("day")


def _previous_ordinal_day(value: arrow.Arrow, ordinal: int) -> arrow.Arrow:
    if ordinal < 1 or ordinal > 31:
        return value.floor("day")
    candidate = arrow.get(year=value.year, month=value.month, day=ordinal)
    if candidate >= value:
        candidate = candidate.shift(months=-1)
    while True:
        try:
            candidate = arrow.get(year=candidate.year, month=candidate.month, day=ordinal)
            break
        except (ValueError, ParserError):
            candidate = candidate.shift(months=-1)
    return candidate.floor("day")


def _previous_weekday(value: arrow.Arrow, target: int) -> arrow.Arrow:
    delta = (value.weekday() - target) % 7
    if delta == 0:
        delta = 7
    return value.shift(days=-delta).floor("day")


def _start_of_previous_year(value: arrow.Arrow) -> arrow.Arrow:
    return value.shift(years=-1).floor("year")


def _end_of_previous_year(value: arrow.Arrow) -> arrow.Arrow:
    return _start_of_previous_year(value).shift(years=1).shift(seconds=-1)


def _start_of_current_year(value: arrow.Arrow) -> arrow.Arrow:
    return value.floor("year")


def _end_of_current_year(value: arrow.Arrow) -> arrow.Arrow:
    return _start_of_current_year(value).shift(years=1).shift(seconds=-1)


def _quarter_start_by_offset(value: arrow.Arrow, offset: int) -> arrow.Arrow:
    current_index = (value.month - 1) // 3
    target_index = current_index + offset
    year_offset, target_index = divmod(target_index, 4)
    target_year = value.year + year_offset
    start_month = target_index * 3 + 1
    return arrow.get(year=target_year, month=start_month, day=1).floor("day")


def _start_of_current_quarter(value: arrow.Arrow) -> arrow.Arrow:
    return _quarter_start_by_offset(value, 0)


def _start_of_previous_quarter(value: arrow.Arrow) -> arrow.Arrow:
    return _quarter_start_by_offset(value, -1)


def _end_of_quarter(start: arrow.Arrow) -> arrow.Arrow:
    return start.shift(months=3).shift(seconds=-1)


def _previous_work_week_start(value: arrow.Arrow) -> arrow.Arrow:
    return _start_of_week(value).shift(weeks=-1)


def _static_work_week_end(value: arrow.Arrow) -> arrow.Arrow:
    return _work_week_end(_previous_work_week_start(value))


def _parse_time_of_day(value: str, reference: arrow.Arrow) -> arrow.Arrow | None:
    match = _TIME_OF_DAY_RE.fullmatch(value)
    if not match:
        return None
    hour = int(match.group("h"))
    minute = int(match.group("m") or 0)
    second = int(match.group("s") or 0)
    suffix = match.group("suffix")
    if suffix:
        normalized = suffix.lower()
        if normalized in {"p", "pm"} and hour < 12:
            hour += 12
        if normalized in {"a", "am"} and hour == 12:
            hour = 0
    return reference.floor("day").shift(hours=hour, minutes=minute, seconds=second)

_SPECIAL_DUE_MAPPINGS: dict[str, Callable[[arrow.Arrow], arrow.Arrow]] = {
    "now": lambda now: now,
    "today": lambda now: _start_of_day(now),
    "sod": lambda now: _start_of_day(now),
    "eod": lambda now: _end_of_day(now),
    "tomorrow": lambda now: _start_of_day(now.shift(days=1)),
    "yesterday": lambda now: _start_of_day(now.shift(days=-1)),
    "later": lambda _: _LATER,
    "someday": lambda _: _LATER,
    "soy": lambda now: _start_of_previous_year(now),
    "eoy": lambda now: _end_of_previous_year(now),
    "socy": lambda now: _start_of_current_year(now),
    "eocy": lambda now: _end_of_current_year(now),
    "soq": lambda now: _start_of_previous_quarter(now),
    "eoq": lambda now: _end_of_quarter(_start_of_previous_quarter(now)),
    "socq": lambda now: _start_of_current_quarter(now),
    "eocq": lambda now: _end_of_quarter(_start_of_current_quarter(now)),
    "som": lambda now: _start_of_month(now).shift(months=-1),
    "eom": lambda now: _start_of_month(now).shift(seconds=-1),
    "socm": lambda now: _start_of_month(now),
    "eocm": lambda now: _start_of_month(now).shift(months=1).shift(seconds=-1),
    "sow": lambda now: _start_of_week(now).shift(weeks=-1),
    "eow": lambda now: _week_end(_start_of_week(now).shift(weeks=-1)),
    "socw": lambda now: _start_of_week(now),
    "eocw": lambda now: _week_end(_start_of_week(now)),
    "soww": lambda now: _previous_work_week_start(now),
    "eoww": lambda now: _static_work_week_end(now),
}


def _parse_iso_duration(iso: str) -> timedelta | None:
    if not iso.startswith("P"):
        return None
    date_part, _, time_part = iso[1:].partition("T")
    total_seconds = 0.0
    total_days = 0.0
    for value, unit in re.findall(r"([0-9]+(?:\.[0-9]+)?)([YMWD])", date_part):
        amount = float(value)
        if unit == "Y":
            total_days += amount * 365
        elif unit == "M":
            total_days += amount * 30
        elif unit == "W":
            total_days += amount * 7
        elif unit == "D":
            total_days += amount
    for value, unit in re.findall(r"([0-9]+(?:\.[0-9]+)?)([HMS])", time_part):
        amount = float(value)
        if unit == "H":
            total_seconds += amount * 3600
        elif unit == "M":
            total_seconds += amount * 60
        elif unit == "S":
            total_seconds += amount
    if total_days == 0 and total_seconds == 0:
        return None
    return timedelta(days=total_days, seconds=total_seconds)

