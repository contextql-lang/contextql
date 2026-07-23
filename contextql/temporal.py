"""UTC temporal normalization shared by refresh and query execution."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


def floor_utc(value: datetime, granularity: str | None) -> datetime:
    """Normalize an aware timestamp to UTC and floor to its CQL bucket."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(
            "Temporal timestamps must include an explicit timezone."
        )
    value = value.astimezone(timezone.utc)
    unit = (granularity or "SECOND").upper()
    if unit == "SECOND":
        return value.replace(microsecond=0)
    if unit == "MINUTE":
        return value.replace(second=0, microsecond=0)
    if unit == "HOUR":
        return value.replace(minute=0, second=0, microsecond=0)
    if unit == "DAY":
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    if unit == "WEEK":
        day = value.replace(hour=0, minute=0, second=0, microsecond=0)
        return day - timedelta(days=day.weekday())
    if unit == "MONTH":
        return value.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
    if unit == "QUARTER":
        month = ((value.month - 1) // 3) * 3 + 1
        return value.replace(
            month=month,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    if unit == "YEAR":
        return value.replace(
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    raise ValueError(f"Unsupported temporal granularity {granularity!r}.")
