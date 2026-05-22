from __future__ import annotations

from datetime import datetime, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_timestamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()
