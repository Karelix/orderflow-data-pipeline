"""Assign ES timestamps to New York trading sessions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo


CLOSED_SESSION_TYPE = "closed"


@dataclass(frozen=True)
class SessionConfig:
    """Trading-session settings loaded from the project config."""

    source_timezone: ZoneInfo
    session_timezone: ZoneInfo
    globex_start: time
    rth_start: time
    rth_end: time
    session_end: time

    @classmethod
    def from_project_config(cls, config: Mapping[str, Any]) -> "SessionConfig":
        """Create session settings from config/dataset.yaml contents."""
        sessions = config["sessions"]
        timezones = config["timezones"]

        return cls(
            source_timezone=ZoneInfo(timezones["source"]),
            session_timezone=ZoneInfo(sessions["timezone"]),
            globex_start=_parse_time(sessions["globex_start"]),
            rth_start=_parse_time(sessions["rth_start"]),
            rth_end=_parse_time(sessions["rth_end"]),
            session_end=_parse_time(sessions["session_end"]),
        )


@dataclass(frozen=True)
class SessionInfo:
    """Session classification for one timestamp."""

    timestamp_utc: datetime
    timestamp_ny: datetime
    session_date: date | None
    session_type: str

    @property
    def is_trading_session(self) -> bool:
        """Return true when the timestamp belongs to a tradeable session window."""
        return self.session_date is not None


def classify_timestamp(timestamp: datetime, config: SessionConfig) -> SessionInfo:
    """Classify a source timestamp into the configured ES trading session."""
    source_timestamp = _ensure_timezone(timestamp, config.source_timezone)
    timestamp_utc = source_timestamp.astimezone(timezone.utc)
    timestamp_ny = timestamp_utc.astimezone(config.session_timezone)

    local_date = timestamp_ny.date()
    local_time = timestamp_ny.time()

    if local_time >= config.globex_start:
        return SessionInfo(
            timestamp_utc=timestamp_utc,
            timestamp_ny=timestamp_ny,
            session_date=local_date + timedelta(days=1),
            session_type="globex",
        )

    if local_time < config.rth_start:
        return SessionInfo(
            timestamp_utc=timestamp_utc,
            timestamp_ny=timestamp_ny,
            session_date=local_date,
            session_type="globex",
        )

    if local_time < config.rth_end:
        return SessionInfo(
            timestamp_utc=timestamp_utc,
            timestamp_ny=timestamp_ny,
            session_date=local_date,
            session_type="rth",
        )

    if local_time < config.session_end:
        return SessionInfo(
            timestamp_utc=timestamp_utc,
            timestamp_ny=timestamp_ny,
            session_date=local_date,
            session_type="post_rth",
        )

    return SessionInfo(
        timestamp_utc=timestamp_utc,
        timestamp_ny=timestamp_ny,
        session_date=None,
        session_type=CLOSED_SESSION_TYPE,
    )


def _parse_time(value: str) -> time:
    return time.fromisoformat(value)


def _ensure_timezone(timestamp: datetime, default_timezone: ZoneInfo) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=default_timezone)

    return timestamp
