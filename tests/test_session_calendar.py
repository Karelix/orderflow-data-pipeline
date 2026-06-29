from datetime import date, datetime, timezone

from src.config import load_config
from src.sessions import CLOSED_SESSION_TYPE, SessionConfig, classify_timestamp


def session_config() -> SessionConfig:
    return SessionConfig.from_project_config(load_config())


def test_globex_start_uses_next_rth_session_date() -> None:
    info = classify_timestamp(
        datetime(2026, 5, 24, 22, 0, 0, tzinfo=timezone.utc),
        session_config(),
    )

    assert info.timestamp_ny.isoformat() == "2026-05-24T18:00:00-04:00"
    assert info.session_date == date(2026, 5, 25)
    assert info.session_type == "globex"


def test_last_second_before_rth_is_globex() -> None:
    info = classify_timestamp(
        datetime(2026, 5, 25, 13, 29, 59, tzinfo=timezone.utc),
        session_config(),
    )

    assert info.timestamp_ny.isoformat() == "2026-05-25T09:29:59-04:00"
    assert info.session_date == date(2026, 5, 25)
    assert info.session_type == "globex"


def test_rth_start_is_rth() -> None:
    info = classify_timestamp(
        datetime(2026, 5, 25, 13, 30, 0, tzinfo=timezone.utc),
        session_config(),
    )

    assert info.timestamp_ny.isoformat() == "2026-05-25T09:30:00-04:00"
    assert info.session_date == date(2026, 5, 25)
    assert info.session_type == "rth"


def test_rth_end_is_post_rth() -> None:
    info = classify_timestamp(
        datetime(2026, 5, 25, 20, 15, 0, tzinfo=timezone.utc),
        session_config(),
    )

    assert info.timestamp_ny.isoformat() == "2026-05-25T16:15:00-04:00"
    assert info.session_date == date(2026, 5, 25)
    assert info.session_type == "post_rth"


def test_session_end_is_closed() -> None:
    info = classify_timestamp(
        datetime(2026, 5, 25, 21, 0, 0, tzinfo=timezone.utc),
        session_config(),
    )

    assert info.timestamp_ny.isoformat() == "2026-05-25T17:00:00-04:00"
    assert info.session_date is None
    assert info.session_type == CLOSED_SESSION_TYPE
    assert not info.is_trading_session


def test_winter_dst_offset_is_handled() -> None:
    info = classify_timestamp(
        datetime(2026, 1, 4, 23, 0, 0, tzinfo=timezone.utc),
        session_config(),
    )

    assert info.timestamp_ny.isoformat() == "2026-01-04T18:00:00-05:00"
    assert info.session_date == date(2026, 1, 5)
    assert info.session_type == "globex"
