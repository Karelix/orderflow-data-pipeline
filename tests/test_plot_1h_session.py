from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from src.config import load_config
from src.ingest.write_parquet import BAR_SCHEMA
from src.manifest import build_manifest_for_parquet_tree, write_manifest_parquet
from src.visualization import (
    build_1h_session_figure,
    load_1h_bars_for_session,
    write_1h_session_plot,
)


def test_load_and_plot_1h_session_from_manifest(tmp_path) -> None:
    config = load_config()
    manifest_path = tmp_path / "metadata" / "manifest.parquet"
    root = tmp_path / "derived"
    parquet_path = (
        root
        / "bars"
        / "timeframe=1h"
        / "year=2026"
        / "month=05"
        / "session=2026-05-25"
        / "part.parquet"
    )
    parquet_path.parent.mkdir(parents=True)
    ny = ZoneInfo("America/New_York")
    rows = [
        _bar_row(timestamp=datetime(2026, 5, 25, 10, 0, tzinfo=ny), close="100.50"),
        _bar_row(timestamp=datetime(2026, 5, 25, 9, 0, tzinfo=ny), close="99.50"),
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=BAR_SCHEMA), parquet_path)
    manifest_entries = build_manifest_for_parquet_tree(
        root=root,
        config=config,
        repo_id="user/orderflow-es-001",
        repo_sequence=1,
        remote_prefix="main/ESU26-CME",
    )
    write_manifest_parquet(manifest_entries, manifest_path)

    loaded = load_1h_bars_for_session(
        config=config,
        session_date=date(2026, 5, 25),
        manifest_path=manifest_path,
        data_tier="main",
    )
    figure = build_1h_session_figure(
        rows=loaded.rows,
        session_date=date(2026, 5, 25),
    )
    output_path = write_1h_session_plot(
        loaded_bars=loaded,
        session_date=date(2026, 5, 25),
        output_path=tmp_path / "plot.html",
    )

    assert loaded.parquet_path == parquet_path
    assert [row["timestamp_ny"].hour for row in loaded.rows] == [9, 10]
    assert loaded.manifest_entry is not None
    assert loaded.manifest_entry.remote_path.endswith(
        "bars/timeframe=1h/year=2026/month=05/session=2026-05-25/part.parquet"
    )
    assert len(figure.data) == 5
    assert figure.data[0].increasing.fillcolor == "white"
    assert figure.data[0].decreasing.fillcolor == "black"
    assert [trace.name for trace in figure.data[1:]] == [
        "Volume",
        "Buying Volume",
        "Selling Volume",
        "Delta",
    ]
    assert list(figure.data[1].text) == ["1,000", "1,000"]
    assert list(figure.layout.yaxis2.ticktext) == ["Vol", "Buy", "Sell", "Delta"]
    assert figure.layout.dragmode == "pan"
    assert figure.layout.xaxis.showspikes is True
    assert output_path.exists()
    assert '"scrollZoom": true' in output_path.read_text(encoding="utf-8")


def _bar_row(timestamp: datetime, close: str) -> dict:
    return {
        "symbol": "ES",
        "contract": "ESU26-CME",
        "timestamp_utc": timestamp.astimezone(ZoneInfo("UTC")),
        "timestamp_ny": timestamp,
        "session_date": date(2026, 5, 25),
        "session_type": "rth",
        "open": Decimal("100.00"),
        "high": Decimal("101.00"),
        "low": Decimal("99.00"),
        "close": Decimal(close),
        "volume": 1000,
        "buying_volume": 600,
        "selling_volume": 400,
        "delta": 200,
        "cumulative_delta": 200,
        "number_of_trades": 25,
        "vwap": Decimal("100.250000"),
    }
