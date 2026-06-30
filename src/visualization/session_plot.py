"""Plot one session's 1h bars as a candlestick chart."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import pyarrow.parquet as pq

from src.manifest import ManifestEntry, find_manifest_entries, read_manifest_parquet
from src.storage import resolve_repo_token


@dataclass(frozen=True)
class LoadedBars:
    """Loaded 1h bar rows and their source."""

    rows: list[dict[str, Any]]
    parquet_path: Path
    manifest_entry: ManifestEntry | None


def load_1h_bars_for_session(
    config: Mapping[str, Any],
    session_date: date,
    manifest_path: str | Path | None = None,
    data_tier: str | None = "main",
    symbol: str | None = None,
    contract: str | None = None,
    download_dir: str | Path = "data_local/tmp/hf_downloads",
    env_file: str | Path | None = None,
    input_parquet: str | Path | None = None,
    remote_path_contains: str | None = None,
) -> LoadedBars:
    """Load 1h bar rows for one session date."""
    selected_symbol = symbol or config["dataset"]["symbol"]
    selected_contract = contract or config["dataset"]["default_contract"]

    if input_parquet is not None:
        parquet_path = Path(input_parquet)
        entry = None
    else:
        entry = _select_manifest_entry(
            config=config,
            manifest_path=manifest_path,
            data_tier=data_tier,
            symbol=selected_symbol,
            contract=selected_contract,
            session_date=session_date,
            remote_path_contains=remote_path_contains,
        )
        parquet_path = _resolve_manifest_parquet_path(
            entry=entry,
            config=config,
            download_dir=download_dir,
            env_file=env_file,
        )

    rows = _read_and_filter_bar_rows(
        parquet_path=parquet_path,
        session_date=session_date,
        symbol=selected_symbol,
        contract=selected_contract,
    )

    if not rows:
        raise ValueError(
            "No 1h bar rows found for "
            f"session_date={session_date.isoformat()} "
            f"symbol={selected_symbol} contract={selected_contract}"
        )

    return LoadedBars(rows=rows, parquet_path=parquet_path, manifest_entry=entry)


def build_1h_session_figure(
    rows: list[dict[str, Any]],
    session_date: date,
    title: str | None = None,
) -> object:
    """Build a Plotly candlestick figure with bottom per-candle volume rows."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError as exc:
        raise RuntimeError(
            "plotly is required for plotting. Update the environment from "
            "environment.yml or install plotly in the active environment."
        ) from exc

    sorted_rows = sorted(rows, key=lambda row: row["timestamp_ny"])
    x_values = [_format_time_label(row["timestamp_ny"]) for row in sorted_rows]
    hover_text = [_format_candle_hover(row) for row in sorted_rows]

    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.76, 0.24],
        vertical_spacing=0.03,
        specs=[[{"type": "xy"}], [{"type": "xy"}]],
    )
    figure.add_trace(
        go.Candlestick(
            x=x_values,
            open=[_as_float(row["open"]) for row in sorted_rows],
            high=[_as_float(row["high"]) for row in sorted_rows],
            low=[_as_float(row["low"]) for row in sorted_rows],
            close=[_as_float(row["close"]) for row in sorted_rows],
            increasing={
                "line": {"color": "black", "width": 1},
                "fillcolor": "white",
            },
            decreasing={
                "line": {"color": "black", "width": 1},
                "fillcolor": "black",
            },
            hovertext=hover_text,
            hoverinfo="text",
            name="1h OHLC",
        ),
        row=1,
        col=1,
    )
    _add_bottom_metric_rows(figure, go, sorted_rows, x_values)

    figure.update_layout(
        title=title or f"1h bars - session {session_date.isoformat()}",
        template="plotly_white",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=780,
        showlegend=False,
        hovermode="closest",
        dragmode="pan",
        margin={"l": 48, "r": 28, "t": 70, "b": 28},
    )
    figure.update_xaxes(
        type="category",
        showgrid=False,
        rangeslider={"visible": False},
        showspikes=True,
        spikecolor="#555555",
        spikedash="dot",
        spikemode="across",
        spikesnap="cursor",
        spikethickness=1,
        row=1,
        col=1,
    )
    figure.update_xaxes(
        type="category",
        showgrid=False,
        showticklabels=True,
        showspikes=True,
        spikecolor="#555555",
        spikedash="dot",
        spikemode="across",
        spikesnap="cursor",
        spikethickness=1,
        row=2,
        col=1,
    )
    figure.update_yaxes(
        showgrid=False,
        zeroline=False,
        title_text="Price",
        showspikes=True,
        spikecolor="#555555",
        spikedash="dot",
        spikesnap="cursor",
        spikethickness=1,
        row=1,
        col=1,
    )
    figure.update_yaxes(
        range=[0.5, 4.5],
        fixedrange=True,
        tickmode="array",
        tickvals=[4, 3, 2, 1],
        ticktext=["Vol", "Buy", "Sell", "Delta"],
        showgrid=True,
        gridcolor="#e6e6e6",
        zeroline=False,
        row=2,
        col=1,
    )

    return figure


def write_1h_session_plot(
    loaded_bars: LoadedBars,
    session_date: date,
    output_path: str | Path,
    title: str | None = None,
) -> Path:
    """Write a 1h session Plotly HTML file."""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    figure = build_1h_session_figure(
        rows=loaded_bars.rows,
        session_date=session_date,
        title=title,
    )
    figure.write_html(
        str(output_file),
        include_plotlyjs="cdn",
        config={
            "scrollZoom": True,
            "displaylogo": False,
            "modeBarButtonsToRemove": ["lasso2d", "select2d"],
        },
    )
    return output_file


def _add_bottom_metric_rows(
    figure: object,
    go: object,
    sorted_rows: list[dict[str, Any]],
    x_values: list[str],
) -> None:
    metrics = [
        ("Volume", "volume", 4, "black"),
        ("Buying Volume", "buying_volume", 3, "black"),
        ("Selling Volume", "selling_volume", 2, "black"),
        ("Delta", "delta", 1, None),
    ]

    for name, key, y_value, color in metrics:
        text_values = [_format_int(row[key]) for row in sorted_rows]
        font_color = (
            [_delta_font_color(row[key]) for row in sorted_rows]
            if key == "delta"
            else color
        )
        figure.add_trace(
            go.Scatter(
                x=x_values,
                y=[y_value] * len(x_values),
                mode="text",
                text=text_values,
                textfont={"size": 10, "color": font_color},
                hoverinfo="skip",
                name=name,
            ),
            row=2,
            col=1,
        )


def _select_manifest_entry(
    config: Mapping[str, Any],
    manifest_path: str | Path | None,
    data_tier: str | None,
    symbol: str,
    contract: str,
    session_date: date,
    remote_path_contains: str | None,
) -> ManifestEntry:
    selected_manifest_path = Path(manifest_path or config["paths"]["manifest_path"])

    if not selected_manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {selected_manifest_path}")

    matches = find_manifest_entries(
        entries=read_manifest_parquet(selected_manifest_path),
        data_tier=data_tier,
        dataset_type="bars",
        symbol=symbol,
        contract=contract,
        timeframe="1h",
        session_date=session_date,
    )

    if remote_path_contains is not None:
        matches = [
            entry
            for entry in matches
            if remote_path_contains in entry.remote_path
        ]

    if not matches:
        raise ValueError(
            "No manifest entry found for "
            f"session_date={session_date.isoformat()} "
            f"symbol={symbol} contract={contract} timeframe=1h"
        )

    return sorted(
        matches,
        key=lambda entry: (
            entry.session_date_min == session_date,
            entry.session_date_max == session_date,
            entry.created_at_utc,
            entry.remote_path,
        ),
        reverse=True,
    )[0]


def _resolve_manifest_parquet_path(
    entry: ManifestEntry,
    config: Mapping[str, Any],
    download_dir: str | Path,
    env_file: str | Path | None,
) -> Path:
    local_path = Path(entry.local_path)

    if local_path.exists():
        return local_path

    try:
        from huggingface_hub import hf_hub_download
    except ImportError as exc:
        raise RuntimeError(
            "huggingface_hub is required to download missing manifest files. "
            "Install or update the conda environment from environment.yml."
        ) from exc

    token = resolve_repo_token(
        config=config,
        repo_id=entry.repo_id,
        env_file=env_file,
        required=True,
    )
    return Path(
        hf_hub_download(
            repo_id=entry.repo_id,
            repo_type="dataset",
            filename=entry.remote_path,
            token=token,
            local_dir=str(download_dir),
        )
    )


def _read_and_filter_bar_rows(
    parquet_path: Path,
    session_date: date,
    symbol: str,
    contract: str,
) -> list[dict[str, Any]]:
    table = pq.read_table(parquet_path)
    rows = [
        row
        for row in table.to_pylist()
        if row.get("session_date") == session_date
        and row.get("symbol") == symbol
        and row.get("contract") == contract
    ]
    return sorted(rows, key=lambda row: row["timestamp_ny"])


def _format_candle_hover(row: Mapping[str, Any]) -> str:
    return (
        f"<b>{_format_time_label(row['timestamp_ny'])}</b><br>"
        f"Open: {_format_price(row['open'])}<br>"
        f"High: {_format_price(row['high'])}<br>"
        f"Low: {_format_price(row['low'])}<br>"
        f"Close: {_format_price(row['close'])}<br>"
        f"Volume: {_format_int(row['volume'])}<br>"
        f"Buying Volume: {_format_int(row['buying_volume'])}<br>"
        f"Selling Volume: {_format_int(row['selling_volume'])}<br>"
        f"Delta: {_format_int(row['delta'])}"
    )


def _format_time_label(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M")


def _format_price(value: Decimal | float | int | None) -> str:
    if value is None:
        return ""

    return f"{_as_float(value):.2f}"


def _format_int(value: object) -> str:
    return f"{int(value):,}"


def _as_float(value: Decimal | float | int) -> float:
    return float(value)


def _delta_font_color(value: object) -> str:
    return "black" if int(value) >= 0 else "#b00020"
