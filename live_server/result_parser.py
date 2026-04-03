"""
result_parser.py - parse backtest result directories for the web UI.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional


_METRIC_ALIASES = {
    "total_returns": ("total_returns", "策略收益"),
    "benchmark_returns": ("benchmark_returns", "基准收益"),
    "sharpe": ("sharpe", "夏普比率"),
    "max_drawdown": ("max_drawdown", "最大回撤"),
    "annualized_returns": ("annualized_returns", "策略年化收益"),
    "volatility": ("volatility", "策略波动率"),
}


def _read_csv_rows(path: Path) -> list:
    if not path.exists():
        return []

    import csv

    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _normalize_metrics(raw_metrics: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for target_key, aliases in _METRIC_ALIASES.items():
        for alias in aliases:
            if alias not in raw_metrics:
                continue
            value = raw_metrics[alias]
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                normalized[target_key] = value
            else:
                normalized[target_key] = numeric if target_key == "sharpe" else numeric / 100.0
            break
    return normalized


def _build_daily_chart(rows: list) -> Dict[str, Any]:
    chart = {
        "dates": [],
        "portfolio_value": [],
        "returns": [],
        "benchmark_returns": [],
    }
    first_total_value = None

    def _to_float(row: Dict[str, Any], *keys: str) -> float:
        for key in keys:
            value = row.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 0.0

    def _pick_value(row: Dict[str, Any], *keys: str) -> tuple[float, Optional[str]]:
        for key in keys:
            value = row.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value), key
            except (TypeError, ValueError):
                continue
        return 0.0, None

    for row in rows:
        date_str = row.get("date", row.get("datetime", ""))
        total_value = _to_float(row, "total_value", "portfolio_value", "net_value")
        if first_total_value is None and total_value:
            first_total_value = total_value

        returns_value, returns_key = _pick_value(row, "returns_pct", "returns", "cumulative_returns")
        if returns_key == "returns_pct":
            cumulative_return = returns_value / 100.0
        elif returns_key in ("returns", "cumulative_returns"):
            cumulative_return = returns_value / 100.0 if (returns_value > 1 or returns_value < -1) else returns_value
        elif first_total_value:
            cumulative_return = total_value / first_total_value - 1.0
        else:
            cumulative_return = 0.0

        benchmark_return, benchmark_key = _pick_value(
            row,
            "benchmark_returns",
            "benchmark_return",
            "benchmark_returns_pct",
            "benchmark_return_pct",
            "benchmark_cumulative_returns",
            "bench_returns",
        )
        if benchmark_key in ("benchmark_returns", "benchmark_return", "benchmark_returns_pct", "benchmark_return_pct"):
            benchmark_return /= 100.0
        elif benchmark_key in ("benchmark_cumulative_returns", "bench_returns") and (
            benchmark_return > 1 or benchmark_return < -1
        ):
            benchmark_return /= 100.0

        chart["dates"].append(date_str)
        chart["portfolio_value"].append(total_value)
        chart["returns"].append(cumulative_return)
        chart["benchmark_returns"].append(benchmark_return)
    return chart


def parse_result(output_dir: str) -> Dict[str, Any]:
    base = Path(output_dir)

    metrics: Dict[str, Any] = {}
    metrics_path = base / "metrics.json"
    if metrics_path.exists():
        try:
            metrics_doc = json.loads(metrics_path.read_text(encoding="utf-8"))
            raw_metrics = metrics_doc.get("metrics", metrics_doc)
            if isinstance(raw_metrics, dict):
                metrics = _normalize_metrics(raw_metrics)
        except Exception:
            pass

    daily_rows = _read_csv_rows(base / "daily_records.csv")
    trades = _read_csv_rows(base / "trades.csv")[:500]
    monthly = _read_csv_rows(base / "monthly_returns.csv")
    annual = _read_csv_rows(base / "annual_returns.csv")

    return {
        "metrics": metrics,
        "daily_chart": _build_daily_chart(daily_rows),
        "trades": trades,
        "monthly_returns": monthly,
        "annual_returns": annual,
    }
