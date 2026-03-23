"""
replay_artifacts.py - Persist replay/smoke-test artifacts to local files.

Stores per-run outputs so we can inspect pipeline behavior day-by-day:
- payload JSON
- WA message text
- confidence tier
- run metrics summary
- optional forecast rows CSV
"""
from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    raw = value.strip().lower()
    if raw == "":
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def artifacts_enabled(dev_mode: bool) -> bool:
    """
    Determine whether replay artifact writing is enabled.

    Behavior:
    - If REPLAY_ARTIFACTS_ENABLED is set, it is respected.
    - Else default to dev_mode (enabled for local/replay work, off for prod by default).
    """
    raw = os.environ.get("REPLAY_ARTIFACTS_ENABLED")
    if raw is not None and raw.strip() != "":
        return _parse_bool(raw, default=False)
    return dev_mode


def _base_dir() -> Path:
    override = os.environ.get("REPLAY_ARTIFACTS_DIR", "").strip()
    if override:
        return Path(override).expanduser()
    # backend-ai/outputs/replay_artifacts
    return Path(__file__).resolve().parents[1] / "outputs" / "replay_artifacts"


def _slug(text: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", str(text)).strip("_")


def _mean_or_none(series: pd.Series) -> float | None:
    if series.empty:
        return None
    value = pd.to_numeric(series, errors="coerce").dropna()
    if value.empty:
        return None
    return float(value.mean())


def build_metrics(
    *,
    owner_id: str,
    run_date: date,
    status: str,
    payload: dict[str, Any],
    raw_rows: int | None,
    clean_rows: int | None,
    missing_days: int,
    has_new_data: bool,
    delivery_ok: bool | None,
    results_df: pd.DataFrame | None,
) -> dict[str, Any]:
    """
    Build a compact metrics dict for replay tracking.
    """
    metrics: dict[str, Any] = {
        "owner_id": str(owner_id),
        "run_date": run_date.isoformat(),
        "status": status,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "confidence_tier": payload.get("confidence_tier"),
        "consecutive_missing_days": int(missing_days),
        "has_new_data": bool(has_new_data),
        "raw_rows": int(raw_rows) if raw_rows is not None else None,
        "clean_rows": int(clean_rows) if clean_rows is not None else None,
        "delivery_ok": delivery_ok,
        "wa_message_length": len(payload.get("wa_message", "") or ""),
        "forecast_count": len(payload.get("forecasts", []) or []),
        "skipped_count": len(payload.get("skipped_skus", []) or []),
        "profit_row_count": len(payload.get("profit_analysis", []) or []),
        "anomaly_count": len(payload.get("anomaly_flags", []) or []),
        "replay_mode": _parse_bool(os.environ.get("REPLAY_MODE"), default=False),
        "replay_scenario": os.environ.get("REPLAY_SCENARIO", "normal").strip().lower() or "normal",
    }

    if results_df is None or results_df.empty:
        metrics["model_counts"] = {}
        metrics["tier_counts"] = {}
        metrics["mean_selected_mape"] = None
        metrics["mean_selected_rmse"] = None
        metrics["mean_selected_smape"] = None
        return metrics

    model_counts = (
        results_df["selected_model"].astype(str).value_counts(dropna=False).to_dict()
        if "selected_model" in results_df.columns
        else {}
    )
    tier_counts = (
        results_df["today_tier"].astype(str).value_counts(dropna=False).to_dict()
        if "today_tier" in results_df.columns
        else {}
    )

    metrics["model_counts"] = model_counts
    metrics["tier_counts"] = tier_counts
    metrics["mean_selected_mape"] = (
        _mean_or_none(results_df["selected_mape"])
        if "selected_mape" in results_df.columns
        else None
    )
    metrics["mean_selected_rmse"] = (
        _mean_or_none(results_df["selected_rmse"])
        if "selected_rmse" in results_df.columns
        else None
    )
    metrics["mean_selected_smape"] = (
        _mean_or_none(results_df["selected_smape"])
        if "selected_smape" in results_df.columns
        else None
    )
    return metrics


def write_run_artifacts(
    *,
    owner_id: str,
    run_date: date,
    payload: dict[str, Any],
    metrics: dict[str, Any],
    results_df: pd.DataFrame | None = None,
) -> Path:
    """
    Persist run artifacts and return the output directory path.
    """
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = _base_dir() / _slug(owner_id) / run_date.isoformat() / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    with (out_dir / "payload.json").open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    with (out_dir / "metrics.json").open("w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2, default=str)

    message = payload.get("wa_message", "") or ""
    with (out_dir / "message.txt").open("w", encoding="utf-8") as f:
        f.write(str(message))

    with (out_dir / "tier.txt").open("w", encoding="utf-8") as f:
        f.write(str(payload.get("confidence_tier", "unknown")))

    if results_df is not None and not results_df.empty:
        results_df.to_csv(out_dir / "forecast_results.csv", index=False)

    return out_dir
