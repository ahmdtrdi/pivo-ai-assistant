"""
pipeline.py — Nightly orchestrator.

One call to run_owner() processes a single account end-to-end:
  1. Ingest new sales data (Sheets / CSV)
  2. Clean + validate
  3. Missing-data check → WA reminder if too stale; skip forecast if 7+ days
  4. Run forecasting loop (Prophet → ARIMA, ≤20 active SKUs)
  5. Assign confidence tiers
  6. Compute profit analysis
  7. Compute trend + stockout_risk
  8. Build Fig2-aligned payload
  9. Generate WA message via Gemini
  10. Send WA via Fonnte
  11. Persist payload to Supabase daily_payloads
  12. Update account.consecutive_missing_days + last_data_received_at

run_all_owners() calls run_owner() per account and isolates failures.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from app import (
    db,
    ingestion,
    cleaning,
    forecasting,
    confidence,
    profit,
    llm,
    delivery,
    payload_schema as payload_contract,
    replay_artifacts,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if raw == "":
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _parse_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if raw == "":
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise ValueError(f"Invalid float for {name}: {raw!r}") from e


def _is_dev_mode() -> bool:
    return _parse_bool_env("DEV_MODE", False)


def _is_replay_mode() -> bool:
    return _parse_bool_env("REPLAY_MODE", False)


def _strict_no_future_data() -> bool:
    return _parse_bool_env("REPLAY_STRICT_NO_FUTURE", True)


def _replay_scenario() -> str:
    scenario = os.environ.get("REPLAY_SCENARIO", "normal").strip().lower() or "normal"
    allowed = {"normal", "missing_input", "spike", "drop"}
    if scenario not in allowed:
        raise ValueError(
            f"Invalid REPLAY_SCENARIO={scenario!r}. Allowed: {sorted(allowed)}"
        )
    return scenario


def _resolve_effective_run_date(run_date: date | None) -> date:
    if run_date is not None:
        return run_date
    if _is_replay_mode():
        cursor = os.environ.get("REPLAY_CURSOR_DATE", "").strip()
        if cursor:
            try:
                return date.fromisoformat(cursor)
            except ValueError as e:
                raise ValueError(
                    f"Invalid REPLAY_CURSOR_DATE={cursor!r}. Use YYYY-MM-DD."
                ) from e
    return date.today()


def _apply_no_future_guard(raw_df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df
    if not _is_replay_mode() and not _strict_no_future_data():
        return raw_df

    ts = pd.to_datetime(raw_df["ds"], errors="coerce")
    keep_mask = ts <= pd.Timestamp(run_date)
    dropped = int((~keep_mask).sum())
    out = raw_df.loc[keep_mask].reset_index(drop=True)
    if dropped > 0:
        logger.info(
            f"No-future-data guard dropped {dropped} row(s) newer than cursor {run_date}."
        )
    return out


def _apply_replay_scenario(raw_df: pd.DataFrame, run_date: date) -> tuple[pd.DataFrame, bool]:
    """
    Apply replay scenario transform.

    Returns:
      (transformed_df, force_no_new_data)
    """
    if not _is_replay_mode():
        return raw_df, False

    scenario = _replay_scenario()
    if raw_df.empty:
        return raw_df, scenario == "missing_input"

    out = raw_df.copy()
    if scenario == "normal":
        return out, False

    if scenario == "missing_input":
        logger.info("[REPLAY_MODE] Scenario=missing_input: forcing has_new_data=False")
        return out, True

    ds_series = pd.to_datetime(out["ds"], errors="coerce").dt.date
    on_cursor = ds_series == run_date
    affected = int(on_cursor.sum())
    if affected == 0:
        logger.info(f"[REPLAY_MODE] Scenario={scenario}: no rows on cursor date {run_date}")
        return out, False

    if scenario == "spike":
        factor = max(_parse_float_env("REPLAY_SPIKE_MULTIPLIER", 2.0), 1.0)
        out.loc[on_cursor, "qty_sold"] = pd.to_numeric(
            out.loc[on_cursor, "qty_sold"], errors="coerce"
        ).fillna(0.0) * factor
        logger.info(
            f"[REPLAY_MODE] Scenario=spike applied to {affected} row(s) at {run_date} with factor={factor:.2f}"
        )
        return out, False

    # scenario == "drop"
    factor = _parse_float_env("REPLAY_DROP_MULTIPLIER", 0.3)
    factor = min(max(factor, 0.0), 1.0)
    out.loc[on_cursor, "qty_sold"] = pd.to_numeric(
        out.loc[on_cursor, "qty_sold"], errors="coerce"
    ).fillna(0.0) * factor
    logger.info(
        f"[REPLAY_MODE] Scenario=drop applied to {affected} row(s) at {run_date} with factor={factor:.2f}"
    )
    return out, False


def _cursor_state_path(override: str | None = None) -> Path:
    if override and override.strip():
        return Path(override).expanduser()
    env_path = os.environ.get("REPLAY_CURSOR_STATE_PATH", "").strip()
    if env_path:
        return Path(env_path).expanduser()
    return Path(__file__).resolve().parents[1] / "outputs" / "replay_cursor_state.json"


def _load_cursor_state(path: Path) -> date | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        raw = str(data.get("cursor_date", "")).strip()
        return date.fromisoformat(raw) if raw else None
    except Exception:
        return None


def _save_cursor_state(path: Path, cursor_date: date) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "cursor_date": cursor_date.isoformat(),
        "updated_at_utc": pd.Timestamp.utcnow().isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _step_cursor_date(base_date: date, step_days: int) -> date:
    if step_days not in (0, 1, 7):
        raise ValueError("step_days must be one of: 0, 1, 7")
    return base_date + timedelta(days=step_days)


# ── Trend & stockout ──────────────────────────────────────────────────────────

def _compute_trend(sku: str, daily_df: pd.DataFrame, next_pred: float) -> tuple[str, bool]:
    hist = (
        daily_df[daily_df["sku"] == sku]
        .sort_values("ds")
        .set_index("ds")["qty_sold"]
    )
    if len(hist) < 14:
        return "stable", False
    last7  = float(hist.iloc[-7:].mean())
    prev7  = float(hist.iloc[-14:-7].mean())
    if prev7 > 0:
        pct = (last7 - prev7) / prev7
        trend = "growing" if pct > 0.10 else ("declining" if pct < -0.10 else "stable")
    else:
        trend = "stable"
    stockout = pd.notna(next_pred) and last7 > 0 and next_pred > last7 * 1.15
    return trend, bool(stockout)


def _maybe_write_replay_artifacts(
    *,
    owner_id: str,
    run_date: date,
    status: str,
    payload: dict,
    raw_rows: int | None,
    clean_rows: int | None,
    missing_days: int,
    has_new_data: bool,
    delivery_ok: bool | None,
    results_df: pd.DataFrame | None,
) -> None:
    if not replay_artifacts.artifacts_enabled(_is_dev_mode()):
        return
    metrics = replay_artifacts.build_metrics(
        owner_id=str(owner_id),
        run_date=run_date,
        status=status,
        payload=payload,
        raw_rows=raw_rows,
        clean_rows=clean_rows,
        missing_days=missing_days,
        has_new_data=has_new_data,
        delivery_ok=delivery_ok,
        results_df=results_df,
    )
    out_dir = replay_artifacts.write_run_artifacts(
        owner_id=str(owner_id),
        run_date=run_date,
        payload=payload,
        metrics=metrics,
        results_df=results_df,
    )
    logger.info(f"Replay artifacts saved: {out_dir}")


# ── Payload builder ───────────────────────────────────────────────────────────

def _build_payload(
    account: dict,
    run_date: date,
    results_df: pd.DataFrame,
    profit_rows: list[dict],
    anomaly_flags: list[str],
    wa_message: str,
    daily_df: pd.DataFrame,
    missing_days: int,
) -> dict:
    modeled = results_df[results_df["selected_model"] != "none"].copy()
    skipped = results_df[results_df["selected_model"] == "none"].copy()

    forecasts_list = []
    for _, r in modeled.sort_values("next_day_pred_qty", ascending=False).iterrows():
        trend, stockout = _compute_trend(r["sku"], daily_df, r["next_day_pred_qty"])
        forecasts_list.append({
            "sku":                r["sku"],
            "sku_name":           r.get("sku_name", r["sku"]),
            "category":           r.get("category", ""),
            "selected_model":     r["selected_model"],
            "tier":               r["today_tier"],
            "tier_if_missing_3d": r["missing3d_tier"],
            "qty_mid":            round(float(r["next_day_pred_qty"]), 2),
            "qty_low":            round(float(r["next_day_lower"]), 2),
            "qty_high":           round(float(r["next_day_upper"]), 2),
            "trend":              trend,
            "stockout_risk":      stockout,
        })

    skipped_list = [
        {
            "sku":      r["sku"],
            "sku_name": r.get("sku_name", r["sku"]),
            "reason":   r["pre_gate_reason"],
            "tier":     "red",
            "gap_days": int(r["today_gap_days"]),
        }
        for _, r in skipped.iterrows()
    ]

    all_tiers = results_df["today_tier"].tolist() if "today_tier" in results_df.columns else []
    payload_tier = confidence.worst_tier(all_tiers)

    return {
        "owner_id":                str(account["id"]),
        "date":                    run_date.isoformat(),
        "model_routing":           "prophet_first_arima_fallback",
        "confidence_tier":         payload_tier,
        "consecutive_missing_days": missing_days,
        "forecasts":               forecasts_list,
        "profit_analysis":         profit_rows,
        "anomaly_flags":           anomaly_flags,
        "wa_message":              wa_message,
        "pwa_url":                 f"pivo.app/u/{account['id']}",
        "skipped_skus":            skipped_list,
    }


# ── Per-owner orchestration ───────────────────────────────────────────────────

def run_owner(account: dict, run_date: date | None = None) -> dict:
    """
    Run the full nightly pipeline for one account.
    Returns the payload dict (whether saved to Supabase or not).
    """
    run_date = _resolve_effective_run_date(run_date)
    dev_mode = _is_dev_mode()

    owner_id = account["id"]
    phone    = account["whatsapp_number"]
    name     = account.get("name", owner_id)
    missing  = int(account.get("consecutive_missing_days", 0))

    logger.info(f"--- Starting pipeline for owner '{name}' ({owner_id}) ---")
    if _is_replay_mode():
        logger.info(f"[REPLAY_MODE] Cursor date: {run_date}")

    # ── 1. Ingest ─────────────────────────────────────────────────────────────
    try:
        raw_df = ingestion.fetch_sales(account)
    except Exception as e:
        logger.error(f"Ingestion failed for {owner_id}: {e}")
        raise
    raw_df = _apply_no_future_guard(raw_df, run_date)
    raw_df, force_no_new_data = _apply_replay_scenario(raw_df, run_date)

    # ── 2. Check for new data ─────────────────────────────────────────────────
    last_received = account.get("last_data_received_at")
    has_new_data  = not raw_df.empty and (
        last_received is None
        or raw_df["ds"].max() > pd.Timestamp(last_received).date()
    )
    if force_no_new_data:
        has_new_data = False

    if has_new_data:
        missing = 0
        if not dev_mode:
            db.update_last_data_received(owner_id)
    else:
        missing += 1
        if not dev_mode:
            db.update_missing_days(owner_id, missing)

    logger.info(f"New data: {has_new_data} | Consecutive missing days: {missing}")

    # ── 3. Missing-data escalation (Table 5) ──────────────────────────────────
    if missing >= 7:
        # Force Red — WA reminder only, no forecast
        escalation = 3
        reminder_msg = (
            f"🔴 *PIVO — Pengingat Penting* — {run_date}\n\n"
            f"Hai {name}! 🙏\n"
            "Sudah lebih dari 7 hari data penjualan tidak diisi.\n"
            "Prediksi tidak bisa dibuat dulu — yuk mulai catat lagi hari ini!\n\n"
            "_— PIVO otomatis mengirim pesan ini setiap hari_"
        )
        delivery_ok = delivery.send_wa(phone, reminder_msg)
        if not dev_mode:
            db.log_reminder(owner_id, escalation)
        logger.info(f"Missing 7+ days — reminder sent, skipping forecast.")
        # Save a Red payload so the PWA still shows something
        payload = {
            "owner_id":                str(owner_id),
            "date":                    run_date.isoformat(),
            "model_routing":           "skipped_missing_data",
            "confidence_tier":         "red",
            "consecutive_missing_days": missing,
            "forecasts":               [],
            "profit_analysis":         [],
            "anomaly_flags":           [],
            "wa_message":              reminder_msg,
            "pwa_url":                 f"pivo.app/u/{owner_id}",
            "skipped_skus":            [],
        }
        payload_contract.validate_payload(payload, context="missing_data_payload")
        _maybe_write_replay_artifacts(
            owner_id=owner_id,
            run_date=run_date,
            status="missing_data_skip",
            payload=payload,
            raw_rows=len(raw_df),
            clean_rows=None,
            missing_days=missing,
            has_new_data=has_new_data,
            delivery_ok=delivery_ok,
            results_df=None,
        )
        if not dev_mode:
            db.upsert_daily_payload(owner_id, run_date, payload, "red")
        return payload

    # Gentle reminder suffix for 1-6 missing days
    reminder_suffix = ""
    if 1 <= missing <= 2:
        reminder_suffix = "\n\n📝 _Jangan lupa catat penjualan hari ini ya!_"
    elif 3 <= missing <= 6:
        reminder_suffix = (
            f"\n\n⚠️ _Data {missing} hari belum masuk — akurasi prediksi berkurang. "
            "Catat penjualan sekarang supaya prediksi tetap tajam!_"
        )

    # ── 4. Clean ─────────────────────────────────────────────────────────────
    daily_df = cleaning.run(raw_df)

    if daily_df.empty:
        logger.warning(f"No usable data after cleaning for {owner_id}")
        return {}

    # ── 5. Forecast ───────────────────────────────────────────────────────────
    inference_today = pd.Timestamp(run_date)
    results_df = forecasting.run_sku_loop(daily_df, inference_today)

    if results_df.empty:
        logger.warning(f"No SKU results for {owner_id}")
        return {}

    # ── 6. Tiers ──────────────────────────────────────────────────────────────
    results_df = confidence.assign_tiers(results_df, inference_today, missing_days_sim=3)

    # Downgrade tier if already in 3-6 missing days (Table 5)
    if 3 <= missing <= 6:
        def _downgrade(row):
            t, r = confidence.apply_missing_data_policy(row["today_tier"], missing, int(row["today_gap_days"]) + missing)
            return pd.Series({"today_tier": t, "today_tier_reason": r})
        overrides = results_df.apply(_downgrade, axis=1)
        results_df["today_tier"]        = overrides["today_tier"]
        results_df["today_tier_reason"] = overrides["today_tier_reason"]

    # ── 7. Profit ─────────────────────────────────────────────────────────────
    use_real_cogs = bool(account.get("use_real_cogs", False))
    profit_rows, anomaly_flags = profit.run(daily_df, results_df, use_real_cogs=use_real_cogs)

    # ── 8. LLM ───────────────────────────────────────────────────────────────
    # Build a partial payload first so the LLM has all context
    partial_payload = _build_payload(
        account, run_date, results_df, profit_rows, anomaly_flags,
        wa_message="", daily_df=daily_df, missing_days=missing
    )
    payload_contract.validate_payload(partial_payload, context="partial_payload_for_llm")
    wa_message = llm.generate_wa_message(partial_payload) + reminder_suffix

    # ── 9. Final payload ──────────────────────────────────────────────────────
    payload = _build_payload(
        account, run_date, results_df, profit_rows, anomaly_flags,
        wa_message=wa_message, daily_df=daily_df, missing_days=missing
    )
    payload_contract.validate_payload(payload, context="final_payload")

    # ── 10. Deliver ───────────────────────────────────────────────────────────
    delivery_ok = delivery.send_wa(phone, wa_message)

    if missing >= 1:
        if not dev_mode:
            escalation = 1 if missing <= 2 else 2
            db.log_reminder(owner_id, escalation)

    _maybe_write_replay_artifacts(
        owner_id=owner_id,
        run_date=run_date,
        status="ok",
        payload=payload,
        raw_rows=len(raw_df),
        clean_rows=len(daily_df),
        missing_days=missing,
        has_new_data=has_new_data,
        delivery_ok=delivery_ok,
        results_df=results_df,
    )

    # ── 11. Persist ───────────────────────────────────────────────────────────
    if dev_mode:
        logger.info(f"[DEV_MODE] Payload preview:\n{json.dumps(payload, indent=2, default=str)[:1500]}...")
    else:
        db.upsert_daily_payload(
            owner_id, run_date, payload, payload["confidence_tier"]
        )
        logger.info(f"Payload saved for {owner_id} / {run_date}")

    return payload


def run_all_owners(run_date: date | None = None) -> None:
    """Run the pipeline for all accounts. Failures are isolated per account."""
    run_date = _resolve_effective_run_date(run_date)
    dev_mode = _is_dev_mode()

    if dev_mode:
        logger.info("[DEV_MODE] Supabase reads/writes are disabled.")
        accounts = [{"id": "demo", "name": "Demo Owner", "whatsapp_number": "0812000000",
                     "sheet_id": "", "consecutive_missing_days": 0, "last_data_received_at": None}]
    else:
        accounts = db.get_all_accounts()

    logger.info(f"Running pipeline for {len(accounts)} accounts on {run_date}")

    for account in accounts:
        try:
            run_owner(account, run_date)
        except Exception as e:
            logger.error(f"Pipeline failed for account {account.get('id')}: {e}", exc_info=True)
            # Continue to next account — one failure does not block others


# ── CLI entry for local testing ───────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run PIVO pipeline locally")
    parser.add_argument("--owner-id", default="demo")
    parser.add_argument("--csv", default=None, help="Path to a local CSV file (dev mode)")
    parser.add_argument("--run-date", default=None, help="Replay cursor date (YYYY-MM-DD)")
    parser.add_argument("--replay-mode", action="store_true", help="Enable replay mode")
    parser.add_argument("--step-days", type=int, choices=[0, 1, 7], default=0, help="Advance replay cursor by days")
    parser.add_argument("--cursor-state-path", default=None, help="Optional path for replay cursor state JSON")
    parser.add_argument(
        "--scenario",
        choices=["normal", "missing_input", "spike", "drop"],
        default=None,
        help="Replay scenario transform to apply on cursor date",
    )
    args = parser.parse_args()

    if args.csv:
        os.environ["DEV_CSV_PATH"] = args.csv
    os.environ["DEV_MODE"] = "true"
    cli_run_date = None
    if args.run_date:
        try:
            cli_run_date = date.fromisoformat(args.run_date)
        except ValueError:
            parser.error("--run-date must use YYYY-MM-DD")

    if args.step_days and not args.replay_mode:
        parser.error("--step-days requires --replay-mode")

    if args.replay_mode:
        os.environ["REPLAY_MODE"] = "true"
        if args.scenario:
            os.environ["REPLAY_SCENARIO"] = args.scenario
        state_path = _cursor_state_path(args.cursor_state_path)

        base_cursor = cli_run_date
        if base_cursor is None:
            state_cursor = _load_cursor_state(state_path)
            if state_cursor is not None:
                base_cursor = state_cursor
            else:
                base_cursor = _resolve_effective_run_date(None)

        stepped_cursor = _step_cursor_date(base_cursor, args.step_days)
        os.environ["REPLAY_CURSOR_DATE"] = stepped_cursor.isoformat()
        _save_cursor_state(state_path, stepped_cursor)
        logger.info(f"[REPLAY_MODE] Cursor set to {stepped_cursor} (state: {state_path})")
        logger.info(f"[REPLAY_MODE] Scenario: {_replay_scenario()}")
        cli_run_date = stepped_cursor

    demo_account = {
        "id":                       args.owner_id,
        "name":                     "Demo Owner",
        "whatsapp_number":          "0812000000",
        "sheet_id":                 "",
        "consecutive_missing_days": 0,
        "last_data_received_at":    None,
        "use_real_cogs":            False,
    }
    run_owner(demo_account, run_date=cli_run_date)

