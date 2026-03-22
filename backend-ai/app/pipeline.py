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

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from app import db, ingestion, cleaning, forecasting, confidence, profit, llm, delivery

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

DEV_MODE = os.environ.get("DEV_MODE", "false").lower() == "true"


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
            "tier":     r["today_tier"],
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
    if run_date is None:
        run_date = date.today()

    owner_id = account["id"]
    phone    = account["whatsapp_number"]
    name     = account.get("name", owner_id)
    missing  = int(account.get("consecutive_missing_days", 0))

    logger.info(f"--- Starting pipeline for owner '{name}' ({owner_id}) ---")

    # ── 1. Ingest ─────────────────────────────────────────────────────────────
    try:
        raw_df = ingestion.fetch_sales(account)
    except Exception as e:
        logger.error(f"Ingestion failed for {owner_id}: {e}")
        raise

    # ── 2. Check for new data ─────────────────────────────────────────────────
    last_received = account.get("last_data_received_at")
    has_new_data  = not raw_df.empty and (
        last_received is None
        or raw_df["ds"].max() > pd.Timestamp(last_received).date()
    )

    if has_new_data:
        missing = 0
        if not DEV_MODE:
            db.update_last_data_received(owner_id)
    else:
        missing += 1
        if not DEV_MODE:
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
        delivery.send_wa(phone, reminder_msg)
        if not DEV_MODE:
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
        if not DEV_MODE:
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
    wa_message = llm.generate_wa_message(partial_payload) + reminder_suffix

    # ── 9. Final payload ──────────────────────────────────────────────────────
    payload = _build_payload(
        account, run_date, results_df, profit_rows, anomaly_flags,
        wa_message=wa_message, daily_df=daily_df, missing_days=missing
    )

    # ── 10. Deliver ───────────────────────────────────────────────────────────
    delivery.send_wa(phone, wa_message)

    if missing >= 1:
        if not DEV_MODE:
            escalation = 1 if missing <= 2 else 2
            db.log_reminder(owner_id, escalation)

    # ── 11. Persist ───────────────────────────────────────────────────────────
    if DEV_MODE:
        logger.info(f"[DEV_MODE] Payload preview:\n{json.dumps(payload, indent=2, default=str)[:1500]}...")
    else:
        db.upsert_daily_payload(
            owner_id, run_date, payload, payload["confidence_tier"]
        )
        logger.info(f"Payload saved for {owner_id} / {run_date}")

    return payload


def run_all_owners(run_date: date | None = None) -> None:
    """Run the pipeline for all accounts. Failures are isolated per account."""
    if run_date is None:
        run_date = date.today()

    if DEV_MODE:
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
    args = parser.parse_args()

    if args.csv:
        os.environ["DEV_CSV_PATH"] = args.csv
    os.environ["DEV_MODE"] = "true"

    demo_account = {
        "id":                       args.owner_id,
        "name":                     "Demo Owner",
        "whatsapp_number":          "0812000000",
        "sheet_id":                 "",
        "consecutive_missing_days": 0,
        "last_data_received_at":    None,
        "use_real_cogs":            False,
    }
    run_owner(demo_account)
