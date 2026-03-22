"""
confidence.py — 4-gate confidence tier logic (context Table 1) + missing-data policy (Table 5).

Extracted from exp02_coffee_stable.ipynb (recency bug already fixed in that version).

Gate 1: data volume   — unique_sale_days < 7  → red  |  7-20 → yellow  |  21+ → continue
Gate 2: recency       — gap > 14 days         → red  (applies inside yellow path too)
Gate 3: volatility    — CV > 2.0              → yellow
Gate 4: interval      — width > 2×yhat        → yellow
Fallback override:    — both MAPE > 40%       → force yellow (unless already red)
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def apply_tier(row: pd.Series | dict, inference_date: pd.Timestamp) -> dict:
    """
    Compute confidence tier for a single SKU result row.
    Returns a dict with tier, tier_reason, and gate diagnostics.
    """
    usd  = row.get("unique_sale_days",    np.nan)
    gap  = int((inference_date - pd.Timestamp(row["last_observed_date"])).days)
    cv   = row.get("sales_cv",            np.nan)
    yhat = row.get("next_day_pred_qty",   np.nan)
    lo   = row.get("next_day_lower",      np.nan)
    hi   = row.get("next_day_upper",      np.nan)

    iw       = (hi - lo) if pd.notna(hi) and pd.notna(lo) else np.nan
    iv_pass  = pd.notna(iw) and pd.notna(yhat) and yhat > 0 and iw <= 2.0 * yhat

    # Gate 1
    if pd.isna(usd) or usd < 7:
        tier, reason = "red", "gate1_data_volume_red"
    elif usd <= 20:
        tier, reason = "yellow", "gate1_data_volume_yellow"
        if gap > 14:                 # Gate 2 still applies in yellow zone
            tier, reason = "red", "gate2_recency_red"
    else:
        if gap > 14:                 # Gate 2
            tier, reason = "red", "gate2_recency_red"
        elif pd.notna(cv) and cv > 2.0:   # Gate 3
            tier, reason = "yellow", "gate3_volatility_yellow"
        elif not iv_pass:            # Gate 4
            tier, reason = "yellow", "gate4_interval_yellow"
        else:
            tier, reason = "green", "all_gates_green"

    # Fallback override
    p = row.get("prophet_mape", np.nan)
    a = row.get("arima_mape",   np.nan)
    both_weak = pd.notna(p) and pd.notna(a) and p > 0.40 and a > 0.40
    if both_weak and tier != "red":
        tier, reason = "yellow", "fallback_force_yellow_both_mape_gt_40"

    return {
        "gap_days":             gap,
        "data_volume_pass":     pd.notna(usd) and usd >= 21,
        "recency_pass":         gap <= 14,
        "volatility_pass":      pd.notna(cv) and cv <= 2.0,
        "interval_pass":        iv_pass,
        "interval_width_ratio": (iw / yhat) if pd.notna(iw) and pd.notna(yhat) and yhat > 0 else np.nan,
        "tier":                 tier,
        "tier_reason":          reason,
        "both_models_weak":     both_weak,
    }


def apply_missing_data_policy(base_tier: str, missing_days: int, gap_after_shift: int) -> tuple[str, str]:
    """
    Downgrade tier based on consecutive missing days (Table 5).
    Recency interaction checked first: gap > 14 → force Red.
    """
    if gap_after_shift > 14:
        return "red", "recency_force_red_gt_14"
    if missing_days >= 7:
        return "red", "missing_7_plus_force_red"
    if 3 <= missing_days <= 6:
        if base_tier == "green":  return "yellow", "missing_3_to_6_downgrade"
        if base_tier == "yellow": return "red",    "missing_3_to_6_downgrade"
        return "red", "missing_3_to_6_keep_red"
    if 1 <= missing_days <= 2:
        return base_tier, "missing_1_to_2_no_downgrade"
    return base_tier, "missing_0_no_change"


def assign_tiers(
    results_df: pd.DataFrame,
    inference_today: pd.Timestamp,
    missing_days_sim: int = 3,
) -> pd.DataFrame:
    """
    Attach `today_tier`, `today_tier_reason`, `missing3d_tier`, `missing3d_tier_reason`
    columns to results_df. Safe against the gap_days Series collision from exp02.
    """
    today_rows = results_df.apply(
        lambda r: pd.Series(apply_tier(r, inference_today)), axis=1
    )

    missing_tiers, missing_reasons = [], []
    for i, row in results_df.iterrows():
        base_gap = int((inference_today - pd.Timestamp(row["last_observed_date"])).days)
        t2, r2 = apply_missing_data_policy(
            today_rows.loc[i, "tier"],
            missing_days_sim,
            base_gap + missing_days_sim,
        )
        missing_tiers.append(t2)
        missing_reasons.append(r2)

    out = results_df.copy()
    out["today_gap_days"]        = today_rows["gap_days"].values
    out["today_tier"]            = today_rows["tier"].values
    out["today_tier_reason"]     = today_rows["tier_reason"].values
    out["missing3d_gap_days"]    = today_rows["gap_days"].values + missing_days_sim
    out["missing3d_tier"]        = missing_tiers
    out["missing3d_tier_reason"] = missing_reasons
    return out


def worst_tier(tiers: list[str]) -> str:
    """Return the worst tier in a list — used to set the payload-level confidence_tier."""
    if "red" in tiers:    return "red"
    if "yellow" in tiers: return "yellow"
    return "green"
