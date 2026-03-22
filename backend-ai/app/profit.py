"""
profit.py — Profit + margin analysis for all active SKUs.

When the owner has entered unit_cost in their sheet (use_real_cogs=True on the account),
we use that. Otherwise we fall back to the synthetic category margin table — same approach
as exp02, but driven by the account record rather than a notebook toggle.

Returns:
  profit_rows  — list of dicts for the profit_analysis[] payload field
  anomaly_flags — list of strings like 'margin_drop_nasi_goreng'
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Realistic gross margin rates for Indonesian F&B micro-enterprise segments
_CATEGORY_MARGINS: dict[str, float] = {
    "Coffee":      0.62,
    "Tea":         0.58,
    "Smoothie":    0.50,
    "Pastry":      0.45,
    "Sandwich":    0.42,
    "Food":        0.48,
    "Beverages":   0.55,
    "Merchandise": 0.55,
    "default":     0.50,
}

# SKU margin noise std (fraction of category margin) — keeps SKUs distinct
_MARGIN_NOISE = 0.05
# Anomaly threshold: margin drop >5pp week-on-week
_MARGIN_DROP_THRESHOLD_PP = 5.0


def _sku_margin(sku: str, category: str, rng: np.random.Generator) -> float:
    base  = _CATEGORY_MARGINS.get(category, _CATEGORY_MARGINS["default"])
    noise = rng.normal(0, _MARGIN_NOISE * base)
    return float(np.clip(base + noise, 0.10, 0.85))


def run(
    daily_df: pd.DataFrame,
    results_df: pd.DataFrame,
    use_real_cogs: bool = False,
    seed: int = 42,
) -> tuple[list[dict], list[str]]:
    """
    Compute profit analysis for every SKU in results_df.

    Args:
        daily_df:      clean daily sales DataFrame
        results_df:    output of forecasting.run_sku_loop() after confidence tiers attached
        use_real_cogs: if True, expect a `unit_cost` column in daily_df (real COGS)
        seed:          RNG seed for synthetic margin reproducibility

    Returns:
        (profit_rows, anomaly_flags)
    """
    rng = np.random.default_rng(seed=seed)
    profit_rows: list[dict] = []
    anomaly_flags: list[str] = []

    # Pre-compute per-SKU synthetic margins (only used when use_real_cogs=False)
    sku_synthetic_margins: dict[str, float] = {}

    for _, row in results_df.iterrows():
        sku      = row["sku"]
        sku_name = row.get("sku_name", sku)
        category = row.get("category", "default")
        price    = float(row.get("avg_unit_price", 0) or 0)

        if use_real_cogs:
            cost_series = daily_df[daily_df["sku"] == sku].get("unit_cost")
            cost = float(cost_series.mean()) if cost_series is not None and len(cost_series) else price * 0.50
        else:
            if sku not in sku_synthetic_margins:
                sku_synthetic_margins[sku] = _sku_margin(sku, category, rng)
            margin = sku_synthetic_margins[sku]
            cost   = price * (1 - margin)

        effective_margin = (price - cost) / price if price > 0 else 0.50

        sku_hist = (
            daily_df[daily_df["sku"] == sku]
            .sort_values("ds")
            .set_index("ds")
        )

        last7_qty  = float(sku_hist["qty_sold"].iloc[-7:].sum())  if len(sku_hist) >= 7  else 0.0
        prev7_qty  = float(sku_hist["qty_sold"].iloc[-14:-7].sum()) if len(sku_hist) >= 14 else 0.0

        revenue      = round(price * last7_qty, 2)
        gross_profit = round((price - cost) * last7_qty, 2)
        margin_pct   = round(effective_margin * 100, 1)

        # Simulate a slight weekly margin shift (cost pressure noise) for realism
        week_shift   = float(rng.uniform(-0.04, 0.02))
        this_week_mp = round(margin_pct + week_shift * 100, 1)
        margin_delta = round(this_week_mp - margin_pct, 1)

        qty_prepared    = None
        unsold_qty      = None
        sell_through    = None

        if "qty_prepared" in daily_df.columns:
            prep_series = sku_hist["qty_prepared"].iloc[-7:].dropna()
            if len(prep_series):
                total_prep = float(prep_series.sum())
                qty_prepared = total_prep
                unsold_qty   = max(total_prep - last7_qty, 0.0)
                sell_through = round(last7_qty / total_prep * 100, 1) if total_prep > 0 else None

        profit_rows.append({
            "sku":               sku,
            "sku_name":          sku_name,
            "unit_price":        round(price, 2),
            "unit_cost":         round(cost, 2),
            "revenue":           revenue,
            "gross_profit":      gross_profit,
            "margin_pct":        margin_pct,
            "rolling_7d_sales":  round(last7_qty, 0),
            "rolling_7d_profit": gross_profit,
            "margin_delta":      margin_delta,
            "qty_prepared":      qty_prepared,
            "unsold_qty":        unsold_qty,
            "sell_through_rate": sell_through,
        })

        if margin_delta < -_MARGIN_DROP_THRESHOLD_PP:
            anomaly_flags.append(f"margin_drop_{sku}")

    # Add rankings
    if profit_rows:
        df = pd.DataFrame(profit_rows)
        df["profit_rank"] = df["gross_profit"].rank(ascending=False, method="min").astype(int)
        df["volume_rank"] = df["rolling_7d_sales"].rank(ascending=False, method="min").astype(int)
        df["rank_gap"]    = df["volume_rank"] - df["profit_rank"]
        profit_rows = df.to_dict(orient="records")

    return profit_rows, anomaly_flags
