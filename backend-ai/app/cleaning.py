"""
cleaning.py — Data cleaning and validation pipeline.

Input:  raw canonical DataFrame from ingestion.py
Output: clean daily-level DataFrame with gap-filled dates, deduped SKU names,
        and quarantined outliers removed.
"""
from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd
from rapidfuzz import process, fuzz

logger = logging.getLogger(__name__)

IQR_FENCE = 3.0  # outlier threshold: values > median + N*IQR are quarantined


def dedup_sku_names(df: pd.DataFrame, threshold: int = 88) -> pd.DataFrame:
    """
    Fuzzy-merge near-duplicate SKU names within the same category.
    E.g. 'Nasi Goreng ', 'nasi goreng', 'Nasi goreng' → canonical first-seen label.
    threshold: minimum score (0-100) to consider two names the same.
    """
    from rapidfuzz import process, fuzz

    canonical: dict[str, str] = {}  # sku_name_lower → canonical form

    for raw_name in df["sku_name"].unique():
        key = raw_name.strip().lower()
        if not canonical:
            canonical[key] = raw_name.strip()
            continue
        match = process.extractOne(key, list(canonical.keys()), scorer=fuzz.token_sort_ratio)
        if match and match[1] >= threshold:
            canonical[key] = canonical[match[0]]
        else:
            canonical[key] = raw_name.strip()

    df = df.copy()
    df["sku_name"] = df["sku_name"].apply(lambda n: canonical.get(n.strip().lower(), n.strip()))
    return df


def quarantine_outliers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove per-SKU qty_sold outliers using IQR fence.
    Returns (clean_df, quarantined_df).
    """
    clean_rows, quarantined_rows = [], []
    for sku, group in df.groupby("sku"):
        q1 = group["qty_sold"].quantile(0.25)
        q3 = group["qty_sold"].quantile(0.75)
        iqr = q3 - q1
        upper = q3 + IQR_FENCE * iqr
        is_outlier = group["qty_sold"] > upper
        clean_rows.append(group[~is_outlier])
        if is_outlier.any():
            logger.warning(f"Quarantined {is_outlier.sum()} rows for SKU '{sku}' (above {upper:.1f})")
            quarantined_rows.append(group[is_outlier])
    clean_df = pd.concat(clean_rows, ignore_index=True)
    quarantined_df = pd.concat(quarantined_rows, ignore_index=True) if quarantined_rows else pd.DataFrame()
    return clean_df, quarantined_df


def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate to one row per (ds, sku). Multiple transactions on the same day/SKU
    are summed (qty) and averaged (unit_price).
    """
    agg = (
        df.groupby(["ds", "sku", "sku_name", "category"], as_index=False)
        .agg(
            qty_sold=("qty_sold",    "sum"),
            unit_price=("unit_price", "mean"),
            qty_prepared=("qty_prepared", "sum"),
        )
    )
    # qty_prepared is optional — if no row had a value, leave as NaN
    if (agg["qty_prepared"] == 0).all():
        agg["qty_prepared"] = np.nan
    return agg


def gap_fill_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each SKU, fill in missing dates with qty_sold=0.
    This ensures the time series is contiguous for the forecasting models.
    """
    pieces = []
    for sku, group in df.groupby("sku"):
        meta = group.iloc[0][["sku", "sku_name", "category"]].to_dict()
        full_idx = pd.date_range(
            pd.Timestamp(group["ds"].min()),
            pd.Timestamp(group["ds"].max()),
            freq="D"
        )
        series = (
            group.set_index("ds")["qty_sold"]
            .reindex(full_idx, fill_value=0.0)
            .astype(float)
        )
        piece = pd.DataFrame({"ds": series.index.date, "qty_sold": series.values})
        piece["sku"]      = meta["sku"]
        piece["sku_name"] = meta["sku_name"]
        piece["category"] = meta["category"]
        # avg_unit_price: forward-fill then back-fill so gap days have a price estimate
        price_series = group.set_index("ds")["unit_price"].reindex(full_idx).ffill().bfill()
        piece["unit_price"] = price_series.values
        pieces.append(piece)

    return pd.concat(pieces, ignore_index=True)


def run(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Full cleaning pipeline. Returns a clean, gap-filled daily DataFrame."""
    if raw_df.empty:
        return raw_df

    df = raw_df.copy()
    df = dedup_sku_names(df)
    df = aggregate_daily(df)
    df, quarantined = quarantine_outliers(df)
    df = gap_fill_dates(df)

    logger.info(
        f"Cleaning complete: {len(df):,} rows, {df['sku'].nunique()} SKUs, "
        f"{quarantined.shape[0]} quarantined rows"
    )
    return df
