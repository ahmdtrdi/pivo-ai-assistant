"""
forecasting.py — Prophet-first / ARIMA-fallback per-SKU forecasting.

Extracted from exp02_coffee_stable.ipynb and promoted to production module.
Key decisions:
- Prophet MAPE > 40% triggers ARIMA fallback (PROPHET_FALLBACK_MAPE_THRESH)
- Active SKUs: sold at least once in last ACTIVE_WINDOW_DAYS days (env-configurable)
- Cap at MAX_ACTIVE_SKUS per run to bound compute time
- Pre-gating: skip model fit for Gate1/Gate2 Red SKUs (saves ~80% of compute)
"""
from __future__ import annotations

import logging
import os
import warnings

import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

try:
    from prophet import Prophet
    _PROPHET_OK = True
except ImportError:
    Prophet = None
    _PROPHET_OK = False
    logger.warning("Prophet not installed — Prophet routing disabled")

try:
    import pmdarima as pm
    _ARIMA_OK = True
except ImportError:
    pm = None
    _ARIMA_OK = False
    logger.warning("pmdarima not installed — ARIMA routing disabled")

# ── Config (env-overridable) ──────────────────────────────────────────────────
TEST_DAYS                    = int(os.getenv("TEST_DAYS",                   "30"))
ACTIVE_WINDOW_DAYS          = int(os.getenv("ACTIVE_WINDOW_DAYS",          "30"))
MAX_ACTIVE_SKUS             = int(os.getenv("MAX_ACTIVE_SKUS",             "20"))
PROPHET_FALLBACK_MAPE_THRESH = float(os.getenv("PROPHET_FALLBACK_MAPE_THRESH", "0.40"))


# ── Metric helpers ────────────────────────────────────────────────────────────

def safe_mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    mask = y_true != 0
    if mask.sum() == 0:
        return np.nan
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])))


def smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = np.abs(y_true) + np.abs(y_pred) + 1e-8
    return float(np.mean(2.0 * np.abs(y_true - y_pred) / denom))


# ── Model runners ─────────────────────────────────────────────────────────────

def run_prophet(train: pd.Series, horizon: int) -> tuple[dict | None, str | None]:
    if not _PROPHET_OK:
        return None, "prophet_not_installed"
    try:
        train_df = train.reset_index()
        train_df.columns = ["ds", "y"]
        m = Prophet(
            interval_width=0.80,
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
        )
        m.fit(train_df)
        future = m.make_future_dataframe(periods=horizon, freq="D", include_history=False)
        fc = m.predict(future)
        return {
            "pred":  np.clip(fc["yhat"].values,       0, None),
            "lower": np.clip(fc["yhat_lower"].values, 0, None),
            "upper": np.clip(fc["yhat_upper"].values, 0, None),
        }, None
    except Exception as e:
        return None, f"prophet_error: {e}"


def run_arima(train: pd.Series, horizon: int) -> tuple[dict | None, str | None]:
    if not _ARIMA_OK:
        return None, "arima_not_installed"
    try:
        try:
            model = pm.auto_arima(
                train, seasonal=True, m=7, stepwise=True,
                suppress_warnings=True, error_action="ignore"
            )
        except Exception:
            model = pm.auto_arima(
                train, seasonal=False, stepwise=True,
                suppress_warnings=True, error_action="ignore"
            )
        pred, conf = model.predict(n_periods=horizon, return_conf_int=True, alpha=0.20)
        return {
            "pred":  np.clip(pred,         0, None),
            "lower": np.clip(conf[:, 0],   0, None),
            "upper": np.clip(conf[:, 1],   0, None),
        }, None
    except Exception as e:
        return None, f"arima_error: {e}"


# ── Main SKU loop ─────────────────────────────────────────────────────────────

def run_sku_loop(daily_df: pd.DataFrame, inference_today: pd.Timestamp) -> pd.DataFrame:
    """
    Run the full Prophet→ARIMA routing loop for all active SKUs.

    Returns a results DataFrame with one row per SKU, ready for confidence.py.
    Columns: sku, sku_name, category, avg_unit_price, history_days,
             unique_sale_days, train_days, test_days, sales_cv,
             prophet_mape, arima_mape, selected_model, selected_mape,
             selected_rmse, selected_smape, pre_gate_reason,
             last_observed_date, gap_days,
             next_day_pred_qty, next_day_lower, next_day_upper,
             next_day_interval_ratio
    """
    last_date   = daily_df["ds"].max()
    active_start = pd.Timestamp(last_date) - pd.Timedelta(days=ACTIVE_WINDOW_DAYS - 1)

    # Active SKU filter
    active_skus = (
        daily_df[pd.to_datetime(daily_df["ds"]) >= active_start]
        .groupby("sku", as_index=False)["qty_sold"].sum()
        .query("qty_sold > 0")["sku"]
        .tolist()
    )

    # Sort by volume, cap at MAX_ACTIVE_SKUS
    sku_volumes = (
        daily_df[daily_df["sku"].isin(active_skus)]
        .groupby("sku", as_index=False)["qty_sold"].sum()
        .sort_values("qty_sold", ascending=False)
    )
    active_skus = sku_volumes.head(MAX_ACTIVE_SKUS)["sku"].tolist()

    sku_meta = (
        daily_df[["sku", "sku_name", "category", "unit_price"]]
        .groupby("sku")
        .agg(sku_name=("sku_name", "first"),
             category=("category", "first"),
             avg_unit_price=("unit_price", "mean"))
    )

    logger.info(f"Processing {len(active_skus)} active SKUs")
    results = []

    for sku in active_skus:
        sku_df  = daily_df[daily_df["sku"] == sku][["ds", "qty_sold"]].copy()
        sku_df["ds"] = pd.to_datetime(sku_df["ds"])

        full_idx = pd.date_range(sku_df["ds"].min(), sku_df["ds"].max(), freq="D")
        series   = sku_df.set_index("ds")["qty_sold"].reindex(full_idx, fill_value=0).astype(float)
        series.index.name = "ds"

        history_days     = len(series)
        unique_sale_days = int((series > 0).sum())
        last_observed    = series.index.max()
        gap_days         = int((inference_today - last_observed).days)

        if history_days < (TEST_DAYS + 7):
            continue

        train = series.iloc[:-TEST_DAYS]
        test  = series.iloc[-TEST_DAYS:]

        train_mean = float(train.mean())
        sales_cv   = float(train.std(ddof=0) / train_mean) if train_mean > 0 else np.inf

        # Pre-gating: skip model fit for certain Gate conditions
        if unique_sale_days < 7 or gap_days > 14:
            pre_gate = "gate1_data_volume_red" if unique_sale_days < 7 else "gate2_recency_red"
            results.append(_make_row(sku, sku_meta, history_days, unique_sale_days,
                                     train, test, sales_cv, last_observed, gap_days,
                                     pre_gate_reason=pre_gate, skipped=True))
            continue

        # Prophet first
        prophet_res, prophet_err = run_prophet(train, TEST_DAYS)
        prophet_mape = safe_mape(test.values, prophet_res["pred"]) if prophet_res else np.nan
        prophet_rmse = float(np.sqrt(mean_squared_error(test.values, prophet_res["pred"]))) if prophet_res else np.nan

        # ARIMA if needed
        arima_res, arima_err = None, None
        arima_mape = arima_rmse = np.nan
        prophet_weak = prophet_res and pd.notna(prophet_mape) and prophet_mape > PROPHET_FALLBACK_MAPE_THRESH
        need_arima = (not prophet_res) or prophet_weak
        if need_arima:
            arima_res, arima_err = run_arima(train, TEST_DAYS)
            arima_mape = safe_mape(test.values, arima_res["pred"]) if arima_res else np.nan
            arima_rmse = float(np.sqrt(mean_squared_error(test.values, arima_res["pred"]))) if arima_res else np.nan

        # Model selection
        if not prophet_res and arima_res:
            sel, sel_res = "arima", arima_res
        elif prophet_res and not prophet_weak:
            sel, sel_res = "prophet", prophet_res
        elif prophet_weak and arima_res:
            sel, sel_res = "arima", arima_res
        elif prophet_res:
            sel, sel_res = "prophet", prophet_res
        else:
            results.append(_make_row(sku, sku_meta, history_days, unique_sale_days,
                                     train, test, sales_cv, last_observed, gap_days,
                                     pre_gate_reason="model_failed_both",
                                     skipped=True,
                                     prophet_mape=prophet_mape, arima_mape=arima_mape))
            continue

        sel_mape  = safe_mape(test.values, sel_res["pred"])
        sel_rmse  = float(np.sqrt(mean_squared_error(test.values, sel_res["pred"])))
        sel_smape = smape(test.values, sel_res["pred"])

        # Next-day inference (reuse last point of the evaluation horizon — fast mode)
        next_pred  = float(sel_res["pred"][-1])
        next_lower = float(sel_res["lower"][-1])
        next_upper = float(sel_res["upper"][-1])
        iw = next_upper - next_lower
        ir = (iw / max(next_pred, 1e-8)) if next_pred > 0 else np.nan

        results.append({
            "sku":                  sku,
            "unique_sale_days":     unique_sale_days,
            "sales_cv":             sales_cv,
            "history_days":         history_days,
            "train_days":           len(train),
            "test_days":            len(test),
            "prophet_mape":         prophet_mape,
            "arima_mape":           arima_mape,
            "prophet_rmse":         prophet_rmse,
            "arima_rmse":           arima_rmse,
            "selected_model":       sel,
            "selected_mape":       sel_mape,
            "selected_rmse":        sel_rmse,
            "selected_smape":       sel_smape,
            "prophet_error":        prophet_err,
            "arima_error":          arima_err,
            "pre_gate_reason":      "model_routed",
            "last_observed_date":   last_observed.date(),
            "gap_days":             gap_days,
            "next_day_pred_qty":    next_pred,
            "next_day_lower":       next_lower,
            "next_day_upper":       next_upper,
            "next_day_interval_ratio": ir,
        })

    results_df = pd.DataFrame(results)
    if not results_df.empty:
        results_df = results_df.join(sku_meta, on="sku")
    return results_df


def _make_row(sku, sku_meta, history_days, unique_sale_days, train, test,
              sales_cv, last_observed, gap_days, pre_gate_reason,
              skipped=False, prophet_mape=np.nan, arima_mape=np.nan) -> dict:
    return {
        "sku":              sku,
        "unique_sale_days": unique_sale_days,
        "sales_cv":         sales_cv,
        "history_days":     history_days,
        "train_days":       len(train),
        "test_days":        len(test),
        "prophet_mape":     prophet_mape,
        "arima_mape":       arima_mape,
        "prophet_rmse":     np.nan,
        "arima_rmse":       np.nan,
        "selected_model":   "none",
        "selected_mape":    np.nan,
        "selected_rmse":    np.nan,
        "selected_smape":   np.nan,
        "prophet_error":    "skipped_by_pre_gate" if skipped else None,
        "arima_error":      "skipped_by_pre_gate" if skipped else None,
        "pre_gate_reason":  pre_gate_reason,
        "last_observed_date": last_observed.date() if hasattr(last_observed, "date") else last_observed,
        "gap_days":         gap_days,
        "next_day_pred_qty":        np.nan,
        "next_day_lower":           np.nan,
        "next_day_upper":           np.nan,
        "next_day_interval_ratio":  np.nan,
    }
