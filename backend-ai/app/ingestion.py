"""
ingestion.py — Fetch raw sales data from Google Sheets or a local CSV.

Expected sheet columns (owner's input format):
    Tanggal | Nama Produk | Kategori | Jumlah Terjual | Harga Satuan | Jumlah Disiapkan (opsional)

Returns a normalised DataFrame with canonical columns:
    ds (date) | sku | sku_name | category | qty_sold | unit_price | qty_prepared
"""
from __future__ import annotations

import os
import re
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Canonical column names expected by the rest of the pipeline
CANONICAL_COLS = ["ds", "sku", "sku_name", "category", "qty_sold", "unit_price", "qty_prepared"]

# Mapping from common Google Sheets header variants → canonical names
_SHEET_COL_MAP = {
    "tanggal":          "ds",
    "date":             "ds",
    "nama produk":      "sku_name",
    "product name":     "sku_name",
    "produk":           "sku_name",
    "kategori":         "category",
    "category":         "category",
    "jumlah terjual":   "qty_sold",
    "qty sold":         "qty_sold",
    "qty":              "qty_sold",
    "harga satuan":     "unit_price",
    "unit price":       "unit_price",
    "harga":            "unit_price",
    "jumlah disiapkan": "qty_prepared",
    "qty prepared":     "qty_prepared",
}


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).lower()).strip("_")


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in _SHEET_COL_MAP.items() if k in df.columns})
    if "qty_prepared" not in df.columns:
        df["qty_prepared"] = None
    return df


def _clean_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    df["ds"] = pd.to_datetime(df["ds"], dayfirst=True, errors="coerce").dt.date
    df = df.dropna(subset=["ds", "sku_name", "qty_sold"])
    df["sku"]         = df["sku_name"].apply(_slugify)
    df["qty_sold"]    = pd.to_numeric(df["qty_sold"],    errors="coerce").fillna(0).astype(float)
    df["unit_price"]  = pd.to_numeric(df["unit_price"],  errors="coerce").fillna(0).astype(float)
    df["qty_prepared"] = pd.to_numeric(df["qty_prepared"], errors="coerce")
    df["category"]    = df.get("category", pd.Series("Lainnya", index=df.index)).fillna("Lainnya")
    return df[CANONICAL_COLS].copy()


# ── Google Sheets ─────────────────────────────────────────────────────────────

def fetch_from_sheets(sheet_id: str, since: Optional[date] = None) -> pd.DataFrame:
    """
    Fetch all rows from the owner's Google Sheet.
    Filters rows where ds >= since if provided.
    Requires GOOGLE_SA_JSON_PATH env var pointing to a service account JSON.
    """
    import gspread
    from google.oauth2.service_account import Credentials

    sa_path = os.environ.get("GOOGLE_SA_JSON_PATH")
    if not sa_path:
        raise EnvironmentError("GOOGLE_SA_JSON_PATH is not set")

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds  = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc     = gspread.authorize(creds)

    ws   = gc.open_by_key(sheet_id).sheet1
    rows = ws.get_all_records()
    df   = pd.DataFrame(rows)

    df = _normalise_columns(df)
    df = _clean_and_cast(df)

    if since:
        df = df[df["ds"] >= since]

    logger.info(f"Sheets fetch: {len(df)} rows from sheet {sheet_id!r}")
    return df.reset_index(drop=True)


# ── CSV fallback ──────────────────────────────────────────────────────────────

def fetch_from_csv(csv_path: str, since: Optional[date] = None) -> pd.DataFrame:
    """Load from a local CSV. Used for local dev and demo mode."""
    df = pd.read_csv(csv_path)
    df = _normalise_columns(df)
    df = _clean_and_cast(df)

    if since:
        df = df[df["ds"] >= since]

    logger.info(f"CSV fetch: {len(df)} rows from {csv_path!r}")
    return df.reset_index(drop=True)


# ── Public entry point ────────────────────────────────────────────────────────

def fetch_sales(account: dict, since: Optional[date] = None) -> pd.DataFrame:
    """
    Fetch sales data for an account. Tries Sheets first, falls back to CSV.
    DEV_CSV_PATH env var forces CSV mode regardless of Sheets config.
    """
    dev_csv = os.environ.get("DEV_CSV_PATH")
    if dev_csv:
        return fetch_from_csv(dev_csv, since=since)

    sa_path = os.environ.get("GOOGLE_SA_JSON_PATH")
    if sa_path and account.get("sheet_id"):
        return fetch_from_sheets(account["sheet_id"], since=since)

    raise EnvironmentError(
        "No data source configured. Set DEV_CSV_PATH or GOOGLE_SA_JSON_PATH + sheet_id."
    )
