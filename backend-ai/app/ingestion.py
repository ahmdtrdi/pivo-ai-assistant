"""
ingestion.py - Fetch raw sales data from Google Sheets or a local CSV.

Expected sheet columns (owner input):
    Tanggal | Nama Produk | Kategori | Jumlah Terjual | Harga Satuan | Jumlah Disiapkan (optional)

Returns a normalized DataFrame with canonical columns:
    ds (date) | sku | sku_name | category | qty_sold | unit_price | qty_prepared
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

CANONICAL_COLS = ["ds", "sku", "sku_name", "category", "qty_sold", "unit_price", "qty_prepared"]
REQUIRED_CANONICAL_INPUT_COLS = ["ds", "sku_name", "qty_sold", "unit_price"]

# Mapping from common Google Sheets/POS header variants -> canonical names
_SHEET_COL_MAP = {
    "tanggal": "ds",
    "date": "ds",
    "tanggal & waktu": "ds",
    "timestamp": "ds",
    "datetime": "ds",

    "nama produk": "sku_name",
    "product name": "sku_name",
    "product_name": "sku_name",
    "produk": "sku_name",

    "kategori": "category",
    "category": "category",
    "product_category": "category",

    "jumlah terjual": "qty_sold",
    "qty sold": "qty_sold",
    "qty": "qty_sold",
    "jumlah produk": "qty_sold",
    "quantity": "qty_sold",

    "harga satuan": "unit_price",
    "unit price": "unit_price",
    "unit_price": "unit_price",
    "harga": "unit_price",
    "harga produk": "unit_price",
    "price": "unit_price",

    "jumlah disiapkan": "qty_prepared",
    "qty prepared": "qty_prepared",
    "qty_prepared": "qty_prepared",
}


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).lower()).strip("_")


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize headers before applying mapping.
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in _SHEET_COL_MAP.items() if k in df.columns})
    if "qty_prepared" not in df.columns:
        df["qty_prepared"] = None
    return df


def _raise_missing_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_CANONICAL_INPUT_COLS if c not in df.columns]
    if not missing:
        return
    available = ", ".join(df.columns.tolist())
    raise ValueError(
        "Missing required columns after header mapping: "
        f"{missing}. Available columns: [{available}]"
    )


def _coerce_numeric(series: pd.Series) -> pd.Series:
    # Remove common formatting noise like currency symbols and thousand separators.
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.replace({"": None, "-": None, "None": None, "nan": None})
    cleaned = cleaned.str.replace(r"[^0-9,.-]", "", regex=True)
    cleaned = cleaned.str.replace(",", "", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")


def _raise_dtype_errors(df: pd.DataFrame) -> None:
    # Date validation
    raw_ds = df["ds"]
    parsed_ds = pd.to_datetime(raw_ds, dayfirst=True, errors="coerce")
    bad_ds = raw_ds[raw_ds.notna() & parsed_ds.isna()]
    if not bad_ds.empty:
        examples = bad_ds.astype(str).head(5).tolist()
        raise ValueError(
            f"Column 'ds' has {len(bad_ds)} invalid date value(s). Examples: {examples}"
        )

    # SKU name validation
    raw_sku_name = df["sku_name"]
    sku_name = raw_sku_name.astype(str).str.strip()
    bad_sku = raw_sku_name.isna() | sku_name.eq("") | sku_name.eq("nan")
    if bool(bad_sku.any()):
        raise ValueError(
            f"Column 'sku_name' has {int(bad_sku.sum())} empty/null value(s)."
        )

    # Numeric validation: qty_sold
    raw_qty = df["qty_sold"]
    qty = _coerce_numeric(raw_qty)
    bad_qty = raw_qty.notna() & qty.isna()
    if bool(bad_qty.any()):
        examples = raw_qty[bad_qty].astype(str).head(5).tolist()
        raise ValueError(
            f"Column 'qty_sold' has {int(bad_qty.sum())} non-numeric value(s). Examples: {examples}"
        )

    # Numeric validation: unit_price
    raw_price = df["unit_price"]
    price = _coerce_numeric(raw_price)
    bad_price = raw_price.notna() & price.isna()
    if bool(bad_price.any()):
        examples = raw_price[bad_price].astype(str).head(5).tolist()
        raise ValueError(
            f"Column 'unit_price' has {int(bad_price.sum())} non-numeric value(s). Examples: {examples}"
        )


def _clean_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    _raise_missing_columns(df)
    _raise_dtype_errors(df)

    out = df.copy()
    out["ds"] = pd.to_datetime(out["ds"], dayfirst=True, errors="coerce").dt.date
    out["sku_name"] = out["sku_name"].astype(str).str.strip()
    out["sku"] = out["sku_name"].apply(_slugify)

    out["qty_sold"] = _coerce_numeric(out["qty_sold"]).fillna(0).astype(float)
    out["unit_price"] = _coerce_numeric(out["unit_price"]).fillna(0).astype(float)
    out["qty_prepared"] = _coerce_numeric(out["qty_prepared"])

    out["category"] = out.get("category", pd.Series("Lainnya", index=out.index)).fillna("Lainnya")

    # Defensive final subset to expected schema order.
    return out[CANONICAL_COLS].copy()


# -- Google Sheets -------------------------------------------------------------

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
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc = gspread.authorize(creds)

    ws = gc.open_by_key(sheet_id).sheet1
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)

    df = _normalise_columns(df)
    df = _clean_and_cast(df)

    if since:
        df = df[df["ds"] >= since]

    logger.info(f"Sheets fetch: {len(df)} rows from sheet {sheet_id!r}")
    return df.reset_index(drop=True)


# -- CSV fallback --------------------------------------------------------------

def fetch_from_csv(csv_path: str, since: Optional[date] = None) -> pd.DataFrame:
    """Load from a local CSV. Used for local dev and demo mode."""
    df = pd.read_csv(csv_path)
    df = _normalise_columns(df)
    df = _clean_and_cast(df)

    if since:
        df = df[df["ds"] >= since]

    logger.info(f"CSV fetch: {len(df)} rows from {csv_path!r}")
    return df.reset_index(drop=True)


# -- Public entry point --------------------------------------------------------

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
