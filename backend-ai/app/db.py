"""
db.py — Supabase client singleton + typed helpers.

All helpers use the SERVICE ROLE key (bypasses RLS).
The frontend PWA reads daily_payloads directly via the anon key.
"""
from __future__ import annotations

import os
from datetime import date, datetime
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

_client: Optional[Client] = None


def get_client() -> Client:
    global _client
    if _client is None:
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_KEY"]
        _client = create_client(url, key)
    return _client


# ── accounts ──────────────────────────────────────────────────────────────────

def get_all_accounts() -> list[dict]:
    """Return all active accounts."""
    res = get_client().table("accounts").select("*").execute()
    return res.data or []


def get_account(account_id: str) -> Optional[dict]:
    res = get_client().table("accounts").select("*").eq("id", account_id).single().execute()
    return res.data


def update_missing_days(account_id: str, value: int) -> None:
    get_client().table("accounts").update({
        "consecutive_missing_days": value
    }).eq("id", account_id).execute()


def update_last_data_received(account_id: str) -> None:
    get_client().table("accounts").update({
        "last_data_received_at": datetime.utcnow().isoformat(),
        "consecutive_missing_days": 0,
    }).eq("id", account_id).execute()


# ── daily_payloads ────────────────────────────────────────────────────────────

def upsert_daily_payload(
    owner_id: str,
    run_date: date,
    payload: dict,
    confidence_tier: str,
) -> None:
    """Insert or overwrite the daily payload for a given owner + date."""
    get_client().table("daily_payloads").upsert({
        "owner_id":        owner_id,
        "date":            run_date.isoformat(),
        "payload":         payload,
        "confidence_tier": confidence_tier,
    }, on_conflict="owner_id,date").execute()


def get_latest_payload(owner_id: str) -> Optional[dict]:
    res = (
        get_client().table("daily_payloads")
        .select("*")
        .eq("owner_id", owner_id)
        .order("date", desc=True)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


# ── reminder_log ──────────────────────────────────────────────────────────────

def log_reminder(owner_id: str, escalation_level: int) -> None:
    """Escalation levels: 1=gentle, 2=downgrade+caveat, 3=force red + NGO alert."""
    get_client().table("reminder_log").insert({
        "owner_id":        owner_id,
        "escalation_level": escalation_level,
    }).execute()
