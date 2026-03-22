"""
delivery.py — WhatsApp delivery via Fonnte API.

Real sends only happen when FONNTE_DRY_RUN != "true".
A delivery failure is always caught and logged — it should never crash the pipeline.
"""
from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)

FONNTE_BASE_URL = "https://api.fonnte.com/send"


def send_wa(phone: str, message: str) -> bool:
    """
    Send a WhatsApp message via Fonnte.

    Returns True on success, False on failure (error is logged, not raised).
    In dry-run mode, just logs and returns True.
    """
    dry_run = os.environ.get("FONNTE_DRY_RUN", "true").lower() == "true"

    if dry_run:
        logger.info(f"[DRY RUN] WA to {phone}:\n{message[:120]}...")
        return True

    token = os.environ.get("FONNTE_TOKEN")
    if not token:
        logger.error("FONNTE_TOKEN not set — cannot send WA message")
        return False

    try:
        resp = requests.post(
            FONNTE_BASE_URL,
            headers={"Authorization": token},
            data={
                "target":  phone,
                "message": message,
                "typing":  "true",
                "delay":   "2",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status"):
            logger.info(f"WA sent to {phone} — status: {data.get('status')}")
            return True
        else:
            logger.warning(f"Fonnte returned non-OK status for {phone}: {data}")
            return False
    except Exception as e:
        logger.error(f"WA delivery failed for {phone}: {e}")
        return False
