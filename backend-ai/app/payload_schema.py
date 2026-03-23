"""
payload_schema.py - Runtime JSON schema validation for PIVO payloads.

Single-source contract path (preferred):
    <repo_root>/contracts/payload_schema.json

This keeps backend output locked to the same schema consumed by delivery/PWA.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft7Validator


class PayloadSchemaError(ValueError):
    """Raised when payload does not satisfy the contract schema."""


@lru_cache(maxsize=1)
def _schema_path() -> Path:
    env_raw = os.environ.get("PAYLOAD_SCHEMA_PATH", "").strip()
    if env_raw:
        env_path = Path(env_raw)
        if not env_path.exists():
            raise FileNotFoundError(f"PAYLOAD_SCHEMA_PATH not found: {env_path}")
        return env_path

    here = Path(__file__).resolve()
    candidates = [
        here.parents[2] / "contracts" / "payload_schema.json",  # repo root (canonical)
        here.parents[1] / "contracts" / "payload_schema.json",  # backend-ai/contracts (optional)
        here.parents[1] / "contract" / "payload_schema.json",   # legacy fallback
    ]

    for c in candidates:
        if c.exists():
            return c

    looked = "\n".join(str(c) for c in candidates)
    raise FileNotFoundError(
        "Payload schema file not found. Checked:\n" + looked
    )


@lru_cache(maxsize=1)
def _validator() -> Draft7Validator:
    schema_file = _schema_path()
    with schema_file.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    return Draft7Validator(schema)


def validate_payload(payload: dict[str, Any], context: str = "payload") -> None:
    """
    Validate payload against the contract schema.

    Raises:
        PayloadSchemaError: if schema validation fails.
    """
    errors = sorted(_validator().iter_errors(payload), key=lambda e: list(e.path))
    if not errors:
        return

    formatted = []
    for err in errors[:8]:
        path = ".".join(str(p) for p in err.path) or "$"
        formatted.append(f"- {path}: {err.message}")

    raise PayloadSchemaError(
        f"Schema validation failed for {context} with {len(errors)} error(s):\n"
        + "\n".join(formatted)
    )
