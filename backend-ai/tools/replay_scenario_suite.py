"""
Replay scenario validation runner.

Runs a small replay QA suite against the backend pipeline using:
- normal
- missing_input
- spike (anomaly proxy)

Outputs:
- outputs/replay_validation/<run_id>/summary.json
- outputs/replay_validation/<run_id>/summary.csv
- artifacts/ (payload + metrics + message per scenario run)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import pipeline
from app import payload_schema as payload_contract


SCENARIOS = ["normal", "missing_input", "spike"]
ALLOWED_TIERS = {"green", "yellow", "red"}
ENV_KEYS = [
    "DEV_MODE",
    "DEV_CSV_PATH",
    "REPLAY_MODE",
    "REPLAY_CURSOR_DATE",
    "REPLAY_STRICT_NO_FUTURE",
    "REPLAY_SCENARIO",
    "REPLAY_SPIKE_MULTIPLIER",
    "REPLAY_DROP_MULTIPLIER",
    "REPLAY_ARTIFACTS_ENABLED",
    "REPLAY_ARTIFACTS_DIR",
    "MAX_ACTIVE_SKUS",
    "TEST_DAYS",
    "GEMINI_API_KEY",
    "GEMINI_MODEL",
]


def _sum_forecast(payload: dict[str, Any]) -> float:
    items = payload.get("forecasts", []) or []
    return float(sum(float(x.get("qty_mid", 0.0) or 0.0) for x in items))


def _worst_tier(tiers: list[str]) -> str:
    if "red" in tiers:
        return "red"
    if "yellow" in tiers:
        return "yellow"
    return "green"


def _payload_signature(payload: dict[str, Any]) -> dict[str, list[str]]:
    def _keys_union(items: list[dict[str, Any]]) -> list[str]:
        if not items:
            return []
        keys: set[str] = set()
        for obj in items:
            if isinstance(obj, dict):
                keys.update(obj.keys())
        return sorted(keys)

    forecasts = payload.get("forecasts", []) or []
    skipped = payload.get("skipped_skus", []) or []
    profits = payload.get("profit_analysis", []) or []
    return {
        "top_level_keys": sorted(payload.keys()),
        "forecast_keys": _keys_union(forecasts),
        "skipped_keys": _keys_union(skipped),
        "profit_keys": _keys_union(profits),
    }


def _sig_mismatch_errors(
    *,
    baseline: dict[str, list[str]],
    current: dict[str, list[str]],
    baseline_scenario: str,
    scenario: str,
) -> list[str]:
    errors: list[str] = []
    for key in ("top_level_keys", "forecast_keys", "skipped_keys", "profit_keys"):
        b = baseline.get(key, [])
        c = current.get(key, [])
        # If both empty, they're equivalent.
        if not b and not c:
            continue
        if b != c:
            errors.append(
                f"{key} mismatch vs {baseline_scenario} in {scenario} "
                f"(baseline={b}, current={c})"
            )
    return errors


def _artifact_day_dir(artifacts_dir: Path, owner_id: str, run_date: date) -> Path:
    return artifacts_dir / owner_id / run_date.isoformat()


def _snapshot_artifact_dirs(day_dir: Path) -> set[str]:
    if not day_dir.exists():
        return set()
    return {p.name for p in day_dir.iterdir() if p.is_dir()}


def _resolve_latest_new_run_dir(day_dir: Path, before: set[str]) -> Path | None:
    if not day_dir.exists():
        return None
    dirs = [p for p in day_dir.iterdir() if p.is_dir()]
    if not dirs:
        return None
    new_dirs = [p for p in dirs if p.name not in before]
    if new_dirs:
        return sorted(new_dirs, key=lambda p: p.name)[-1]
    return sorted(dirs, key=lambda p: p.name)[-1]


def _load_results_df(run_dir: Path | None) -> pd.DataFrame:
    if run_dir is None:
        return pd.DataFrame()
    csv_path = run_dir / "forecast_results.csv"
    if not csv_path.exists():
        return pd.DataFrame()
    return pd.read_csv(csv_path)


def _check_tier_consistency(
    *,
    payload: dict[str, Any],
    results_df: pd.DataFrame,
) -> tuple[str, list[str], dict[str, Any]]:
    errors: list[str] = []
    diag: dict[str, Any] = {}
    payload_tier = str(payload.get("confidence_tier", ""))
    diag["payload_tier"] = payload_tier

    if payload_tier not in ALLOWED_TIERS:
        errors.append(f"invalid payload confidence_tier={payload_tier!r}")

    if not results_df.empty and {"today_tier", "today_gap_days", "unique_sale_days"}.issubset(results_df.columns):
        result_tiers = [str(t) for t in results_df["today_tier"].dropna().tolist()]
        if result_tiers:
            expected = _worst_tier(result_tiers)
            diag["expected_tier_from_results"] = expected
            if payload_tier in ALLOWED_TIERS and payload_tier != expected:
                errors.append(
                    f"payload confidence_tier={payload_tier} "
                    f"!= worst results_df today_tier={expected}"
                )

        bad_recency = int(
            ((pd.to_numeric(results_df["today_gap_days"], errors="coerce") > 14)
             & (results_df["today_tier"].astype(str) != "red")).sum()
        )
        bad_gate1_red = int(
            ((pd.to_numeric(results_df["unique_sale_days"], errors="coerce") < 7)
             & (results_df["today_tier"].astype(str) != "red")).sum()
        )
        bad_gate1_yellow = int(
            ((pd.to_numeric(results_df["unique_sale_days"], errors="coerce").between(7, 20, inclusive="both"))
             & (results_df["today_tier"].astype(str) == "green")).sum()
        )

        diag["bad_recency_rows"] = bad_recency
        diag["bad_gate1_red_rows"] = bad_gate1_red
        diag["bad_gate1_yellow_rows"] = bad_gate1_yellow

        if bad_recency > 0:
            errors.append(f"found {bad_recency} row(s) with today_gap_days>14 but tier!=red")
        if bad_gate1_red > 0:
            errors.append(f"found {bad_gate1_red} row(s) with unique_sale_days<7 but tier!=red")
        if bad_gate1_yellow > 0:
            errors.append(f"found {bad_gate1_yellow} row(s) with 7<=unique_sale_days<=20 but tier=green")

    if payload.get("model_routing") == "prophet_first_arima_fallback":
        forecast_tiers = [str(x.get("tier", "")) for x in (payload.get("forecasts", []) or [])]
        skipped_count = len(payload.get("skipped_skus", []) or [])
        if skipped_count > 0:
            forecast_tiers.extend(["red"] * skipped_count)
        if forecast_tiers:
            expected_payload_tier = _worst_tier(forecast_tiers)
            diag["expected_tier_from_payload_items"] = expected_payload_tier
            if payload_tier in ALLOWED_TIERS and payload_tier != expected_payload_tier:
                errors.append(
                    f"payload confidence_tier={payload_tier} "
                    f"!= worst tier derived from forecasts/skipped={expected_payload_tier}"
                )

    return ("pass" if not errors else "fail"), errors, diag


def _prepare_env(
    csv_path: Path,
    run_date: date,
    artifact_dir: Path,
    *,
    max_active_skus: int,
    test_days: int,
) -> dict[str, str | None]:
    backup = {k: os.environ.get(k) for k in ENV_KEYS}
    os.environ["DEV_MODE"] = "true"
    os.environ["DEV_CSV_PATH"] = str(csv_path)
    os.environ["REPLAY_MODE"] = "true"
    os.environ["REPLAY_CURSOR_DATE"] = run_date.isoformat()
    os.environ["REPLAY_STRICT_NO_FUTURE"] = "true"
    os.environ["REPLAY_ARTIFACTS_ENABLED"] = "true"
    os.environ["REPLAY_ARTIFACTS_DIR"] = str(artifact_dir)
    os.environ["MAX_ACTIVE_SKUS"] = str(max_active_skus)
    os.environ["TEST_DAYS"] = str(test_days)
    # Force deterministic local fallback (no network wait for Gemini).
    os.environ["GEMINI_API_KEY"] = ""
    os.environ["GEMINI_MODEL"] = "gemini-2.0-flash"
    return backup


def _restore_env(backup: dict[str, str | None]) -> None:
    for k, v in backup.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def _override_forecasting_runtime(
    *,
    max_active_skus: int,
    test_days: int,
) -> dict[str, Any]:
    """
    Override forecasting module constants at runtime.

    Forecasting currently reads env vars at import-time, so this makes profile
    switches (fast vs full) deterministic inside one Python process.
    """
    mod = pipeline.forecasting
    backup = {
        "MAX_ACTIVE_SKUS": getattr(mod, "MAX_ACTIVE_SKUS", None),
        "TEST_DAYS": getattr(mod, "TEST_DAYS", None),
    }
    mod.MAX_ACTIVE_SKUS = int(max_active_skus)
    mod.TEST_DAYS = int(test_days)
    return backup


def _restore_forecasting_runtime(backup: dict[str, Any]) -> None:
    mod = pipeline.forecasting
    if "MAX_ACTIVE_SKUS" in backup:
        mod.MAX_ACTIVE_SKUS = backup["MAX_ACTIVE_SKUS"]
    if "TEST_DAYS" in backup:
        mod.TEST_DAYS = backup["TEST_DAYS"]


def run_suite(
    *,
    csv_path: Path,
    run_date: date,
    owner_id: str,
    out_root: Path,
    max_active_skus: int,
    test_days: int,
) -> dict[str, Any]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = out_root / run_id
    artifacts_dir = out_dir / "artifacts"
    out_dir.mkdir(parents=True, exist_ok=True)

    backup = _prepare_env(
        csv_path,
        run_date,
        artifacts_dir,
        max_active_skus=max_active_skus,
        test_days=test_days,
    )
    forecasting_backup = _override_forecasting_runtime(
        max_active_skus=max_active_skus,
        test_days=test_days,
    )
    rows: list[dict[str, Any]] = []
    payload_baseline_sig: dict[str, list[str]] | None = None
    payload_baseline_scenario = ""

    account = {
        "id": owner_id,
        "name": "Replay Validation Owner",
        "whatsapp_number": "0812000000",
        "sheet_id": "",
        "consecutive_missing_days": 0,
        "last_data_received_at": None,
        "use_real_cogs": False,
    }

    baseline_payload: dict[str, Any] | None = None

    try:
        for scenario in SCENARIOS:
            os.environ["REPLAY_SCENARIO"] = scenario
            row: dict[str, Any] = {
                "scenario": scenario,
                "status": "pass",
                "error": "",
                "confidence_tier": None,
                "model_routing": None,
                "forecast_count": 0,
                "missing_days": None,
                "forecast_sum": None,
                "max_active_skus": int(pipeline.forecasting.MAX_ACTIVE_SKUS),
                "test_days": int(pipeline.forecasting.TEST_DAYS),
            }
            try:
                day_dir = _artifact_day_dir(artifacts_dir, owner_id, run_date)
                before_dirs = _snapshot_artifact_dirs(day_dir)
                payload = pipeline.run_owner(deepcopy(account), run_date=run_date)
                errors: list[str] = []

                # Contract validation (explicit check inside suite).
                try:
                    payload_contract.validate_payload(payload, context=f"suite_{scenario}")
                    row["payload_schema_status"] = "pass"
                except Exception as exc:  # noqa: BLE001
                    row["payload_schema_status"] = "fail"
                    errors.append(f"payload schema invalid: {type(exc).__name__}: {exc}")

                tier = payload.get("confidence_tier")
                if tier not in ALLOWED_TIERS:
                    errors.append(f"invalid confidence_tier={tier!r}")

                msg = payload.get("wa_message", "")
                if not isinstance(msg, str) or msg.strip() == "":
                    errors.append("wa_message is empty")

                run_dir = _resolve_latest_new_run_dir(day_dir, before_dirs)
                results_df = _load_results_df(run_dir)
                row["artifact_run_dir"] = str(run_dir) if run_dir is not None else ""
                row["results_rows"] = int(len(results_df))

                tier_status, tier_errors, tier_diag = _check_tier_consistency(
                    payload=payload,
                    results_df=results_df,
                )
                row["tier_consistency_status"] = tier_status
                row["tier_consistency_notes"] = "; ".join(tier_errors)
                row["tier_payload_expected_from_results"] = tier_diag.get("expected_tier_from_results")
                row["tier_payload_expected_from_payload_items"] = tier_diag.get(
                    "expected_tier_from_payload_items"
                )
                row["tier_bad_recency_rows"] = tier_diag.get("bad_recency_rows")
                row["tier_bad_gate1_red_rows"] = tier_diag.get("bad_gate1_red_rows")
                row["tier_bad_gate1_yellow_rows"] = tier_diag.get("bad_gate1_yellow_rows")
                if tier_errors:
                    errors.extend(tier_errors)

                payload_sig = _payload_signature(payload)
                row["payload_top_keys"] = "|".join(payload_sig["top_level_keys"])
                row["payload_forecast_keys"] = "|".join(payload_sig["forecast_keys"])
                row["payload_skipped_keys"] = "|".join(payload_sig["skipped_keys"])
                row["payload_profit_keys"] = "|".join(payload_sig["profit_keys"])

                if payload_baseline_sig is None:
                    payload_baseline_sig = payload_sig
                    payload_baseline_scenario = scenario
                    row["payload_stability_status"] = "baseline"
                    row["payload_stability_notes"] = ""
                else:
                    sig_errors = _sig_mismatch_errors(
                        baseline=payload_baseline_sig,
                        current=payload_sig,
                        baseline_scenario=payload_baseline_scenario,
                        scenario=scenario,
                    )
                    if sig_errors:
                        row["payload_stability_status"] = "fail"
                        row["payload_stability_notes"] = "; ".join(sig_errors)
                        errors.extend(sig_errors)
                    else:
                        row["payload_stability_status"] = "pass"
                        row["payload_stability_notes"] = ""

                row["confidence_tier"] = tier
                row["model_routing"] = payload.get("model_routing")
                row["forecast_count"] = len(payload.get("forecasts", []) or [])
                row["missing_days"] = payload.get("consecutive_missing_days")
                row["forecast_sum"] = _sum_forecast(payload)

                if scenario == "missing_input":
                    md = int(payload.get("consecutive_missing_days", 0) or 0)
                    if md < 1:
                        errors.append("missing_input did not raise missing_days >= 1")

                if scenario == "normal":
                    baseline_payload = payload

                if scenario == "spike" and baseline_payload is not None:
                    baseline_sum = _sum_forecast(baseline_payload)
                    row["baseline_forecast_sum"] = baseline_sum
                    # Soft sanity check: spike run should produce valid output;
                    # we record delta but don't hard-fail on direction.
                    row["forecast_delta_vs_normal"] = row["forecast_sum"] - baseline_sum

                if errors:
                    row["status"] = "fail"
                    row["error"] = "; ".join(errors)
            except Exception as exc:  # noqa: BLE001
                row["status"] = "fail"
                row["error"] = f"{type(exc).__name__}: {exc}"
                row["trace"] = traceback.format_exc(limit=3)

            rows.append(row)
    finally:
        _restore_forecasting_runtime(forecasting_backup)
        _restore_env(backup)

    overall_status = "pass" if all(r["status"] == "pass" for r in rows) else "fail"
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall_status,
        "csv_path": str(csv_path),
        "run_date": run_date.isoformat(),
        "owner_id": owner_id,
        "scenarios": rows,
        "output_dir": str(out_dir),
    }

    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    pd.DataFrame(rows).to_csv(out_dir / "summary.csv", index=False)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run replay scenario QA suite")
    parser.add_argument("--csv", required=True, help="CSV path for replay validation")
    parser.add_argument("--run-date", required=True, help="Cursor date YYYY-MM-DD")
    parser.add_argument("--owner-id", default="demo")
    parser.add_argument("--out-dir", default="outputs/replay_validation")
    parser.add_argument("--max-active-skus", type=int, default=6)
    parser.add_argument("--test-days", type=int, default=14)
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        parser.error(f"--csv file not found: {csv_path}")

    try:
        run_date = date.fromisoformat(args.run_date)
    except ValueError:
        parser.error("--run-date must be YYYY-MM-DD")
        return

    out_root = Path(args.out_dir)
    summary = run_suite(
        csv_path=csv_path,
        run_date=run_date,
        owner_id=args.owner_id,
        out_root=out_root,
        max_active_skus=args.max_active_skus,
        test_days=args.test_days,
    )

    print(f"Replay scenario suite: {summary['overall_status']}")
    print(f"Output: {summary['output_dir']}")


if __name__ == "__main__":
    main()
