# PIVO Data Flow (Real Ops + PoC Modes)

Last updated: 2026-03-23

This document explains how sales data enters the PIVO backend in real deployment, while keeping a practical toggle for PoC/manual testing and replay demos.

## 1) Recommended Real Flow (Production-Oriented)
1. POS exports daily sales as CSV.
2. CSV is uploaded to a fixed Google Drive folder.
3. Google Apps Script reads new CSV files and appends rows to one master Google Sheet tab (`raw_sales`).
4. Backend pulls the Google Sheet nightly via Google Sheets API.
5. Pipeline runs: forecasting -> confidence -> LLM -> delivery.
6. Final JSON payload is persisted and delivered (WA + PWA).

## 2) Sheet Operating Rules
- One Google Sheet per outlet (or per owner if single outlet).
- One append-only tab for raw rows (`raw_sales`).
- Keep a stable header row; do not rename columns casually.
- Do not create a new Sheet file every day.
- Append rows only; do not overwrite old rows.

## 3) Dedup and Data Hygiene
- Add/use a dedup key when appending:
  - Preferred: `transaction_id` from POS.
  - Fallback: composite key (`date + receipt_id + sku`).
- If CSV is accidentally re-uploaded, dedup logic should prevent duplicate rows.

## 4) Input Modes (Toggle Strategy)

### Mode A: Auto-Import to Sheet (target default for real rollout)
- Source: POS CSV -> Drive folder -> Apps Script append -> Sheet API pull.
- Best for non-technical owners (no spreadsheet operations needed).

### Mode B: Manual PoC CSV (already supported)
- Source: local CSV file passed directly to backend.
- Current toggle:
  - CLI: `python -m app.pipeline --csv <path>`
  - Env: `DEV_CSV_PATH=<path>`
- Use this for backend development and quick smoke tests.

### Mode C: Replay Demo Mode (implemented baseline)
- Source: frozen historical dataset + simulated date cursor.
- Goal: demo tomorrow/next-week behavior without new live data input.
- Current controls:
  - Env: `REPLAY_MODE=true`, `REPLAY_CURSOR_DATE=YYYY-MM-DD`
  - CLI: `--replay-mode --run-date YYYY-MM-DD`
  - Day-step: `--replay-mode --step-days 1` or `--replay-mode --step-days 7`
- Scenario toggles:
  - `normal`: baseline replay behavior
  - `missing_input`: force `has_new_data=false` to test reminder/escalation branch
  - `spike`: multiply `qty_sold` on cursor date (`REPLAY_SPIKE_MULTIPLIER`, default 2.0)
  - `drop`: reduce `qty_sold` on cursor date (`REPLAY_DROP_MULTIPLIER`, default 0.3)
- Cursor state is persisted in JSON (`REPLAY_CURSOR_STATE_PATH` or default `backend-ai/outputs/replay_cursor_state.json`).
- Strict no-future-data guard: rows with `ds > cursor_date` are dropped before processing.
- Current artifact output path (when enabled): `backend-ai/outputs/replay_artifacts/<owner>/<date>/<run_id>/`
- Files written per run: `payload.json`, `metrics.json`, `message.txt`, `tier.txt`, optional `forecast_results.csv`.

## 5) Recommendation for Pilot
- During pilot phase:
  - Keep Mode B (manual CSV) for engineering validation.
  - Introduce Mode A with a simple Apps Script importer for facilitator/owner operations.
  - Add Mode C for demos and stakeholder walkthroughs.

This phased approach keeps architecture aligned while reducing operator burden for MSMEs.
