# Prototype Readiness Checklist

Use this checklist before moving prototype logic into full application development.

## 1) Architecture Guardrails
- [ ] Keep architecture close to plan: forecasting -> confidence -> LLM -> delivery.
- [ ] Critique architecture assumptions when metrics/data prove a weakness.
- [ ] Keep fast-dev mode optional, never default for production behavior.

## 2) Data Contract
- [ ] Freeze core input schema (`date`, `outlet_id`, `sku`, `qty_sold`, `sales`).
- [ ] Validate required columns and dtypes at ingestion.
- [ ] Define missing/null behavior per field.
- [ ] Version the contract whenever field names change.

## 3) Replay Mode (Dev and QA)
- [ ] Use a frozen historical dataset (read-only).
- [ ] Implement simulated date cursor with strict no-future-data access.
- [ ] Support day-step controls (`+1 day`, `+7 days`).
- [ ] Support scenario toggles (normal, missing input, spike, drop).
- [ ] Store daily replay artifacts (metrics, payload, message, tier).

## 4) Cold Start Strategy
- [ ] Provide generalized baseline path for new MSMEs (no long wait with zero value).
- [ ] Gate recommendations by confidence during early period.
- [ ] Transition from generalized to local model once local data is sufficient.
- [ ] Keep explicit fallback output for low-data SKUs (`no prediction` or coarse estimate).

## 5) Forecasting Logic
- [ ] Keep Prophet primary and ARIMA fallback routing.
- [ ] Define sparse-SKU path (intermittent method or aggregate fallback).
- [ ] Ensure no leakage in train/test split and replay mode.
- [ ] Log chosen model per SKU, every run.
- [ ] Run full mode benchmarks after fast-dev iterations stabilize.

## 6) Confidence Tier System
- [ ] Align tier gates with `context/pivo_table_appendices.html`.
- [ ] Enforce recency rule consistently (`last sale > 14 days` force red).
- [ ] Keep missing-data policy aligned (1-2 no downgrade, 3-6 downgrade, 7+ red).
- [ ] Record machine-readable tier reasons (`tier_reason` fields).

## 7) LLM Explanation Layer
- [ ] LLM consumes structured payload only.
- [ ] Message tone follows tier (green/yellow/red) with hard constraints.
- [ ] Prevent hallucinated numbers (only payload-driven values allowed).
- [ ] Keep template fallback if LLM API is unavailable.

## 8) Payload and Delivery
- [ ] Lock payload schema used by backend + PWA + WhatsApp.
- [ ] Save JSON artifacts every run for traceability.
- [ ] Include skipped-SKU explanation in payload.
- [ ] Define retry and failure handling for delivery channel outages.

## 9) Metrics and Tracking
- [ ] Auto-log each run to `experiments/poc_metrics_tracker.csv`.
- [ ] Track both model metrics and business metrics.
- [ ] Model metrics: MAPE, RMSE, SMAPE (plus sparse-safe metrics if needed).
- [ ] Business metrics: stockout rate, waste, margin trend, action adoption.
- [ ] Separate fast-dev run status from full benchmark status.

## 10) Ops and Safety
- [ ] Define daily runtime budget and compute limits.
- [ ] Add monitoring for schema drift, data staleness, and accuracy degradation.
- [ ] Add human override path and capture override reasons.
- [ ] Define incident behavior when model/LLM/delivery components fail.

## 11) Go / No-Go Gates for Real App Integration
- [ ] Tier logic is consistent (no contradictory gate outcomes).
- [ ] Replay scenarios pass (normal + missing + anomaly).
- [ ] JSON payload contract stable for both frontend and backend.
- [ ] Fast-dev and full-mode both run successfully.
- [ ] Team agrees on launch thresholds for both model and business KPIs.

