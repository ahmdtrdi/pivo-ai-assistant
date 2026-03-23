# BE-AI-DEVLOG

## 2026-03-22 — Checklist State Marked Against Current Backend

### The Change
- Updated `backend-ai/PROTOTYPE_READINESS_CHECKLIST.md` with checked/unchecked status based on actual implementation in `backend-ai/app/*`.
- Added a review date marker so status is traceable to this snapshot.

### The Reasoning
- Marked only items that are clearly implemented in code (routing, tier gates, fallback behavior, payload persistence, failure isolation).
- Left contract lock, replay controls, generalized cold-start, and metrics automation unchecked because they are not fully implemented yet.

### The Tech Debt
- No runtime payload schema validation yet.
- No replay cursor/scenario framework yet.
- No run-level metrics tracker automation yet.
- Delivery has failure catch but no retry policy.

## 2026-03-23 — Ingestion Schema Validation Patch

### The Change
- Updated `backend-ai/app/ingestion.py` to add explicit required-column validation and dtype validation for `ds`, `sku_name`, `qty_sold`, and `unit_price`.
- Expanded header alias mapping for common CSV/POS variants (including `timestamp`, `product_name`, `jumlah produk`, and `harga produk`).
- Marked `Validate required columns and dtypes at ingestion` as complete in `backend-ai/PROTOTYPE_READINESS_CHECKLIST.md`.

### The Reasoning
- We now fail fast with clear, actionable error messages when upstream data shape/type is invalid.
- This avoids silent coercion to zeros that can poison downstream forecasts and confidence tiers.
- Added alias coverage so smoke-test CSVs are accepted with less manual renaming.

### The Tech Debt
- Validation is strict and may reject files with a few noisy rows; later we may want a warn-and-quarantine mode.
- Locale-specific decimal format handling is still simplified (commas are treated as thousand separators).

## 2026-03-23 — Payload Contract Lock (Schema Validation)

### The Change
- Added `backend-ai/app/payload_schema.py` to load and validate payloads against `contracts/payload_schema.json`.
- Wired schema validation into `backend-ai/app/pipeline.py` for:
  - missing-data payload,
  - partial payload before LLM call,
  - final payload before delivery/persistence.
- Updated `backend-ai/requirements.txt` to include `jsonschema==4.23.0`.
- Marked checklist item `Lock payload schema used by backend + PWA + WhatsApp` as complete.

### The Reasoning
- Figure 1 architecture treats payload as channel-agnostic contract output; runtime schema validation enforces that single source of truth.
- Validating before send/save prevents accidental payload drift from silently reaching WhatsApp or PWA consumers.

### The Tech Debt
- Validation currently fails hard; we may later add a guarded fallback payload for schema exceptions.
- Contract versioning strategy is still not implemented.

### Patch Note (same day)
- Fixed `payload_schema.py` env-path handling so `PAYLOAD_SCHEMA_PATH` is only used when explicitly set (avoids accidental `.` resolution).

## 2026-03-23 — Data Flow Documentation + Mode Toggle Checklist

### The Change
- Added `backend-ai/docs/DATA_FLOW.md` documenting real ingestion operations and PoC/demo mode toggles.
- Added `Data Flow Reference` section to `backend-ai/BE-README.md` linking the new doc.
- Extended `backend-ai/PROTOTYPE_READINESS_CHECKLIST.md` with a new section:
  - `12) Data Ingestion Modes (Ops Practicality)`.

### The Reasoning
- Real rollout needs a practical operator flow that does not require MSME owners to do manual spreadsheet appends.
- The new checklist section makes mode strategy explicit: auto-import for operations, manual CSV for engineering PoC, replay for demos.

### The Tech Debt
- Auto-import (Drive -> Sheet append) and replay toggle are documented but not implemented yet.
- Mode selection is not yet enforced as an explicit per-owner runtime setting.

## 2026-03-23 — Replay Artifact Logging (Checklist Item 3)

### The Change
- Added `backend-ai/app/replay_artifacts.py` to persist per-run artifacts:
  - `payload.json`,
  - `metrics.json`,
  - `message.txt`,
  - `tier.txt`,
  - optional `forecast_results.csv`.
- Wired artifact logging into `backend-ai/app/pipeline.py` for:
  - normal successful runs,
  - missing-data skip runs (`missing >= 7`).
- Added `.env` toggles in `backend-ai/.env.example`:
  - `REPLAY_ARTIFACTS_ENABLED`,
  - `REPLAY_ARTIFACTS_DIR`.
- Marked checklist item `Store daily replay artifacts (metrics, payload, message, tier)` as complete.
- Updated `backend-ai/docs/DATA_FLOW.md` with artifact output path and file list.

### The Reasoning
- Replay and smoke-test validation needs deterministic artifacts that can be reviewed day by day.
- Persisting both payload and compact metrics reduces debugging friction and supports demo narratives.
- Default behavior ties artifact writing to `DEV_MODE` unless explicitly overridden.

### The Tech Debt
- We still need a true replay cursor/day-step engine (artifacts are now available, but replay controls are separate).
- Artifact retention policy (cleanup/rotation) is not implemented yet.

### Patch Note (same day)
- Cleaned unused imports in `backend-ai/app/pipeline.py` (`timedelta`, `numpy`) during artifact integration.

## 2026-03-23 — Replay Cursor + No-Future-Data Guard

### The Change
- Updated `backend-ai/app/pipeline.py` to support a simulated replay cursor date with strict no-future-data filtering.
- Added runtime env parsing helpers (instead of import-time constants) for:
  - `DEV_MODE`,
  - `REPLAY_MODE`,
  - `REPLAY_STRICT_NO_FUTURE`.
- Added cursor resolution logic:
  - explicit `run_date` argument wins,
  - else `REPLAY_CURSOR_DATE` when `REPLAY_MODE=true`,
  - else `date.today()`.
- Added no-future guard to drop rows where `ds > cursor_date` during replay/strict mode.
- Added CLI flags in `pipeline.py`:
  - `--replay-mode`,
  - `--run-date YYYY-MM-DD`.
- Updated `backend-ai/.env.example` with replay controls.
- Updated docs/checklist:
  - checked `Implement simulated date cursor with strict no-future-data access`,
  - checked `Implement replay mode toggle (demo without live input)`.

### The Reasoning
- Replay mode requires deterministic time travel behavior; the model must never see future rows relative to cursor date.
- Runtime env parsing removes a subtle bug where CLI env overrides were not reflected because constants were read too early.

### The Tech Debt
- Day-step controls (`+1 day`, `+7 days`) are still not implemented yet.
- Scenario toggles (normal/missing/spike/drop) are still pending.

### Patch Note (same day)
- Corrected `backend-ai/BE-README.md` local/replay CSV command examples to use `data/Serayu_Chicken_60k.csv`.

## 2026-03-23 — Replay Day-Step Controls (+1 / +7)

### The Change
- Extended `backend-ai/app/pipeline.py` replay tooling with day-step controls:
  - `--step-days 1`
  - `--step-days 7`
- Added replay cursor state persistence:
  - `_cursor_state_path()`, `_load_cursor_state()`, `_save_cursor_state()`
  - default state file: `backend-ai/outputs/replay_cursor_state.json`
  - optional override: `REPLAY_CURSOR_STATE_PATH` / `--cursor-state-path`
- Added replay env/docs updates:
  - `.env.example` now includes `REPLAY_CURSOR_STATE_PATH`
  - docs/readme include day-step usage examples.
- Marked checklist item `Support day-step controls (+1 day, +7 days)` as complete.

### The Reasoning
- Replay demos need deterministic, low-friction progression without manually editing cursor dates every run.
- Persisted cursor state enables quick iterative walkthroughs while preserving no-future-data guarantees.

### The Tech Debt
- Scenario toggles (normal/missing/spike/drop) are still not implemented.
- Cursor state currently stores one global replay date; per-owner cursor state may be needed later.

## 2026-03-23 — Replay Scenario Toggles (normal/missing/spike/drop)

### The Change
- Added replay scenario support in `backend-ai/app/pipeline.py`:
  - `normal`,
  - `missing_input`,
  - `spike`,
  - `drop`.
- Added scenario application hook after no-future filtering and before `has_new_data` evaluation.
- Added CLI flag:
  - `--scenario normal|missing_input|spike|drop`
- Added env configuration in `.env.example`:
  - `REPLAY_SCENARIO`,
  - `REPLAY_SPIKE_MULTIPLIER`,
  - `REPLAY_DROP_MULTIPLIER`.
- Updated replay metrics (`backend-ai/app/replay_artifacts.py`) to log:
  - `replay_mode`,
  - `replay_scenario`.
- Updated docs/readme examples and marked checklist scenario toggle item complete.

### The Reasoning
- Replay demos and QA now can exercise key branches without hand-editing data files.
- `missing_input` directly tests reminder/escalation behavior.
- `spike` and `drop` provide fast anomaly-like demand shocks on the cursor day.

### The Tech Debt
- Scenario effects are currently day-local (cursor date only); multi-day scenario windows may be needed later.
- We still need pass/fail automation for replay scenario test cases.

## 2026-03-23 — Replay Scenario Validation Gate Closed

### The Change
- Ran replay scenario smoke validation with the Serayu dataset using:
  - `python tools/replay_scenario_suite.py --csv data/Serayu_Chicken_60k.csv --run-date 2026-01-10 --owner-id demo --max-active-skus 6 --test-days 14`
- Validation output generated at:
  - `backend-ai/outputs/replay_validation/20260323T014150Z/summary.json`
  - `backend-ai/outputs/replay_validation/20260323T014150Z/summary.csv`
- Updated `backend-ai/PROTOTYPE_READINESS_CHECKLIST.md`:
  - checked `Replay scenarios pass (normal + missing + anomaly)`.
  - refreshed `Last reviewed` date to `2026-03-23`.

### The Reasoning
- This closes the key replay QA gate before frontend-less smoke testing, with reproducible artifacts for all scenarios (`normal`, `missing_input`, `spike`).
- Using reduced dev limits (`max_active_skus`, `test_days`) keeps validation fast while still exercising routing, confidence, and payload generation paths.

### The Tech Debt
- Current anomaly coverage uses `spike` as proxy; adding `drop` into formal pass/fail suite would improve anomaly completeness.
- We still need remaining go/no-go gates (tier consistency, payload stability sign-off, fast-dev vs full-mode parity, KPI threshold agreement).

## 2026-03-23 — Tier Consistency + Payload Stability Gates (Combined)

### The Change
- Extended `backend-ai/tools/replay_scenario_suite.py` to validate two additional go/no-go gates in one run:
  - Tier consistency gate:
    - payload `confidence_tier` must match worst SKU tier from `forecast_results.csv`
    - recency guard consistency (`today_gap_days > 14` => tier must be `red`)
    - gate1 consistency (`unique_sale_days < 7` => `red`, `7..20` cannot be `green`)
    - payload-level tier cross-check against forecast/skipped tier composition
  - Payload stability gate:
    - explicit schema validation per scenario via `app.payload_schema.validate_payload`
    - key-signature stability checks across scenarios (`top`, `forecasts`, `skipped_skus`, `profit_analysis`)
- Captured new validation outputs at:
  - `backend-ai/outputs/replay_validation/20260323T014909Z/summary.json`
  - `backend-ai/outputs/replay_validation/20260323T014909Z/summary.csv`
- Updated checklist in `backend-ai/PROTOTYPE_READINESS_CHECKLIST.md`:
  - checked `Tier logic is consistent (no contradictory gate outcomes)`
  - checked `JSON payload contract stable for both frontend and backend`

### The Reasoning
- These two gates were blocking smoke-test confidence. Running them inside the existing replay suite keeps the process fast, repeatable, and tied to real pipeline artifacts.
- We validate the same output surface frontend/delivery consume, rather than only unit-level assumptions.

### The Tech Debt
- Stability currently compares key signatures (shape); it does not yet enforce strict value-level invariant tests per field.
- Anomaly validation in this suite still uses `spike` as the primary anomaly proxy; adding `drop` to mandatory pass/fail remains a good follow-up.

## 2026-03-23 — Fast-Dev vs Full-Mode Validation Gate Closed

### The Change
- Patched `backend-ai/tools/replay_scenario_suite.py` to enforce forecasting profile overrides at runtime:
  - `MAX_ACTIVE_SKUS`
  - `TEST_DAYS`
- Added profile trace columns to suite output rows:
  - `max_active_skus`
  - `test_days`
- Ran two separate replay validation suites on the same date/dataset:
  1. Fast-dev profile: `--max-active-skus 6 --test-days 14`
     - Output: `backend-ai/outputs/replay_validation/20260323T015758Z/summary.csv`
     - Verified logs show `Processing 6 active SKUs`
  2. Full profile: `--max-active-skus 20 --test-days 30`
     - Output: `backend-ai/outputs/replay_validation/20260323T015849Z/summary.csv`
     - Verified logs show `Processing 20 active SKUs`
- Updated `backend-ai/PROTOTYPE_READINESS_CHECKLIST.md`:
  - checked `Fast-dev and full-mode both run successfully`.

### The Reasoning
- Forecasting config was import-time bound, so env-only overrides inside one process could silently fail and make fast/full runs look identical.
- Runtime override in the suite removes that ambiguity and gives deterministic proof for both operating modes before smoke testing.

### The Tech Debt
- Forecasting still loads defaults at module import; long-term, config should be read per-run (function args or runtime getter) to avoid this class of issue outside the validation suite.
- Remaining go/no-go item is non-technical alignment: team KPI launch thresholds.

## 2026-03-23 — Prophet Runtime Diagnosis (cmdstanpy Pin)

### The Change
- Updated `backend-ai/requirements.txt` to pin:
  - `cmdstanpy==1.2.5`
- Installed `cmdstanpy==1.2.5` in local `python10` env and verified versions:
  - `prophet==1.1.6`
  - `cmdstanpy==1.2.5`

### The Reasoning
- Root cause of the original smoke-test error (`'Prophet' object has no attribute 'stan_backend'`) was reproduced and traced to backend loading behavior with `cmdstanpy==1.3.0` against Prophet's bundled CmdStan path.
- Pinning to `cmdstanpy==1.2.5` resolves the init-time backend crash (Prophet init succeeds).

### The Tech Debt
- Prophet fit still hangs in this environment, and direct execution of bundled `prophet_model.bin` returns Windows status `3221225781` (DLL/runtime issue). This is a separate runtime dependency problem beyond the init-time cmdstanpy mismatch.
- Need a dedicated runtime fix path for Windows local env (either full conda-forge Prophet stack or explicit DLL/runtime path hardening) before we can rely on Prophet in production-like runs.

## 2026-03-23 — Fixed Prophet Installation on Windows

### The Change
- Uninstalled `prophet==1.1.6` installed via `conda-forge`, which resulted in `STATUS_ILLEGAL_INSTRUCTION` (`3221225657`) errors due to an incompatible compiled binary.
- Installed `prophet==1.1.6` from PyPI using `pip` to bypass the conda binary and rely on pre-built pip wheels. 
- Restored missing dependencies (`joblib`, etc.) affected by the conda uninstall.
- Validated via smoke tests that Prophet no longer crashes and does not incorrectly trigger the ARIMA fallback.

### The Reasoning
- Conda-forge binaries for `prophet` and `cmdstanpy` can occasionally contain instructions (like AVX2) that fail on specific Windows processors. Using `pip install prophet` circumvents this by utilizing universally compatible binaries and resolves the `3221225657` termination signal.

### The Tech Debt
- The problem is specific to the local Windows development environment setup; CI/CD and deployment environments usually don't suffer from this instruction set mismatch.
