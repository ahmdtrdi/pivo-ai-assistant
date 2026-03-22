# PIVO Backend â€” Prototype

Python backend for the PIVO nightly pipeline. Drop this folder into `backend-ai/` in the actual repo.

## Structure

```
prototype/
â”œâ”€â”€ app/
â”‚   â”œâ”€â”€ db.py           Supabase helpers (accounts, daily_payloads, reminder_log)
â”‚   â”œâ”€â”€ ingestion.py    Google Sheets reader + CSV fallback
â”‚   â”œâ”€â”€ cleaning.py     Gap-fill, fuzzy SKU dedup, outlier quarantine
â”‚   â”œâ”€â”€ forecasting.py  Prophet â†’ ARIMA routing loop (â‰¤20 active SKUs)
â”‚   â”œâ”€â”€ confidence.py   4-gate confidence tier + missing-data policy
â”‚   â”œâ”€â”€ profit.py       Profit & margin analysis (real COGS or synthetic)
â”‚   â”œâ”€â”€ llm.py          Gemini 2.0 Flash tier-aware WA message generation
â”‚   â”œâ”€â”€ delivery.py     Fonnte WhatsApp API wrapper
â”‚   â””â”€â”€ pipeline.py     Main nightly orchestrator
â”œâ”€â”€ scheduler.py        APScheduler cron (23:00 WIB / 16:00 UTC)
â”œâ”€â”€ contracts/
â”‚   â””â”€â”€ payload_schema.json   JSON Schema for daily_payloads.payload (copy to repo contracts/)
â”œâ”€â”€ supabase/
â”‚   â””â”€â”€ seed.sql        Demo data for local dev
â”œâ”€â”€ requirements.txt
â”œâ”€â”€ Procfile            Railway/Render worker process
â”œâ”€â”€ railway.toml
â””â”€â”€ .env.example
```

## Setup (local dev)

```bash
# 1. Create venv and install deps
python -m venv .venv && source .venv/bin/activate   # Unix
# or: .venv\Scripts\activate                         # Windows
pip install -r requirements.txt

# 2. Copy env vars
cp .env.example .env
# Edit .env â€” at minimum set SUPABASE_URL + SUPABASE_SERVICE_KEY

# 3. (Optional) Apply Supabase seed data
# Run supabase/seed.sql in your Supabase SQL editor

# 4. Run one pipeline cycle locally (CSV mode, no Sheets key needed)
python -m app.pipeline --csv ../data/external/coffee_shop_sales.csv

# 5. Verify the payload was printed to stdout (DEV_MODE=true is auto-set)
```

## Deployment to Railway

1. Create a new Railway project â†’ empty service â†’ connect GitHub repo
2. Set all env vars from `.env.example` in Railway's Variables panel
3. Set `FONNTE_DRY_RUN=false` and add your real Fonnte token
4. Set `GOOGLE_SA_JSON_PATH` (upload the service account JSON as a Railway volume)
5. Deploy â€” Railway will detect `railway.toml` and run `python scheduler.py`

## Moving to the actual repo

```
# Copy this folder to backend-ai/
cp -r prototype/ ../your-repo/backend-ai/

# Copy the contract schema
cp backend-ai/../contracts/payload_schema.json ../your-repo/contracts/
```

No code changes needed â€” just update `.env` values.

## Key design decisions

- **No daily_sales table** â€” raw data is fetched fresh from Sheets each run; only the final payload is persisted in Supabase.
- **`consecutive_missing_days` on the account row** â€” the pipeline increments this on each run without new data and resets it when data arrives. Three escalation levels map to Table 5.
- **Per-account failure isolation** â€” `run_all_owners()` catches exceptions per account so one bad owner doesn't block the rest.
- **Dry-run mode by default** â€” `FONNTE_DRY_RUN=true` in `.env.example` means WA sends are logged, not actually sent, until you explicitly flip it.


