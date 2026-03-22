-- seed.sql — Demo data for local development and pipeline smoke tests.
-- Run this in your Supabase SQL editor after the schema is applied.

-- Demo account (no real phone number — Fonnte DRY_RUN must be enabled)
INSERT INTO accounts (id, name, whatsapp_number, sheet_id, consecutive_missing_days)
VALUES (
  'a0000000-0000-0000-0000-000000000001',
  'Demo Warung Kopi',
  '62812000000',
  '',   -- empty = CSV-only mode in local dev
  0
)
ON CONFLICT DO NOTHING;

-- Sample daily_payload so the PWA has something to show immediately
INSERT INTO daily_payloads (owner_id, date, payload, confidence_tier)
VALUES (
  'a0000000-0000-0000-0000-000000000001',
  CURRENT_DATE,
  '{
    "owner_id": "a0000000-0000-0000-0000-000000000001",
    "date": "2024-01-01",
    "model_routing": "prophet_first_arima_fallback",
    "confidence_tier": "yellow",
    "consecutive_missing_days": 0,
    "forecasts": [
      {
        "sku": "kopi_susu",
        "sku_name": "Kopi Susu",
        "category": "Coffee",
        "selected_model": "arima",
        "tier": "yellow",
        "tier_if_missing_3d": "red",
        "qty_mid": 42.0,
        "qty_low": 30.0,
        "qty_high": 55.0,
        "trend": "growing",
        "stockout_risk": false
      }
    ],
    "profit_analysis": [],
    "anomaly_flags": [],
    "wa_message": "🟡 Prediksi: Kopi Susu sekitar 42 cup besok (perkiraan awal).",
    "pwa_url": "pivo.app/u/a0000000-0000-0000-0000-000000000001",
    "skipped_skus": []
  }',
  'yellow'
)
ON CONFLICT (owner_id, date) DO NOTHING;
