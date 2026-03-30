import os
from datetime import date
from dotenv import load_dotenv

load_dotenv()
os.environ["DEV_CSV_PATH"] = "data/Serayu_Chicken_60k.csv"
os.environ["REPLAY_MODE"] = "true"
os.environ["REPLAY_CURSOR_DATE"] = "2026-01-11"

# Explicitly disable DEV_MODE so `run_all_owners()` fetches real owner numbers from your Supabase table
os.environ["DEV_MODE"] = "false" 

from app import pipeline

print("Running complete end-to-end pipeline (Supabase + local CSV + Gemini Auth + Fonnte Delivery)...")
pipeline.run_all_owners(date(2026, 1, 11))
