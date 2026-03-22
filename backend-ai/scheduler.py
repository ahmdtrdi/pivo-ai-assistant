"""
scheduler.py — APScheduler cron entry point.

Runs pipeline.run_all_owners() nightly at 23:00 WIB (16:00 UTC by default).
Configure via CRON_HOUR_UTC and CRON_MINUTE_UTC env vars.

Deploy as a worker process on Railway/Render (see Procfile and railway.toml).
"""
import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()

from app.pipeline import run_all_owners

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

CRON_HOUR   = int(os.environ.get("CRON_HOUR_UTC",   "16"))
CRON_MINUTE = int(os.environ.get("CRON_MINUTE_UTC",  "0"))


def main() -> None:
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_all_owners,
        trigger=CronTrigger(hour=CRON_HOUR, minute=CRON_MINUTE, timezone="UTC"),
        id="nightly_pipeline",
        name=f"PIVO nightly pipeline ({CRON_HOUR:02d}:{CRON_MINUTE:02d} UTC)",
        misfire_grace_time=300,  # allow up to 5 min late start
        coalesce=True,           # merge missed runs into one
    )

    logger.info(
        f"Scheduler started — nightly pipeline at {CRON_HOUR:02d}:{CRON_MINUTE:02d} UTC "
        f"(23:00 WIB). Waiting..."
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
