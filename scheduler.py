"""
Gem Finder Scheduler - Runs as a persistent service.
Executes the gem finder scan once daily at 6:00 AM ET (before market open).
Deploy this on Railway as a persistent service if you prefer over cron.
"""

import time
import logging
from datetime import datetime, timezone, timedelta

from gem_finder import run_gem_finder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scheduler")

# Run at 6:00 AM ET (11:00 UTC) - before US market open
RUN_HOUR_UTC = 11
RUN_MINUTE_UTC = 0


def next_run_time() -> datetime:
    """Calculate the next scheduled run time."""
    now = datetime.now(timezone.utc)
    target = now.replace(hour=RUN_HOUR_UTC, minute=RUN_MINUTE_UTC, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return target


def main():
    log.info("Gem Finder Scheduler started.")
    log.info(f"Scheduled daily at {RUN_HOUR_UTC:02d}:{RUN_MINUTE_UTC:02d} UTC (6:00 AM ET)")

    # Run immediately on first startup
    log.info("Running initial scan on startup...")
    try:
        run_gem_finder()
    except Exception as e:
        log.error(f"Initial run failed: {e}")

    while True:
        target = next_run_time()
        wait_seconds = (target - datetime.now(timezone.utc)).total_seconds()
        log.info(f"Next run at {target.isoformat()} ({wait_seconds / 3600:.1f}h from now)")

        # Sleep in chunks so we can log periodically
        while wait_seconds > 0:
            sleep_time = min(wait_seconds, 3600)  # Wake every hour to log
            time.sleep(sleep_time)
            wait_seconds -= sleep_time
            if wait_seconds > 0:
                log.info(f"Waiting... {wait_seconds / 3600:.1f}h until next run")

        log.info("Starting scheduled run...")
        try:
            run_gem_finder()
        except Exception as e:
            log.error(f"Scheduled run failed: {e}")


if __name__ == "__main__":
    main()
