import time
from datetime import timedelta, datetime

def scheduler_loop(logger, run_fn, sch_days:list, sch_time:list):
    """
    Wraps the main() scraper function to run at specific times (HHMM format)
    and only on specified weekdays. times = ["0800", "1240", "1530"] run_days = ["mon", "tue", "wed", "thu", "fri"]
    
    """
    while True:
        now = datetime.now()
        weekday_str = now.strftime("%a").lower()

        # Skip non-run days (like weekends)
        if weekday_str not in sch_days:
            logger.info(f"Skipping today ({weekday_str.upper()}) — not in run days.")
            tomorrow = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
            wait_seconds = (tomorrow - now).total_seconds()
            time.sleep(wait_seconds)
            continue  # restart loop

        # --- Determine next scheduled run time ---
        today_times = [datetime.strptime(t, "%H%M").time() for t in sch_time]
        future_runs = [datetime.combine(now.date(), t) for t in today_times if datetime.combine(now.date(), t) > now]

        if future_runs:
            next_run = future_runs[0]
        else:
            # All times passed — find next valid run day
            next_day = now.date() + timedelta(days=1)
            while next_day.strftime("%a").lower() not in sch_days:
                next_day += timedelta(days=1)
            next_run = datetime.combine(next_day, today_times[0])

        wait_seconds = (next_run - now).total_seconds()
        logger.info(f"Next Schdeuled Run @ {next_run.strftime('%d-%m-%y %H:%M')}. Waiting {int(wait_seconds)} seconds...")
        time.sleep(wait_seconds)

        # --- Execute scheduled run ---
        weekday_str = datetime.now().strftime("%a").lower()
        if weekday_str in sch_days:
            try:
                logger.info("=" * 60)
                logger.info(f"Running Schduled Program @ {datetime.now().strftime('%H:%M')} ({weekday_str.upper()})")

                run_fn() #< -- function runs here
                
                logger.info(f"Completed Scheduled Run @ {datetime.now().strftime('%H:%M')}")
            except Exception as e:
                logger.critical(f"Run failed: {type(e).__name__}: {e}")
                
        else:
            logger.info(f"Skipped run because today ({weekday_str.upper()}) is not in run days.")
