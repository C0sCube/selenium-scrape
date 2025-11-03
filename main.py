import os, warnings, ssl, traceback, time
from datetime import datetime, timedelta
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import LOG_DIR, OUTPUT_PATH,SESSION_ROOT
from app.constants import RUN_DAYS, SCHEDULE_TIMES
from app.constants import ALL_BANK_CODES
from app.logger import setup_logger, set_global_logger
from app.BankScraper import BankScraper
from app.utils import Helper
from app.mailer import Mailer

PROGRAM_NAME = "Interest Rates Bank WebScraper"

# --- Setup Global Logger ---
logger = setup_logger(name="scraper", log_dir=LOG_DIR)
set_global_logger(logger)


def main(bank_codes, process=False, is_headless=False, send_mail = False, report_type = "pdf"):
    """Core scraping and processing routine.
    1️⃣ Starts Selenium session
    2️⃣ Runs scraping for given bank codes
    3️⃣ Saves raw cache
    4️⃣ Optionally processes, compares, and generates reports"""
    scraper = BankScraper()
    mailer = Mailer()
    
    #start mail
    if send_mail:
        logger.info("Sending start email...")
        mailer.start_mail(
            program= PROGRAM_NAME,
            data = bank_codes
        )

    if not scraper.start_session(headless=is_headless):
        raise RuntimeError("Failed to initialize Selenium driver")
    
    final_dict = scraper.runner(bank_codes)
    #report pdf,xlsx
    scraper.create_scrape_report(final_dict,report_type)
    scraper.export_error_log()
    if process: scraper.process_cache(final_dict)

    #end mail
    if send_mail:
        logger.info("Sending completion email...")
        attatchments = [ scraper.pdf_path,scraper.xls_path, scraper.comparison_xlsx]
        mailer.end_mail(
            program=PROGRAM_NAME,
            data = scraper.email_msg,
            attachments=attatchments,
            custom_html=Helper.read_html(scraper.error_html_path)
        )
    
    logger.info("Scraping Program Completed Successfully.")
        
def scheduler_loop(bank_codes, process=True, headless=False, times=None, run_days=None, send_mail = False):
    """
    Wraps the main() scraper function to run at specific times (HHMM format)
    and only on specified weekdays.

    Example:
        times = ["0800", "1400", "2200"]
        run_days = ["mon", "tue", "wed", "thu", "fri"]
    """
    logger.info(f"Scheduler configuration → Times: {times}, Run Days: {run_days}")

    try:
        while True:
            now = datetime.now()
            weekday_str = now.strftime("%a").lower()

            # 💤 Skip non-run days (like weekends)
            if weekday_str not in run_days:
                logger.info(f"Skipping today ({weekday_str.upper()}) — not in run days.")
                tomorrow = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
                wait_seconds = (tomorrow - now).total_seconds()
                time.sleep(wait_seconds)
                continue  # restart loop

            # --- Determine next scheduled run time ---
            today_times = [datetime.strptime(t, "%H%M").time() for t in times]
            future_runs = [datetime.combine(now.date(), t) for t in today_times if datetime.combine(now.date(), t) > now]

            if future_runs:
                next_run = future_runs[0]
            else:
                # All times passed — find next valid run day
                next_day = now.date() + timedelta(days=1)
                while next_day.strftime("%a").lower() not in run_days:
                    next_day += timedelta(days=1)
                next_run = datetime.combine(next_day, today_times[0])

            wait_seconds = (next_run - now).total_seconds()
            logger.info(f"Next run scheduled at {next_run.strftime('%d-%m-%y %H:%M')}. Waiting {int(wait_seconds)} seconds...")
            time.sleep(wait_seconds)

            # --- Execute scheduled run ---
            weekday_str = datetime.now().strftime("%a").lower()
            if weekday_str in run_days:
                try:
                    logger.save("=" * 60)
                    logger.notice(f"Running scraper at {datetime.now().strftime('%H:%M')} ({weekday_str.upper()})")
                    main(bank_codes, process=process, is_headless=headless, send_mail=send_mail)
                    logger.info(f"Completed run at {datetime.now().strftime('%H:%M')}")
                except Exception as e:
                    logger.critical(f"Run failed: {type(e).__name__}: {e}")
                    logger.debug(traceback.format_exc())
            else:
                logger.info(f"Skipped run because today ({weekday_str.upper()}) is not in run days.")

    except KeyboardInterrupt:
        logger.warning("Scheduler interrupted manually.")
        logger.debug(traceback.format_exc())
        if send_mail:
            mailer = Mailer()
            mailer.fatal_error_mail(
                program=f"{PROGRAM_NAME}: {datetime.now().strftime("%d-%m-%y")}",
                custom_msg="Keyboard Interrupt",
                error_message="User manually stopped the scheduler.",
                exception_obj=None
            )
        

    except Exception as e:
        logger.critical(f"Unexpected scheduler failure: {type(e).__name__}: {e}")
        logger.debug(traceback.format_exc())
        if send_mail:
            mailer = Mailer()
            mailer.fatal_error_mail(
                program=f"{PROGRAM_NAME}: {datetime.now().strftime("%d-%m-%y")}",
                custom_msg="Unexpected error in scheduler loop",
                error_message=str(e),
                exception_obj=e
            )
        
if __name__ == "__main__":
    logger.notice("Starting Scraper Scheduler...")
    bank_codes = ALL_BANK_CODES

    scheduler_loop(
        bank_codes,
        process=True,
        times= ["0800","1400"],  #SCHEDULE_TIMES,
        run_days=RUN_DAYS,
        send_mail=True
    )

