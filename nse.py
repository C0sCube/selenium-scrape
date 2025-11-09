import os, warnings, ssl, traceback, time, json
from datetime import datetime, timedelta
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import ALL_BANK_CODES
from app.constants import load_days, load_times,out_nse_path
from app.logger import setup_logger, set_global_logger
from app.BankScraper import BankScraper
from app.utils import Helper
from app.mailer import Mailer

PROGRAM_NAME = "Interest Rates Bank WebScraper"

# --- Setup Global Logger ---
global logger, log_path
log_path = os.path.join(out_nse_path(),"log")
logger = setup_logger(name="scraper", log_dir=log_path, log_level=5)
set_global_logger(logger)


shared_json_path = os.path.join(out_nse_path(), "latest_scrape.json")
save_latest = True


import json

def update_nse_series(scraped_path, config_path = r"configs\param_table.json5"):
    
    def extract_series_keys(data):
        series_keys = []
        for series_name in ["Series A", "Series B"]:
            items = data.get("data", {}).get(series_name, [])
            keys = [item.get("key") for item in items if "key" in item]
            series_keys.extend(keys)
        return series_keys
    try:
        with open(scraped_path, 'r') as f:
            scraped = json.load(f)
        nested = scraped["records"][0]["scraped_data"][0]["response"][0]["value"]
        keys = extract_series_keys(nested)

        with open(config_path, 'r') as f:
            config = json.load(f)
        config["NSE_1"]["blocks"][0]["base_api"]["params"]["series"] = keys
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

        print("✨ NSE_1 series updated successfully.")
    except Exception as e:
        print("⚠️ Something went wrong:", e)

def main(
    bank_codes, 
    process=False, 
    send_mail = False, 
    report_type = "pdf",
    rewrite = False, 
    minimize = False,
    save_latest = True
):
    """Core scraping and processing routine.
    1️⃣ Starts Selenium session
    2️⃣ Runs scraping for given bank codes
    3️⃣ Saves raw cache
    4️⃣ Optionally processes, compares, and generates reports"""
    
    output_root = out_nse_path()
    runtime_path = Helper.create_dir(output_root, "session", f"session_nse")
    session_latest_path = os.path.join(output_root,"session","session_latest")
    
    scraper = BankScraper(
        runtime_path=runtime_path,
        session_latest_path=session_latest_path
    )
    mailer = Mailer()
    
    #start mail
    if send_mail:
        logger.info("Sending start email...")
        mailer.start_mail(
            program= PROGRAM_NAME,
            data = bank_codes
        )

    if not scraper.start_session(minimized=minimize):
        raise RuntimeError("Failed to initialize Selenium driver")
    
    final_dict = scraper.runner(bank_codes)
    if save_latest:
        with open(shared_json_path, "w", encoding="utf-8") as f:
            json.dump(final_dict, f, indent=2, ensure_ascii=False)
        update_nse_series(shared_json_path)
            
    #report pdf,xlsx
    scraper.create_scrape_report(final_dict,report_type,rewrite=rewrite)
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
    return final_dict
        
def scheduler_loop(
    bank_codes, 
    process=False, 
    times=None, 
    run_days=None, 
    send_mail = False,
    minimize = False,
    report_type = "pdf",
    rewrite = False,
    save_latest = True
    ):
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
                    main(
                        bank_codes, 
                        process=process, 
                        send_mail=send_mail,
                        minimize=minimize,
                        report_type=report_type,
                        rewrite=rewrite,
                        save_latest=save_latest
                    )
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


import threading
if __name__ == "__main__":
    logger.notice("Starting Scraper Scheduler...")

    # Thread for NSE_0
    t1 = threading.Thread(target=scheduler_loop, args=(["NSE_0"],), kwargs={
        "times": ["1455"],
        "run_days": load_days(),
        "minimize": True,
        "report_type": None,
    })

    # Thread for NSE_1
    t2 = threading.Thread(target=scheduler_loop, args=(["NSE_1"],), kwargs={
        "times": ["1500", "1510","1520","1530","1540","1550","1600", "1610","1620","1630","1640","1650","1700"],
        "run_days": load_days(),
        "minimize": True,
        "report_type": "xlsx",
        "save_latest": False,
    })

    # Start both threads
    t1.start()
    t2.start()

    # Optional: Wait for both threads to finish (they won't unless interrupted)
    t1.join()
    t2.join()
      
# if __name__ == "__main__":
#     logger.notice("Starting Scraper Scheduler...")
#     bank_codes = ["NSE_0"]
#     scheduler_loop(
#         bank_codes,
#         times= ["1330"],
#         run_days=load_days(),
#         minimize = True,
#         report_type = None,
#     )

    
    
#     bank_codes = ["NSE_1"]
#     scheduler_loop(
#         bank_codes,
#         times= ["1335","1345"],
#         run_days=load_days(),
#         minimize = True,
#         report_type = "xlsx",
#         save_latest=False
#     )

