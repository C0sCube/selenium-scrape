import os, warnings, ssl, traceback, time
from datetime import datetime, timedelta, timezone
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import LOG_DIR, OUTPUT_PATH, SESSION_ROOT #paths
from app.constants import ALL_BANK_CODES, PUB_BANK_CODES,PVT_BANK_CODES #bank_codes
from app.logger import setup_logger, set_global_logger
from app.BankScraper import BankScraper
from app.utils import Helper

today = datetime.now()
logger = setup_logger(name="scraper", log_dir=LOG_DIR)
set_global_logger(logger)


def main(bank_codes, process = False, is_headless = False, minimize = False):
    
    #set path
    today = datetime.now()
    session_path = Helper.create_dir(OUTPUT_PATH, "session", f"session_{today.strftime('%y%m%d_%H%M')}")
    latest_dir = Helper.create_dir(SESSION_ROOT, "session_latest")
    
    
    scraper = BankScraper(session_path)

    if not scraper.start_session(headless = is_headless, minimized=minimize):  raise RuntimeError("Failed to initialize Selenium driver")

    final_dict = scraper.runner(bank_codes)
    scraper.close_session()
    scraper.create_scrape_report(final_dict)
    
    error_txt, error_html = scraper.export_error_log()
    if error_txt:logger.notice(f"Error summary saved at {error_txt}")
    if error_html:logger.notice(f"HTML summary saved at {error_html}")

    
    cache_path = os.path.join(session_path, final_dict["metadata"]["cfname"])
    Helper.save_json(final_dict, cache_path)
    logger.save(f"Cache saved at: {cache_path}")
    
    if process:
        process_path = os.path.join(session_path, final_dict["metadata"]["pfname"])
        prev_process = os.path.join(latest_dir, "PROCESS_LATEST.json")
        scraper.process_cache(final_dict, process_path,prev_process)

    logger.info("Scraping Program Completed Successfully.")


if __name__ == "__main__":
    
    try:
        logger.notice("Starting Scraper Program.")
        bank_codes = ALL_BANK_CODES #PUB_BANK_CODES #ALL_BANK_CODES 
        main(bank_codes, process=False, is_headless=False, minimize=True)
        
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user.")
        logger.debug(traceback.format_exc())

    except Exception as e:
        logger.error(f"Unhandled Error in main.py: [{type(e).__name__}] {e}")
        logger.debug(traceback.format_exc())    

















    # finally:
    #     # Always save partial cache if something fails
    #     try:
    #         if 'final_dict' in locals():
    #             cache_path = os.path.join(session_dir, final_dict["metadata"]["cfname"])
    #             Helper.save_json(final_dict, cache_path, typ="json")
    #             logger.save(f"(Final) Cache safely written to {cache_path}")
    #     except Exception as e:
    #         logger.error(f"Failed to save final cache: {e}")

    #     logger.info("Program Ended.")
