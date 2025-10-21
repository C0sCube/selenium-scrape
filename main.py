import os, warnings, ssl, traceback
from datetime import datetime
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import *
from app.logger import setup_logger, set_global_logger
from app.BankScraper import BankScraper
from app.utils import Helper

today = datetime.now()
runtime_path = Helper.create_dir(OUTPUT_PATH, "session", f"session_{today.strftime('%y%m%d_%H%M')}")
latest_dir = Helper.create_dir(SESSION_ROOT, "session_latest")

logger = setup_logger(name="scraper", log_dir=Helper.create_dir(LOG_DIR,today.strftime("%Y-%m-%d")))
set_global_logger(logger)


def main(bank_codes, scraper, process = False, headless = False):

    if not scraper.start_session(headless = headless):
        raise RuntimeError("Failed to initialize Selenium driver")

    final_dict = scraper.runner(bank_codes)
    scraper.close_session()

    if process:
        process_path = os.path.join(runtime_path, final_dict["metadata"]["pfname"])
        prev_process = os.path.join(latest_dir, "PROCESS_LATEST.json")
        scraper.process_cache(final_dict, process_path,prev_process)

    # scraper.generate_doc_report(final_dict)
    cache_path = os.path.join(runtime_path, final_dict["metadata"]["cfname"])
    Helper.save_json(final_dict, cache_path)
    logger.save(f"Cache saved at: {cache_path}")
    
    logger.info("Scraping Program Completed Successfully.")


if __name__ == "__main__":
    
    try:
        logger.notice("Starting Scraper Program.")
        scraper = BankScraper(runtime_path)
        bank_codes = ["PVB_2"] #PUB_BANK_CODES #ALL_BANK_CODES 
        main(bank_codes, scraper)
        
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
