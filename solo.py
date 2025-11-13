import os, warnings, ssl, traceback
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import * #paths

from app.logger import setup_logger, set_global_logger
from app.BankScraper import BankScraper
 
log_dir = os.path.join(output_path(),"log")
logger = setup_logger(name="scraper", log_dir=log_dir, log_level=5)
set_global_logger(logger)

def main(
    bank_codes, 
    process = False,
    minimize = False,
    report_type = "pdf"
):
        
    scraper = BankScraper()
    if not scraper.start_session(minimized=minimize):  
        raise RuntimeError("Failed to initialize Selenium driver")

    final_dict = scraper.runner(bank_codes)
    scraper.create_scrape_report(final_dict,report_type=report_type)
    scraper.export_error_log()
    if process: scraper.process_cache(final_dict)

    logger.info("Scraping Program Completed Successfully.")


if __name__ == "__main__":
    
    try:
        logger.notice("Starting Scraper Program.")
        bank_codes = FRN_BANK_CODES + SFB_BANK_CODES #PUB_BANK_CODES #ALL_BANK_CODES 
        main(
            bank_codes, 
            process=True, 
            minimize=True,
            report_type="pdf"
        )
        
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
