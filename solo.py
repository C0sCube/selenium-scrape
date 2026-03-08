import os, warnings, ssl, traceback
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import * #paths

from app.logger import setup_logger, set_global_logger
from app.BankScraper import BankScraper
 
def main(
    bank_codes, 
    process = False,
    minimize = False,
    report_type = "pdf"
):
    
    logger.notice("Starting Scraper Program.")
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
        # --- Setup Global Logger ---
        log_path = os.path.join(output_path(),"log")
        logger = setup_logger(name="scraper", log_dir=log_path, log_level=4)
        set_global_logger(logger)
        bank_codes =["BSE_4"] 
        main(
            bank_codes, 
            process=False, 
            minimize=False,
            report_type="xlsx"
        )

    except Exception as e:
        logger.error(f"Unhandled Error in main.py: [{type(e).__name__}] {e}")
        logger.debug(traceback.format_exc())    

