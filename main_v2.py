import os, warnings, ssl, traceback, time
from datetime import datetime, timedelta
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import output_path, schedule_config, load_config
from app.logger import setup_logger, globalize_logger
from app.scraper import ScraperHandler
from app.mailer import Mailer
from app.schedular import schedule_program


def program_handler():
    
    logger.info(f"Running Program: {PROGRAM_NAME}")
    mailer = Mailer()    
    try:
        
        config = load_config()
        scraper = ScraperHandler(config)
        if mailer.SEND_MAIL:
            logger.info("Sending start email...")
            mailer.start_mail( program=PROGRAM_NAME,dev=True)
            
        
        scraper.ReadContracts()
        
        scraper.RunContracts()
        

        # # ---- Start Selenium ----
        # if not scraper.start_session(minimized=minimize):
        #     raise RuntimeError("Failed to initialize Selenium driver")

        # # ---- Main scrape ----
        # final_dict = scraper.runner(bank_codes)

        # # ---- Reports ----
        # scraper.create_scrape_report(final_dict, report_type)
        # error_html_path = scraper.export_error_log()

        # if process:
        #     scraper.process_cache(final_dict)

    
        # if mailer.SEND_MAIL:
        #     logger.info("Sending completion email...")
        #     attachments = [
        #         scraper.paths["xlsx_latest"],
        #         scraper.paths["pdf_latest"],
        #         scraper.paths["compare_xlsx"]
        #     ]
        #     print(attachments)
        #     mailer.end_mail(
        #         program=PROGRAM_NAME,
        #         attachments=attachments,
        #         custom_html=Helper.read_html(error_html_path),
        #     )

        logger.info("Scraping run completed successfully.")

    except Exception:
        logger.critical("Scraping run failed.")
        logger.error(traceback.format_exc())
        raise

    finally:
        # ---- Always cleanup ----
        try:
            scraper.close()
        except Exception:
            logger.warning("Failed to cleanly close scraper session.")



if __name__ == "__main__":
    
    PROGRAM_NAME = "Interest Rates WebScraper"
    OUTPUT_PATH = output_path()
    # --- Setup Global Logger ---
    logger = setup_logger(
        name="scraper", 
        log_dir=os.path.join(OUTPUT_PATH,"log"), 
        log_level=5
    )
    globalize_logger(logger)

    
    logger.notice("Starting Scraper Scheduler...")
    config_sch = schedule_config()
    schedule_program(
        logger,
        program_handler,
        config_sch["days"], 
        config_sch["time"] 
    )
