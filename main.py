import os, warnings, ssl, traceback, time
from datetime import datetime, timedelta
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import output_path, get_schedule_config,load_config
from app.logger import setup_logger, get_global_logger, set_global_logger
from app.BankScraper import BankScraper
from app.utils import Helper
from app.mailer import Mailer
from app.schedular import scheduler_loop


def program_handler(
    process=True,
    report_type="pdf",
    minimize=True,
):

    scraper = BankScraper()
    mailer = Mailer()
    logger = get_global_logger()
    
    gen_config = load_config()
    bank_codes = [
        k for k in gen_config.keys()
        if k != "POST_SCRAPE_OPS"
    ]
    # bank_codes = ["PSB_4"]
    

    try:
        logger.info(f"Running Program: {PROGRAM_NAME}")
        # ---- Start mail ----
        if mailer.SEND_MAIL:
            logger.info("Sending start email...")
            mailer.start_mail(
                program=PROGRAM_NAME,
                data=bank_codes,
                dev=True
            )

        # ---- Start Selenium ----
        if not scraper.start_session(minimized=minimize):
            raise RuntimeError("Failed to initialize Selenium driver")

        # ---- Main scrape ----
        final_dict = scraper.runner(bank_codes)

        # ---- Reports ----
        scraper.create_scrape_report(final_dict, report_type)
        error_html_path = scraper.export_error_log()

        if process:
            scraper.process_cache(final_dict)

        # ---- Completion mail ----
        if mailer.SEND_MAIL:
            logger.info("Sending completion email...")
            attachments = [
                scraper.paths["xlsx_latest"],
                scraper.paths["pdf_latest"],
                scraper.paths["compare_xlsx"]
            ]
            print(attachments)
            mailer.end_mail(
                program=PROGRAM_NAME,
                attachments=attachments,
                custom_html=Helper.read_html(error_html_path),
            )

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


# def main():
#     mailer = Mailer()
#     logger = get_global_logger()
#     gen_config = load_config()
#     bank_codes = list(gen_config.keys())
    
#     program_handler(
#         bank_codes,
#         logger,
#         mailer,
#         process=PROCESS_FILE,
#         minimize=MINIMIZE,
#     )


if __name__ == "__main__":
    
    PROGRAM_NAME = "Interest Rates WebScraper"
    PROCESS_FILE= True
    MINIMIZE = False
    # --- Setup Global Logger ---
    log_path = os.path.join(output_path(),"log")
    logger = setup_logger(name="scraper", log_dir=log_path, log_level=5)
    set_global_logger(logger)

    
    logger.notice("Starting Scraper Scheduler...")
    # BANK_CODES = ALL_BANK_CODES + FRN_BANK_CODES +SFB_BANK_CODES
    
    config_sch = get_schedule_config()
    
    
    scheduler_loop(
        logger,
        program_handler,
        config_sch["days"], 
        config_sch["time"] 
    )
