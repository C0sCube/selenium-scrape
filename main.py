import os, warnings, ssl, traceback, time
from datetime import datetime, timedelta
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import ALL_BANK_CODES, FRN_BANK_CODES, SFB_BANK_CODES
from app.constants import load_days, load_times,output_path, get_session_dir, create_dir
from app.logger import setup_logger, set_global_logger
from app.BankScraper import BankScraper
from app.utils import Helper
from app.mailer import Mailer
from app.schedular import scheduler_loop


def program_handler(
    bank_codes,
    logger,
    process=False,
    send_mail=False,
    report_type="pdf",
    minimize=False,
):

    scraper = BankScraper()
    mailer = Mailer()

    try:
        # ---- Start mail ----
        if send_mail:
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
        if send_mail:
            logger.info("Sending completion email...")
            attachments = [
                scraper.paths["xlsx_latest"],
                scraper.paths["pdf_latest"],
                scraper.comparison_xlsx
            ]
            mailer.end_mail(
                program=PROGRAM_NAME,
                attachments=attachments,
                custom_html=Helper.read_html(error_html_path),
            )

        logger.info("Scraping run completed successfully.")

    except Exception:
        logger.critical("Scraping run failed.")
        logger.debug(traceback.format_exc())
        raise  # let scheduler log failure

    finally:
        # ---- Always cleanup ----
        try:
            scraper.close()
        except Exception:
            logger.warning("Failed to cleanly close scraper session.")


def main():
    logger.info(f"Running Program: {PROGRAM_NAME}")
    program_handler(
        BANK_CODES,
        logger=logger,
        process=PROCESS_FILE,
        send_mail=SEND_MAIL,
        minimize=MINIMIZE,
    )


if __name__ == "__main__":
    
    PROGRAM_NAME = "Interest Rates WebScraper"
    PROCESS_FILE= True
    SEND_MAIL = False
    MINIMIZE = False
    # --- Setup Global Logger ---
    log_path = os.path.join(output_path(),"log")
    logger = setup_logger(name="scraper", log_dir=log_path, log_level=5)
    set_global_logger(logger)

    
    logger.notice("Starting Scraper Scheduler...")
    BANK_CODES = ["PSB_4"]#ALL_BANK_CODES + FRN_BANK_CODES +SFB_BANK_CODES
    
    scheduler_loop(
        logger,
        main,
        load_days(), #days
        load_times() #times
    )
