import os, warnings, ssl, traceback, time
from datetime import datetime, timedelta
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
# from app.constants import output_path, schedule_config,load_config
# from app.logger import setup_logger, get_global_logger, globalize_logger
# from app.BankScraper import BankScraper
# from app.utils import Helper
# from app.mailer import Mailer
# from app.schedular import schedule_program

from app.constants import output_path, get_schedule_config,load_config, get_config_prog
from app.logger import setup_logger, get_global_logger, set_global_logger
from app.BankScraper import BankScraper
from app.utils import Helper
from app.mailer import Mailer
from app.schedular import scheduler_loop

def program_handler(
    process=True,
    report_type="xlsx",
    minimize=True,
):

    scraper = BankScraper()
    mailer = Mailer()
    logger = get_global_logger()

    
    gen_config = load_config()
    bank_codes = ["iNAV_4"]
 
    

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


        final_dict = scraper.runner(bank_codes)     
        scraper.create_scrape_report(final_dict, report_type)
        error_html_path = scraper.export_error_log()

        if process:
            scraper.process_cache(final_dict)

        # ---- Completion mail ----
        if mailer.SEND_MAIL:
            logger.info("Sending completion email...")
            attachments = [
                scraper.paths["xlsx_archive"],
                scraper.paths["pdf_latest"],
                scraper.paths["compare_xlsx"]
            ]

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


if __name__ == "__main__":
    
    PROGRAM_NAME = "Interest Rates WebScraper"
    # --- Setup Global Logger ---
    log_path = os.path.join(output_path(),"log")
    logger = setup_logger(name="scraper", log_dir=log_path, log_level=5)
    setup_logger(logger)
    
    mail_config =  {
        "send_mail":True,
        "sender": "kaustubh.keny@cogencis.com",
        "dev_recipients": [
            "Kaustubh.Keny@cogencis.com"
        ],
        "recipients": [
            "Kaustubh.Keny@cogencis.com"
        ],
        "cc": [
            "Kaustubh.Keny@cogencis.com"
        ],
        "server": "172.17.0.126",
        "port": 25
    }

    
    logger.notice("Starting Scraper Scheduler...")
    # config_sch = schedule_config()
    # schedule_program(
    config_sch = get_schedule_config()
    scheduler_loop(
        logger,
        lambda: program_handler(
            process=False,
            minimize=False,
            report_type="xlsx"
        ),
        ["mon","tue","wed","thu","fri","sat"], 
        ["1535"] 
    )
