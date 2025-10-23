import os, warnings, ssl, traceback, copy
from datetime import datetime
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

# --- Internal Imports ---
from app.constants import *
from app.logger import setup_logger, set_global_logger
from app.BankScraper import BankScraper
from app.utils import Helper

# --- Setup Logging ---
today = datetime.now()
logger = setup_logger(name="post_process", log_dir=Helper.create_dir(LOG_DIR, today.strftime("%Y-%m-%d")))
set_global_logger(logger)


def main():
    """
    Post-scraping pipeline:
    1. Load an existing CACHE JSON file.
    2. Process it using OperationExecutor.
    3. Compare with previous processed cache.
    4. Generate comparison JSON and Excel reports.
    """

    try:
        logger.notice("Starting post-scraping processing...")

        # === 1️⃣ Define paths ===
        CACHE_PATH = r"C:\Users\rando\Office Projects\outputs\scrape_output\session\session_251020_2341\CACHE201025T2341.json"
        session_path = os.path.dirname(CACHE_PATH)
        latest_dir = Helper.create_dir(SESSION_ROOT, "session_latest")

        # === 2️⃣ Initialize scraper utilities ===
        scraper = BankScraper(session_path)
        logger.notice(f"Loaded cache: {CACHE_PATH}")

        # === 3️⃣ Load and process cache ===
        cache_data = Helper.load_json(CACHE_PATH)
        process_path = os.path.join(session_path, cache_data["metadata"]["pfname"])
        prev_process = os.path.join(latest_dir, "PROCESS_LATEST.json")

        scraper.process_cache(cache_data, process_path, prev_process)

        logger.info("✅ Post-scraping processing completed successfully.")

    except KeyboardInterrupt:
        logger.warning("Process interrupted by user.")
        logger.debug(traceback.format_exc())

    except Exception as e:
        logger.error(f"Unhandled Error in post_scrape_main.py: [{type(e).__name__}] {e}")
        logger.debug(traceback.format_exc())


if __name__ == "__main__":
    main()
