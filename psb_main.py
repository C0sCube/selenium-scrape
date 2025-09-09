import os, warnings,time, ssl, traceback
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow-specific-selenium
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

from app.utils import Helper
from app.program_logger import get_forever_logger
from app.BankScraper import BankScraper
from app.mailer import Mailer
from app.constants import LOG_DIR, CCH_DIR, CONFIG, PATHS

logger = get_forever_logger(name="scraper", log_dir=LOG_DIR)
BANK_CODES = [f"PSB_{i}" for i in range(1,13)]
try:
    final_dict = BankScraper.get_final_struct()
    for code in BANK_CODES:
        if code not in CONFIG:
            continue
        try:
            bank_params = CONFIG[code]
            scraper = BankScraper(bank_params, logger, PATHS)
            result = scraper.run()
        except Exception as e:
            logger.error(f"Failed scraping {code}: {e}")
            result = {"bank_code": code, "scraped_data": [{"error": str(e)}]}
        
        clean_result = BankScraper.dedupe_responses(result)
        final_dict["records"].append(clean_result)
        time.sleep(3)
        
except KeyboardInterrupt:
    logger.warning("Process Interrupted by User!")  
    
except Exception as e:
    logger.error(f"Error in Main.py :[{type(e).__name__}] {e}")
    logger.debug(f"Traceback:\n{traceback.format_exc()}")
    
finally:
    Helper.save_json(final_dict, os.path.join(CCH_DIR, final_dict["metadata"]["cfname"]))
    logger.save("Saved Cached Data.")