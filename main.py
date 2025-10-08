import os, warnings,time, ssl, traceback
from datetime import datetime
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow-specific-selenium
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

from app.utils import Helper
from app.logger import setup_logger
from app.BankScraper import BankScraper
from app.constants import CONFIG, PATHS
from app.constants import CACHE_REP_DIR,LOG_DIR, CCH_DIR
from app.constants import ALL_BANK_CODES,PUB_BANK_CODES,PVT_BANK_CODES
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logger = setup_logger(name="scraper", log_dir=LOG_DIR)
bank_codes = ["PSB_6"]#PVT_BANK_CODES #PUB_BANK_CODES #ALL_BANK_CODES

try:
    logger.notice("Starting Program.")
    final_dict = BankScraper.get_final_struct()
    for code in bank_codes:
        if code not in CONFIG:
            continue
        try:
            bank_params = CONFIG[code]
            scraper = BankScraper(bank_params, PATHS)
            result = scraper.run()
        except Exception as e:
            logger.error(f"Failed scraping {code}: {e}")
            result = {"bank_code": code, "scraped_data": [{"error": str(e)}]}
        
        clean_result = BankScraper.dedupe_responses(result)
        final_dict["records"].append(clean_result)
        time.sleep(3)
    

    #doc report
    doc_path = os.path.join(CACHE_REP_DIR,f"cache_{timestamp}_DATA.docx")
    # IbbiHelper.cache_to_excel_report(final_dict,format_="data",excel_out=doc_path)
    BankScraper.generate_cache_report(final_dict, doc_path)
    logger.save("Initial Cache Report Saved.")
    
    
except KeyboardInterrupt:
    logger.warning("Process Interrupted by User!")
    logger.debug(f"Traceback:\n{traceback.format_exc()}")
    
except Exception as e:
    logger.error(f"Error in Main.py :[{type(e).__name__}] {e}")
    logger.debug(f"Traceback:\n{traceback.format_exc()}")

finally:
    Helper.save_json(final_dict, os.path.join(CCH_DIR, final_dict["metadata"]["cfname"]),typ="json")
    logger.save("Saved Cached Data.")
    logger.notice("Ending Program.")