import os, warnings,time, ssl, traceback
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow-specific-selenium
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

from app.utils import Helper
from app.program_logger import get_forever_logger
from app.BankScraper import BankScraper
from app.mailer import Mailer
from app.constants import CONFIG, PATHS, PROGRAM_NAME
from app.constants import ALL_BANK_CODES,PVT_BANK_CODES,PUB_BANK_CODES
from app.constants import CACHE_REP_DIR,LOG_DIR, CCH_DIR, PRS_DIR


logger = get_forever_logger(name="scraper", log_dir=LOG_DIR)
# bank_codes = PVT_BANK_CODES
# bank_codes = PUB_BANK_CODES
bank_codes = [f"PSB_{i}" for i in range(1,13)]+[f"PVB_{i}" for i in range(1,23)]
# bank_codes = ["PVB_1","PVB_2"]
Mailer().start_mail(PROGRAM_NAME,data=bank_codes)

try:
    logger.notice("Starting Program.")
    final_dict = BankScraper.get_final_struct()
    for code in bank_codes:
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
    

    #doc report
    doc_path = os.path.join(CACHE_REP_DIR,"SAMPLE_DATA.docx")
    BankScraper.generate_cache_report(final_dict,output_path=doc_path)
    logger.save("Initial Cache Report Saved.")
    
    Mailer().end_mail(PROGRAM_NAME,attachments=[doc_path])
    
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
    
#post-scrape
# proce_dict = BankScraper.post_scrape(final_dict, POST_SCRAPE_OPS, logger)
# Helper.save_json(proce_dict,os.path.join(PROCESS_DIR, final_dict["metadata"]["pfname"]))