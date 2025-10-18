import os, warnings,time, ssl, traceback
from datetime import datetime
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow-specific-selenium
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context


from app.constants import *
from app.logger import setup_logger, set_global_logger
logger = setup_logger(name="scraper",log_dir=LOG_DIR)
set_global_logger(logger)
from app.BankScraper import BankScraper
from app.utils import Helper

bank_codes = ALL_BANK_CODES  #PUB_BANK_CODES #PVT_BANK_CODES #ALL_BANK_CODES

try:
    logger.notice("Starting Program.")
    scraper = BankScraper()
    final_dict = scraper.get_final_struct()
    scraper.load_driver(headless=False)
    for code in bank_codes:
        if code not in CONFIG:
            logger.error(f"Code: {code} not in config, skipping..")
            continue
        try:
            bank_params = CONFIG[code]
            result = scraper.run(bank_params)
        except Exception as e:
            logger.error(f"Failed scraping {code}: {e}")
            result = {"bank_code": code, "scraped_data": [{"error": str(e)}]}
        
        clean_result = BankScraper.dedupe_responses(result)
        final_dict["records"].append(clean_result)

    scraper.exit_driver()
    

    #doc report

    # doc_path = os.path.join(CACHE_REP_DIR,f"cache_{datetime.now().strftime("%Y%m%d_%H%M")}_DATA.docx")
    # BankScraper.generate_cache_report(final_dict, doc_path)
    # logger.save("Initial Cache Report Saved.")
    
    # value = input("DO PROCESSING AS WELL(Y/N):")
    # if value == 'Y':
    #     prs = POST_SCRAPE_OPS["sha1"]
    #     prs_data = BankScraper.generate_prs_cache(final_dict,prs)
    #     prs_path = os.path.join(PRS_DIR,f"prs_{datetime.now().strftime("%Y%m%d_%H%M")}.json")
    #     Helper.save_json(prs_data,prs_path)
    # else:
    #     print("No processing allowed.")
    
    path = os.path.join(CCH_DIR, final_dict["metadata"]["cfname"])
    Helper.save_json(final_dict, path,typ="json")
    logger.save(f"Saved At: {path}")
    logger.info("Ending Program.") 
    
except KeyboardInterrupt:
    logger.warning("Process Interrupted by User!")
    logger.debug(f"Traceback:\n{traceback.format_exc()}")
    
except Exception as e:
    logger.error(f"Error in Main.py :[{type(e).__name__}] {e}")
    logger.debug(f"Traceback:\n{traceback.format_exc()}")

# finally:
#     path = os.path.join(CCH_DIR, final_dict["metadata"]["cfname"])
#     Helper.save_json(final_dict, path,typ="json")
#     logger.save(f"Saved At: {path}")
#     logger.info("Ending Program.")