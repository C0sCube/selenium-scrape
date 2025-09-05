import os, warnings,time, ssl, traceback
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow-specific-selenium
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

from app.utils import Helper
from app.program_logger import get_forever_logger
from app.BankScraper import BankScraper
from app.mailer import Mailer
from app.constants import LOG_DIR, CCH_DIR, PRS_DIR, CONFIG, PATHS

config = Helper.load_json(r"configs\param_table.json5", typ="json5")
paths = Helper.load_json(r"paths.json")

# PROGRAM_NAME = "SCRAPE_BANK_RATE_KAUSTUBH"

# Logger
logger = get_forever_logger(name="scraper", log_dir=LOG_DIR)

BANK_CODES = [f"PSB_{i}" for i in range(1,13)]+["PVB_1","PVB_2","PVB_3","PVB_4","PVB_5","PVB_6","PVB_7","PVB_8","PVB_10","PVB_11","PVB_12","PVB_13","PVB_14","PVB_15","PVB_16","PVB_18","PVB_19","PVB_20","PVB_21","PVB_22"]
BANK_CODES = ["PVB_13"]

# Mailer().start_mail(PROGRAM_NAME,BANK_CODES)
final_dict = BankScraper.get_final_struct()
try:
    #scrape
    for code in BANK_CODES:
        if code not in CONFIG:
            continue
        bank_params = config[code]
        scraper = BankScraper(bank_params, logger,PATHS)
        result = scraper.run()
        final_dict["records"].append(result)
        # final_dict["registry"].update({code:bank_params["bank_name"]})
        time.sleep(3)
    
    #post-scrape
    # proce_dict = BankScraper.post_scrape(final_dict, POST_SCRAPE_OPS, logger)

except KeyboardInterrupt:
    logger.warning("Process Interrupted by User!")
    
except Exception as e:
    logger.error(f"Error in Main.py :[{type(e).__name__}] {e}")
    logger.debug(f"Traceback:\n{traceback.format_exc()}")

finally:
    Helper.save_json(final_dict, os.path.join(CCH_DIR, final_dict["metadata"]["cfname"]))
    # Helper.save_json(proce_dict,os.path.join(PROCESS_DIR, final_dict["metadata"]["pfname"]))
    logger.save("Saved Cached Data.")