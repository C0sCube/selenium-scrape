# import  requests,os, warnings,time, ssl
# os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow-specific
# warnings.filterwarnings('ignore') 
# from datetime import datetime
# from app.utils import Helper
# from app.program_logger import get_forever_logger
# from app.action_executor import ActionExecutor
# from app.operation_executor import OperationExecutor

# ssl._create_default_https_context = ssl._create_stdlib_context

# config = Helper.load_json(r"configs\param_table.json5", typ="json5")
# paths = Helper.load_json(r"paths.json")


# #constants
# BANK_CODES = ["PSB_1","PSB_2","PSB_3","PSB_4","PSB_5","PSB_6","PSB_7","PSB_8","PSB_9","PSB_10","PSB_11","PSB_12","PSB_13"]
# # BANK_CODES = ["PSB_12"]

# LOG_DIR,CACHE_DIR,PROCESS_DIR = Helper.create_dirs(paths["output"],["logs","cache","processed"])
# TODAY = datetime.now().strftime("%d%m%Y")


# logger = get_forever_logger(name="scraper",log_dir=LOG_DIR)
# final_dict = {"metadata":{"program":"main.py","timestamp":TODAY,"config":"params.json5","filename":f"up{TODAY}.json5"},"records":[]}

# #selenium
# for code in BANK_CODES:
    
#     BANK_PARAMS = config[code]
#     website = BANK_PARAMS["base_url"]
#     #website specific
#     scrape_data = {"bank_name": BANK_PARAMS["bank_name"],"bank_code":BANK_PARAMS["bank_type_code"],"base_url":BANK_PARAMS["base_url"],"scraped_data": []}
#     try:
#         executor = ActionExecutor(logger,BANK_PARAMS,paths)
#         # executor.create_driver()
#         executor.create_uc_driver()
#         logger.info("Driver Created.")
#         executor.driver.get(website)
#         logger.info("Page fetched successfully")
#         logger.info(f"Scraping From Bank {BANK_PARAMS["bank_name"]}")

#         for idx, block in enumerate(BANK_PARAMS["blocks"]):
#             logger.info(f"Running Block: {idx}")
#             data, timestamp = executor.execute_blocks(block)
            
#             for d in data:
#                 scrape_data["scraped_data"].append(d)

#     except Exception as e:
#         logger.error(f"Error scraping {BANK_PARAMS['bank_name']}: {e}")
#         scrape_data["scraped_data"] = {"error": str(e)}

#     finally:
#         executor.driver.quit()
#         time.sleep(2)

#     final_dict["records"].append(scrape_data)

# HRM = Helper.get_timestamp(sep="")

# Helper.save_json(final_dict,os.path.join(CACHE_DIR,f"cache_{TODAY}_{HRM}.json5"))

import  requests,os, warnings,time, ssl
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow-specific
warnings.filterwarnings('ignore') 
from datetime import datetime
from app.utils import Helper
from app.program_logger import get_forever_logger
from app.action_executor import ActionExecutor
from app.operation_executor import OperationExecutor
from app.BankScraper import BankScraper

ssl._create_default_https_context = ssl._create_stdlib_context

config = Helper.load_json(r"configs\param_table.json5", typ="json5")
paths = Helper.load_json(r"paths.json")

# Constants
LOG_DIR, CACHE_DIR, PROCESS_DIR = Helper.create_dirs(paths["output"], ["logs", "cache", "processed"])
TODAY = datetime.now().strftime("%d%m%Y")
HRM = Helper.get_timestamp(sep="")
FILE_NAME = f"cache_{TODAY}_{HRM}.json5"

# Logger
logger = get_forever_logger(name="scraper", log_dir=LOG_DIR)

# Final output structure
final_dict = {
    "metadata": {
        "program": "main.py",
        "timestamp": TODAY,
        "config": "params_table.json5",
        "filename": FILE_NAME
    },
    "records": []
}

BANK_CODES = [f"PSB_{i}" for i in range(1,13)]
BANK_CODES = ["PSB_5"]

for code in BANK_CODES:
    bank_params = config[code]
    scraper = BankScraper(bank_params, logger,paths)
    result = scraper.run()
    final_dict["records"].append(result)
    time.sleep(2)

Helper.save_json(final_dict, os.path.join(CACHE_DIR, FILE_NAME))

# #post scraping ops
# ops = OperationExecutor()

# function_to_execute= {
#     "sha256":"_generate_hash_sha256",
#     "sha1":"_generate_hash_sha1",
#     "original_value":"boomerang" #default
# }

# date_dict = ops.runner(final_dict,function_to_execute)

# #save the data
# Helper.save_json(date_dict,os.path.join(PROCESS_DIR,f"process_{TODAY}_{HRM}.json5"))