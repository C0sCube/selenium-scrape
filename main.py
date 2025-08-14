import  requests,os, warnings,time
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow-specific
warnings.filterwarnings('ignore') 
from datetime import datetime
from app.utils import Helper
from app.program_logger import get_forever_logger
from app.action_executor import ActionExecutor
from app.operation_executor import OperationExecutor

config = Helper.load_json(r"configs\param_table.json5", typ="json5")
paths = Helper.load_json(r"paths.json")


#constants
BANK_CODES = ["PSB_1","PSB_2","PSB_3","PSB_4","PSB_5","PSB_6","PSB_7","PSB_8","PSB_9","PSB_10","PSB_11","PSB_12","PSB_13"]
BANK_CODES = ["PSB_13"]

LOG_DIR,CACHE_DIR,PROCESS_DIR = Helper.create_dirs(paths["output"],["logs","cache","processed"])
TODAY = datetime.now().strftime("%d%m%Y")


logger = get_forever_logger(name="scraper",log_dir=LOG_DIR)
final_dict = {"metadata":{"program":"main.py","timestamp":TODAY,"config":"params.json5","filename":f"up{TODAY}.json5"},"records":[]}

#selenium
for code in BANK_CODES:
    
    WEBSITE = config[code]
    logger.info(f"Scraping From Bank {WEBSITE["bank_name"]}")
    website = WEBSITE["base_url"]
    #website specific
    scrape_data = {"bank_name": WEBSITE["bank_name"],"bank_code":WEBSITE["bank_type_code"],"base_url":WEBSITE["base_url"],"scraped_data": []}
    executor = ActionExecutor(logger,WEBSITE,paths)
    try:
        # executor.attach_headers()
        executor.driver.get(website)
        logger.info("Page fetched successfully")

        for idx, block in enumerate(WEBSITE["blocks"]):
            logger.info(f"Running Block: {idx}")
            data, timestamp = executor.execute_blocks(block)
            
            for d in data:
                scrape_data["scraped_data"].append(d)

    except Exception as e:
        logger.error(f"Error scraping {WEBSITE['bank_name']}: {e}")
        scrape_data["scraped_data"] = {"error": str(e)}

    finally:
        time.sleep(2)
        executor.driver.quit()
        time.sleep(2)

    final_dict["records"].append(scrape_data)

HRM = Helper.get_timestamp(sep="")

Helper.save_json(final_dict,os.path.join(CACHE_DIR,f"cache_{TODAY}_{HRM}.json5"))
#post scraping ops
ops = OperationExecutor()

function_to_execute= {
    "sha256":"_generate_hash_sha256",
    "sha1":"_generate_hash_sha1",
    "original_value":"boomerang" #default
}

date_dict = ops.runner(final_dict,function_to_execute)

#save the data
Helper.save_json(date_dict,os.path.join(PROCESS_DIR,f"process_{TODAY}_{HRM}.json5"))
