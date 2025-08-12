import  requests,os, warnings,time
warnings.filterwarnings('ignore') 
from datetime import datetime
from app.utils import Helper
from app.program_logger import setup_logger
from app.action_executor import ActionExecutor
from app.operation_executor import OperationExecutor

config = Helper.load_json(r"configs\param_table.json5", typ="json5")
paths = Helper.load_json(r"paths.json")


#constants
# BANK_CODES = ["PSB_1","PSB_2","PSB_3","PSB_4","PSB_5","PSB_6","PSB_7","PSB_8","PSB_9","PSB_10","PSB_11","PSB_12"]
BANK_CODES = ["PSB_1","PSB_2","PSB_3"]

LOG_DIR,CACHE_DIR,PROCESS_DIR = Helper.create_dirs(paths["output"],["logs","cache","processed"])
TODAY = datetime.now().strftime("%d%m%Y")


logger = setup_logger(name="scraper",log_dir=LOG_DIR)
final_dict = {"metadata":{"program":"main.py","timestamp":TODAY,"config":"params.json5","filename":f"up{TODAY}.json5"},"records":[]}

#selenium
for code in BANK_CODES:
    
    WEBSITE = config[code]
    logger.info(f"Scraping From Bank {WEBSITE["bank_name"]}")
    website = WEBSITE["base_url"]
    #website specific
    scrape_data = {"bank_name": WEBSITE["bank_name"],"bank_code":WEBSITE["bank_type_code"],"base_url":WEBSITE["base_url"],"scraped_data": {}}
    executor = ActionExecutor(logger,WEBSITE,paths)
    try:
        # executor.attach_headers()
        executor.driver.get(website)
        logger.info("Page fetched successfully")

        for idx, block in enumerate(WEBSITE["blocks"]):
            logger.info(f"Running Block: {idx}")
            data, timestamp = executor.execute_blocks(block)
            scrape_data["scraped_data"].update(data)

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
date_dict = ops.runner(final_dict,"extract_date")

#save the data
Helper.save_json(date_dict,os.path.join(PROCESS_DIR,f"process_{TODAY}_{HRM}.json5"))
