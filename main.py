from selenium import webdriver
import  requests,os

from datetime import datetime
from app.utils import Helper
from app.program_logger import setup_logger
from app.action_executor import ActionExecutor
from app.operation_executor import OperationExecutor
from selenium.webdriver.chrome.options import Options


config = Helper.load_json(r"configs\params.json5", typ="json5")
paths = Helper.load_json(r"paths.json")


#constants
BANK_CODES = ["PSB_1","PSB_2","PSB_3","PSB_4","PSB_5","PSB_6","PSB_7","PSB_8","PSB_9","PSB_10","PSB_11","PSB_12"]
# BANK_CODES = ["PSB_12"]

LOG_DIR,CACHE_DIR,PROCESS_DIR = Helper.create_dirs(paths["output"],[paths["folders"]["log"],paths["folders"]["cache"],paths["folders"]["processed"]])
TODAY = datetime.now().strftime("%d%m%Y")


logger = setup_logger(name="scraper",log_dir=LOG_DIR)
final_dict = {"metadata":{"program":"main.py","timestamp":TODAY,"config":"params.json5","filename":f"up{TODAY}.json5"},"records":[]}

#selenium
for code in BANK_CODES:
    
    WEBSITE = config[code]
    logger.info(f"Scraping From Bank {WEBSITE["bank_name"]}")
    website = WEBSITE["base_url"]
    #website specific
    scrape_data = {"bank_name": WEBSITE["bank_name"],"base_url":WEBSITE["base_url"],"response": {}}
    executor = ActionExecutor(driver, logger,WEBSITE,paths)

    try:
        session = requests.Session()
        session.headers.update(WEBSITE["headers"])
        if code in ["PSB_12","PSB_7"]: resp = session.get(website, timeout=120)
        else: resp = session.get(website, timeout=25,verify=False)
            
        if resp.status_code not in [200, 201, 202, 203, 204, 206, 301, 302, 303, 307, 308]:
            logger.warning(f"{website} returned status: {resp.status_code}")
            raise Exception(f"{resp.status_code}")
        
        executor.attach_headers(driver,WEBSITE)
        driver.get(website)
        logger.info("Page fetched successfully")

        for idx, block in enumerate(WEBSITE["blocks"]):
            logger.info(f"Running Block: {idx}")
            data, timestamp = executor.execute_blocks(block)
            scrape_data["response"].update(data)

    except Exception as e:
        logger.error(f"Error scraping {WEBSITE['bank_name']}: {e}")
        scrape_data["response"] = {"error": str(e)}

    finally:
        driver.quit()

    final_dict["records"].append(scrape_data)

driver.quit()
Helper.save_json(final_dict,os.path.join(CACHE_DIR,f"cache_{TODAY}.json5"))
#post scraping ops
ops = OperationExecutor()
date_dict = ops.runner(final_dict,"extract_date")

#save the data
Helper.save_json(date_dict,os.path.join(PROCESS_DIR,f"process_{TODAY}.json5"))



    

for code in BANK_CODES:
    WEBSITE = config[code]
    logger.info(f"Scraping From Bank {WEBSITE['bank_name']}")
    website = WEBSITE["base_url"]
    scrape_data = {"bank_name": WEBSITE["bank_name"], "base_url": website, "response": {}}

    # Create a fresh driver per bank
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    try:
        session = requests.Session()
        session.headers.update(WEBSITE["headers"])
        timeout = 120 if code in ["PSB_12", "PSB_7"] else 25
        verify_ssl = WEBSITE.get("verify_ssl", False)  # configurable per bank
        resp = session.get(website, timeout=timeout, verify=verify_ssl)

        if resp.status_code not in [200, 201, 202, 203, 204, 206, 301, 302, 303, 307, 308]:
            logger.warning(f"{website} returned status: {resp.status_code}")
            raise Exception(f"{resp.status_code}")

        executor = ActionExecutor(driver, logger, WEBSITE, paths)
        executor.attach_headers(driver, WEBSITE)
        driver.get(website)
        logger.info("Page fetched successfully")

        for idx, block in enumerate(WEBSITE["blocks"]):
            logger.info(f"Running Block: {idx}")
            data, timestamp = executor.execute_blocks(block)
            scrape_data["response"].update(data)

    except Exception as e:
        logger.error(f"Error scraping {WEBSITE['bank_name']}: {e}")
        scrape_data["response"] = {"error": str(e)}

    finally:
        driver.quit()

    final_dict["records"].append(scrape_data)