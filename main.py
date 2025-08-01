from selenium import webdriver
import json5, requests,os

from datetime import datetime
from app.program_logger import setup_logger
from app.action_executor import ActionExecutor
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.chrome.webdriver import WebDriver

with open(r"configs\params.json5") as f:
    config = json5.load(f)
with open(r"paths.json") as f:
    paths = json5.load(f)


#constants
BANK_CODES = ["PSB_1","PSB_2","PSB_3","PSB_4","PSB_5","PSB_6","PSB_7","PSB_8","PSB_9","PSB_10","PSB_11","PSB_12"] #"PSB_1","PSB_2","PSB_3","PSB_6","PSB_7","PSB_8","PSB_9","PSB_10","PSB_11","PSB_12"
BANK_CODES = ["PSB_3"]
LOG_DIR = os.path.join(paths["output"],paths["folders"]["log"])
CACHE_DIR = os.path.join(paths["output"],paths["folders"]["cache"])
TODAY = datetime.now().strftime("%d%m%Y")

os.makedirs(LOG_DIR,exist_ok=True)
os.makedirs(CACHE_DIR,exist_ok=True)

logger = setup_logger(name="scraper",log_dir=LOG_DIR)
final_dict = {
    "metadata":{
        "program":"main.py",
        "timestamp":None,
        "config":"params.json5"
    },
    "records":[]
    
}

#selenium
options = Options()
driver = webdriver.Chrome(options=options)

for code in BANK_CODES:
    WEBSITE = config[code]
    website = WEBSITE["base_url"]
    
    scrape_data = {"bank_name": WEBSITE["bank_name"],"base_url":WEBSITE["base_url"],"response": {}}
    
    logger.info(f"Scraping From Bank {WEBSITE["bank_name"]}")
    executor = ActionExecutor(driver, logger,WEBSITE,paths)

    try:
        resp = requests.get(website, headers=WEBSITE["headers"], timeout=5, verify=False)
        if resp.status_code not in [200, 201, 202, 203, 204, 206, 301, 302, 303, 307, 308]:
            logger.warning(f"{website} returned status: {resp.status_code}")
            raise Exception(f"{resp.status_code}")
        
        executor.attach_headers(driver,WEBSITE["headers"])
        driver.get(website)
        logger.notice("Page fetched successfully")
        
    except Exception as e:
        logger.error(f"Requests error: {e}")
        logger.notice(f"Skipping for {WEBSITE["bank_name"]}")
        scrape_data["response"] = {"error": str(e)}
        final_dict["records"].append(scrape_data)
        
        continue
    
    for idx,block in enumerate(WEBSITE["blocks"]):
        print(f"Running Block: {idx}")
        data,timestamp = executor.execute_blocks(block)
        # scrape_data["response"].update(
        #     {f"block_{idx+1}":{
        #     "timestamp":timestamp,
        #     "scrape_data":data
        # }})
        scrape_data["response"].update(data)
    
    final_dict["records"].append(scrape_data)
    
with open(os.path.join(CACHE_DIR,f"cache_{TODAY}.json5"),"w+") as f:
    json5.dump(final_dict,f)
driver.quit()
