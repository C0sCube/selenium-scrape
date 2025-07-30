from selenium import webdriver
import json5, datetime, requests
import certifi

from app.program_logger import setup_logger
from app.action_executor import ActionExecutor
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver

options = Options()

logger = setup_logger(name="scraper")

with open(r"configs\params.json5") as f:
    config = json5.load(f)

options = Options()
driver = webdriver.Chrome(options=options)

BANK_CODES = ["PSB_1"] #"PSB_1","PSB_2","PSB_3","PSB_6","PSB_7","PSB_8","PSB_9","PSB_10","PSB_11","PSB_12"

final_dict = {
    "metadata":{
        "program":"main.py",
        "timestamp":None,
        "config":"params.json5"
    },
    "records":[]
    
}

for code in BANK_CODES:
    WEBSITE = config[code]
    website = WEBSITE["base_url"]
    
    scrape_data = {
                "bank_name": WEBSITE["bank_name"],
                "base_url":WEBSITE["base_url"],
                "response": {}
            }
    
    try:
        req = requests.get(website, timeout=20, verify=certifi.where())
        if req.status_code == '200':
            logger.info(f"Scraping From Bank {WEBSITE["bank_name"]}")
            executor = ActionExecutor(driver, logger)
            executor.attach_headers(driver,WEBSITE["headers"])
            driver.get(website)
            logger.notice("Page fetched successfully")
            
            for idx,block in enumerate(WEBSITE["blocks"]):
                print(f"Running Block: {idx}")
                data,timestamp = executor.execute_blocks(block)
                scrape_data["response"].update(
                    {f"block_{idx+1}":{
                    "timestamp":timestamp,
                    "scrape_data":data
                }})
            


    except requests.exceptions.RequestException as e:
        logger.error(f"Request critical error: {e}")
        logger.error(f"Skipping Bank {WEBSITE["bank_name"]}")
        scrape_data["response"] = {"error":e}
    
    final_dict["records"].append(scrape_data)
    
with open("example.json5","w+") as f:
    json5.dump(final_dict,f)
driver.quit()
