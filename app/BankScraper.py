from app.action_executor import ActionExecutor
from selenium.common.exceptions import TimeoutException

import time, traceback, threading
from datetime import datetime

class BankScraper:
    def __init__(self, bank_params, logger, paths):
        self.bank_params = bank_params
        self.logger = logger
        self.paths = paths
        self.scrape_data = {
            "bank_name": bank_params["bank_name"],
            "bank_code": bank_params["bank_type_code"],
            "base_url": bank_params["base_url"],
            "scraped_data": []
        }
        self.executor = ActionExecutor(logger, bank_params, paths) # not inherit, call here!!

    @staticmethod
    def get_final_struct():
        
        date = datetime.now().strftime("%d%m%Y")
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        
        return {
            "metadata": {
                "program": "main.py",
                "date": date,
                "start_time":timestamp,
                "end_time":"",
                "config": "params_table.json5",
                "filename": f"{timestamp}_cache.json"
            },
            "records": [],
            "registry":{},
        }
    
    def run(self):
        try:
            self.executor.create_uc_driver()
            self.logger.info("Driver Created.")
            try:
                self.executor.driver.set_page_load_timeout(50)
                self.executor.driver.get(self.bank_params["base_url"])
            except TimeoutException:
                self.logger.error("Page load timed out. Attempting to stop...")
                self.executor.driver.execute_script("window.stop();")

            self.logger.notice(f"Page fetched successfully.")
            self.logger.info(f"Scraping {self.bank_params['bank_name']}: {self.bank_params["bank_type_code"]}")
                   
            for idx, block in enumerate(self.bank_params["blocks"]):
                self.logger.info(f"Running Block: {idx}")
                data = self.executor.execute_blocks(block)
                self.scrape_data["scraped_data"].extend(data)

        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            self.logger.error(f"Error in BankScraper.py {self.bank_params['bank_name']}:[{error_type}] {error_msg}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            self.scrape_data["scraped_data"] = [{"error_Type": error_type, "error_Message": error_msg,"error_from": "BankScraper.py"}]
        finally:
            self.executor.driver.quit()
        return self.scrape_data
