from app.action_executor import ActionExecutor
from app.operation_executor import OperationExecutor
from selenium.common.exceptions import TimeoutException

import time, traceback, threading, pprint
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
        timestamp = datetime.now().strftime("%y-%m-%dT%H-%M-%S")
        
        return {
            "metadata": {
                "program": "main.py",
                "date": date,
                "start_time":timestamp,
                "config": "params_table.json5",
                "cfname": f"{timestamp}_cache.json",
                "pfname": f"{timestamp}_process.json"
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
                self.logger.notice(f"Page fetched successfully.")
            except TimeoutException:
                self.logger.error("Page load timed out. Attempting to stop...")
                self.executor.driver.execute_script("window.stop();")

            self.logger.info(f"========{self.bank_params['bank_name']}: {self.bank_params["bank_type_code"]}========")
                   
            # for idx, block in enumerate(self.bank_params["blocks"]):
            #     self.logger.info(f"Running Block: {idx}")
            #     data = self.executor.execute_blocks(block)
            #     self.scrape_data["scraped_data"].extend(data)
            actions = self.bank_params["blocks"]
            data = self.executor.execute_blocks(actions)
            self.scrape_data["scraped_data"].extend(data)

        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            self.logger.error(f"Error in BankScraper.py {self.bank_params['bank_name']}:[{error_type}] {error_msg}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            self.scrape_data["scraped_data"] = [{"error_Type": error_type, "error_Message": error_msg,"error_from": "BankScraper.py"}]
        finally:
            self.executor.driver.close()
            del self.executor.driver
            
        return self.scrape_data

    @staticmethod
    def post_scrape(data: dict, ops_rules: dict, logger=None) -> dict:
        processed_data = {}

        if not ops_rules:
            if logger:
                logger.notice("No post-scrape operations defined. Skipping.")
            return processed_data

        if logger:
            logger.notice(f"Running post-scrape ops:\n{pprint.pformat(ops_rules)}")

        try:
            ops = OperationExecutor()
            processed_data = ops.runner(data, ops_rules)

        except Exception as e:
            if logger:
                logger.error(f"[PostScrape] Error: {type(e).__name__} - {e}")
                logger.debug(f"[PostScrape] Traceback:\n{traceback.format_exc()}")
            processed_data = {"error": str(e)}

        return processed_data   