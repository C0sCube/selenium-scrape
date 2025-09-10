from app.action_executor import ActionExecutor
from app.operation_executor import OperationExecutor
from selenium.common.exceptions import TimeoutException, InvalidSessionIdException
import time, traceback, threading, pprint, hashlib
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
        
        self.operator = OperationExecutor(logger)

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
                self.logger.notice("Page fetched successfully.")
            except TimeoutException:
                self.logger.error("Page load timed out. Attempting to stop...")
                try:
                    self.executor.driver.execute_script("window.stop();")
                except InvalidSessionIdException:
                    self.logger.error("Driver session lost during timeout handling.")
                    return {"error": "Driver crashed during load"}
            
            self.logger.info(f"========{self.bank_params['bank_name']}: {self.bank_params['bank_type_code']}========")

            actions = self.bank_params["blocks"]
            data = self.executor.execute_blocks(actions)
            self.scrape_data["scraped_data"].extend(data)

        except InvalidSessionIdException as e:
            self.logger.error(f"Driver session invalid for {self.bank_params['bank_name']}. Restart required.")
            self.scrape_data["scraped_data"] = [{"error_Type": "InvalidSessionId", "error_Message": str(e)}]
        
        except Exception as e:
            self.logger.error(f"Error in BankScraper.py {self.bank_params['bank_name']}: {type(e).__name__} {e}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            self.scrape_data["scraped_data"] = [{"error_Type": type(e).__name__, "error_Message": str(e),"error_from": "BankScraper.py"}]
        
        finally:
            try:
                self.executor.driver.quit()
            except Exception:
                pass
            self.executor.driver = None
        
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
    
    @staticmethod
    def dedupe_responses(result: dict) -> dict:
        if "scraped_data" not in result:
            return result
        
        for action in result["scraped_data"]:
            if "response" not in action:
                continue

            seen = set()
            unique = []
            for resp in action["response"]:
                if "value" in resp and resp.get("type") == "pdf": #pdf specific
                    val = resp["value"]
                    val_hash = hashlib.sha256(val.encode("utf-8")).hexdigest()
                    if val_hash not in seen:
                        seen.add(val_hash)
                        unique.append(resp)
                else:
                    unique.append(resp)
            action["response"] = unique
        return result
    
    @staticmethod
    def generate_cache_report(data, output_path="DepositRate_Comparison_Report.docx"):
        OperationExecutor.generate_cache_doc_report(data, output_path= output_path)
    
            
        