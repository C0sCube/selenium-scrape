from app.action_executor import ActionExecutor
from app.operation_executor import OperationExecutor
from selenium.common.exceptions import NoSuchWindowException, WebDriverException, TimeoutException, InvalidSessionIdException
import traceback, pprint, hashlib, time
from datetime import datetime
from app.logger import get_global_logger

class BankScraper:
    def __init__(self):
        self.logger = get_global_logger()
        self.executor = ActionExecutor() # not inherit, call here!!
        self.operator = OperationExecutor()
        
        self.error_progs = ["*START*"]

    @staticmethod
    def get_final_struct():
        today = datetime.now()
        date, timestamp = today.strftime("%d%m%y"),today.strftime("%H%M")        
        return {
            "metadata": {
                "program": "main.py",
                "date": date,
                "start_time":timestamp,
                "config": "params_table.json5",
                "cfname": f"CACHE{date}T{timestamp}.json",
                "pfname": f"PROCESSED{date}T{timestamp}.json"
            },
            "records": [],
            "registry":{},
        }
        
    def load_driver(self, retries=3, delay=10):
        attempt = 0
        while attempt < retries:
            try:
                self.logger.info(f"Attempt {attempt + 1} to create driver...")
                self.executor.create_uc_driver()
                if not self.executor.driver:
                    self.logger.error("Driver creation returned None.")
                    return None

                self.executor.driver.set_page_load_timeout(50)
                self.logger.info("Driver created successfully.")
                return self.executor.driver  # Return the driver if successful

            except WebDriverException as e:
                self.logger.error(f"WebDriver error: {e}. Retrying...")
                self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
                attempt += 1
                time.sleep(delay)

            except Exception as e:
                self.logger.error(f"Unexpected error: {e}. Retrying...")
                self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
                attempt += 1
                time.sleep(delay)

        self.logger.error("Failed to create driver after multiple attempts.")
        return None

    
    def exit_driver(self):
        self.logger.info("Closing the Driver.")
        self.executor.driver.quit()
        self.logger.info("Driver Closed.")

    def run(self, bank_params):
        
        scraped_data = []
        
        try:
            self.logger.info(f"==========={bank_params['bank_type_code']}:{bank_params['bank_name']}===========")           
            self.executor.set_params(bank_params)
            self.executor.get_website()
            data = self.executor.execute_blocks()
            scraped_data.extend(data)
        
        except Exception as e:
            self.logger.error(f"Error in BankScraper.py {bank_params['bank_name']}: {type(e).__name__} {e}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            scraped_data = [{"error_Type": type(e).__name__, "error_Message": str(e),"error_from": "BankScraper.py"}]
            self.error_progs.append(f"BankName:{bank_params['bank_name']},ErrorType:{type(e).__name__},ErrorMessage:{str(e)}")
        
        return {
            "bank_name": bank_params["bank_name"],
            "bank_code": bank_params["bank_type_code"],
            "base_url": bank_params["base_url"],
            "scraped_data":scraped_data
        }

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
    
    @staticmethod
    def generate_prs_cache(data,fns):
        ops = OperationExecutor()
        prs_data = ops.runner(data,fns)
        return prs_data
    
            
        