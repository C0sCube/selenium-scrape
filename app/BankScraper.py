from app.action_executor import ActionExecutor
from app.operation_executor import OperationExecutor
from selenium.common.exceptions import NoSuchWindowException, WebDriverException, TimeoutException, InvalidSessionIdException
import traceback, pprint, hashlib, time
from datetime import datetime
from app.logger import get_global_logger


# app/BankScraper.py
import traceback, time, hashlib, pprint, os
from datetime import datetime
from selenium.common.exceptions import WebDriverException
from app.logger import get_global_logger
from app.utils import Helper
from app.constants import POST_SCRAPE_OPS, CONFIG

class BankScraper:
    """Main controller for orchestrating scraping per bank."""

    def __init__(self,path):
        """
        Args:
            executor: Instance of ActionExecutor
            operator: Optional instance of OperationExecutor (for post ops)
            logger: Shared logger instance
        """
        self.executor = ActionExecutor()
        self.operator = OperationExecutor()
        self.logger = get_global_logger()
        self.errors = []  # structured error store
        
        self.RUNTIME_PATH = path
        self.CONFIG = CONFIG

    # =====================================================
    # LIFECYCLE METHODS
    # =====================================================

    def start_session(self, headless=False, retries=3, retry_delay=10):
        """Initialize Selenium driver with retries."""
        attempt = 0
        while attempt < retries:
            try:
                self.logger.notice(f"Attempt {attempt + 1} to create driver...")
                self.executor.create_uc_driver(headless=headless)
                if not self.executor.driver:
                    raise RuntimeError("Driver creation returned None")

                self.executor.driver.set_page_load_timeout(50)
                self.logger.save("Driver created successfully.")
                return True
            except WebDriverException as e:
                self.logger.error(f"WebDriver error: {e}. Retrying...")
                attempt += 1
                time.sleep(retry_delay)
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}. Retrying...")
                self.logger.debug(traceback.format_exc())
                attempt += 1
                time.sleep(retry_delay)
        self.logger.critical("Failed to create driver after multiple attempts.")
        return False

    def close_session(self):
        """Safely quit driver session."""
        try:
            if self.executor.driver:
                self.logger.info("Closing browser session...")
                self.executor.driver.quit()
                self.logger.save("Driver closed successfully.")
        except Exception as e:
            self.logger.warning(f"Error while closing driver: {e}")

    # =====================================================
    # SCRAPING LOGIC
    # =====================================================

    def scrape_all(self, configs: dict):
        """Scrape all banks defined in the config."""
        results = []
        for bank_code, params in configs.items():
            try:
                result = self.scrape_bank(params)
                results.append(result)
            except Exception as e:
                self.record_error(bank_name=params.get("bank_name"), exception=e)
        return results

    def scrape_bank(self, bank_params: dict) -> dict:
        """Scrape a single bank using its parameters."""
        bank_name = bank_params.get("bank_name")
        self.logger.notice(f"=== {bank_params['bank_type_code']}: {bank_name} ===")

        scraped_data = []
        try:
            self.executor.set_params(bank_params)
            self.executor.get_website()
            data = self.executor.execute_blocks()
            scraped_data.extend(data)
        except Exception as e:
            self.record_error(bank_name, e)
            scraped_data = [{
                "error_Type": type(e).__name__,
                "error_Message": str(e),
                "error_from": "BankScraper.scrape_bank"
            }]
        finally:
            # always clean session between banks
            if self.executor.driver:
                self.executor.driver.delete_all_cookies()

        return {
            "bank_name": bank_name,
            "bank_code": bank_params.get("bank_type_code"),
            "base_url": bank_params.get("base_url"),
            "scraped_data": scraped_data
        }

    # =====================================================
    # UTILITY + STATIC METHODS
    # =====================================================

    @staticmethod
    def get_final_struct():
        """Create a fresh data structure for current run."""
        now = datetime.now()
        date, timestamp = now.strftime("%d%m%y"), now.strftime("%H%M")
        return {
            "metadata": {
                "date": date,
                "start_time": timestamp,
                "cfname": f"CACHE{date}T{timestamp}.json",
                "pfname": f"PROCESS{date}T{timestamp}.json"
            },
            "records": [],
            "registry": {}
        }

    @staticmethod
    def dedupe_responses(result: dict) -> dict:
        """Remove duplicate hashes. Currently Used only for PDF data"""
        if "scraped_data" not in result:
            return result

        for action in result["scraped_data"]:
            if "response" not in action:
                continue
            seen = set()
            unique = []
            for resp in action["response"]:
                if "value" in resp and resp.get("type") == "pdf":
                    val_hash = hashlib.sha256(resp["value"].encode("utf-8")).hexdigest()
                    if val_hash not in seen:
                        seen.add(val_hash)
                        unique.append(resp)
                else:
                    unique.append(resp)
            action["response"] = unique
        return result

    def record_error(self, bank_name, exception):
        """Capture and log structured errors."""
        error_entry = {
            "bank": bank_name,
            "type": type(exception).__name__,
            "message": str(exception),
            "traceback": traceback.format_exc(),
            "timestamp": datetime.now().isoformat()
        }
        self.errors.append(error_entry)
        self.logger.error(f"[{bank_name}] {type(exception).__name__}: {exception}")

    # def export_error_log(self, path):
    #     """Write all captured errors to a text file."""
    #     if not self.errors:
    #         self.logger.info("No errors captured this run.")
    #         return
    #     Helper.save_text(
    #         [f"{e['timestamp']} | {e['bank']} | {e['type']} | {e['message']}" for e in self.errors],
    #         path,
    #         mode='w'
    #     )
    #     self.logger.save(f"Error log written to {path}")

    def process_cache(self, final_dict, save_path, prev_path):
        try:
            processed_cache = self.operator.runner(final_dict, POST_SCRAPE_OPS["sha1"])
            import copy
            paste_cache = copy.deepcopy(processed_cache)

            Helper.save_json(processed_cache, save_path)
            self.logger.save(f"Processed cache saved at: {save_path}")

        
            timestamp = datetime.now().strftime("%d%m%yT%H%M")
            compare_path = os.path.join(self.RUNTIME_PATH, f"COMPARE{timestamp}.json")
            excel_path = os.path.join(self.RUNTIME_PATH, f"RATE_COMPARISON_{timestamp}.xlsx")

            if os.path.exists(prev_path):
                try:
                    old_data = Helper.load_json(prev_path)
                    if isinstance(old_data, dict):
                        self.logger.notice("Loaded previous processed cache for comparison.")
                        comparison = self.operator.process_comparison(old_data, processed_cache, key="sha1")
                        Helper.save_json(comparison, compare_path)
                        self.operator.generate_sorted_excel_report(comparison, excel_path)
                        self.logger.save(f"Comparison report generated:\nJSON → {compare_path}\nExcel → {excel_path}")
                    # else:
                    #     self.logger.warning("Invalid previous processed cache format. Skipping comparison.")
                except Exception as e:
                    self.logger.warning(f"Comparison failed: {type(e).__name__} - {e}")
            else:
                self.logger.warning("No previous processed cache found for comparison.")

            # Update baseline for next run
            Helper.save_json(paste_cache, prev_path)
            self.logger.notice(f"Updated baseline processed cache for next run at: {prev_path}")

        except Exception as e:
            self.logger.error(f"process_cache failed: {type(e).__name__} - {e}")
            self.logger.debug(traceback.format_exc())
            
    def generate_doc_report(self, cache_data):
        """Generate DOCX report for the cache."""
        try:
            timestamp = datetime.now().strftime("%d%m%yT%H%M")
            doc_path = os.path.join(self.RUNTIME_PATH, f"SCRAPE_REPORT_{timestamp}.docx")
            self.operator.generate_cache_doc_report(cache_data, doc_path)
            self.logger.save(f"Cache DOCX report generated at: {doc_path}")
        except Exception as e:
            self.logger.error(f"generate_doc_report failed: {type(e).__name__} - {e}")
            self.logger.debug(traceback.format_exc())

    def runner(self,bank_codes):
        
        final_dict = BankScraper.get_final_struct()
        for code in bank_codes:
            if code not in CONFIG:
                self.logger.error(f"Code: {code} not found in config. Skipping...")
                continue
            bank_params = CONFIG[code]
            
            #set correct driver
            # is_headless = bank_params.get("headless",True)
            # self.executor.driver = self.executor.headless_driver if is_headless else self.executor.visible_driver
            
            try:
                result = self.scrape_bank(bank_params)
                result = BankScraper.dedupe_responses(result)
            except Exception as e:
                self.record_error(bank_params.get("bank_name"), e)
                result = {"bank_code": code, "scraped_data": [{"error": str(e)}]}
            final_dict["records"].append(result)

        return final_dict
        pass
    
            
        