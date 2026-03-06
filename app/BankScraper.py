from app.action_executor import ActionExecutor
from app.operation_executor import  OperationExecutorLatest
from app.pdf_report import PDFReportBuilderPro
from selenium.common.exceptions import WebDriverException #type: ignore

# app/BankScraper.py
import traceback, time, hashlib, os, time, shutil
from copy import deepcopy
from datetime import datetime
from app.logger import get_global_logger, log_exceptions
from app.utils import Helper
from app.constants import load_config, get_session_dir, create_dir

class BankScraper:
    """Main controller for orchestrating scraping per bank."""

    def __init__(self):
        """
        Args:
            executor: Instance of ActionExecutor
            operator: Optional instance of OperationExecutor (for post ops)
            logger: Shared logger instance
        """
        self.executor = ActionExecutor()
        self.operator = OperationExecutorLatest()
        
        self.reporter = PDFReportBuilderPro()
        self.CONFIG = load_config()
        
        self.logger = get_global_logger()
        self.errors = []  # structured error store
        
        self.date_now = datetime.now()
        self.RUNTIME_PATH = create_dir(get_session_dir(), f"session_{self.date_now.strftime('%y%m%d_%H%M')}")
        self.SESSION_LATEST = create_dir(get_session_dir(), "session_latest")
        self.PREV_SCRP_JSN = os.path.join(self.SESSION_LATEST, "PROCESS_LATEST.json")
        self.ERROR_HTML_PATH = os.path.join(self.RUNTIME_PATH,"error_data.html")
        
        self.email_msg = None
        self.metadata = self.build_metadata(self.date_now)
        self.paths = self.build_paths(self.RUNTIME_PATH, self.metadata)
        
        
        BASE_TIMEOUT = 120
        SESSION_RETRIES = 3
        SESSION_RETRY_DELAY = 10
        DRIVER_PAGE_LOAD_TIMEOUT = 120

               
    # =====================================================
    # LIFECYCLE METHODS
    # =====================================================

    @log_exceptions(level="critical", return_value=False)
    def start_session(self, headless=False,minimized = False, retries=3, retry_delay=10):
        """Initialize Selenium driver with retries."""
        for attempt in range(retries):
            try:
                self.logger.info(f"Attempt {attempt + 1} to create driver...")
                self.executor.create_uc_driver(headless=headless, minimized=minimized)
                if not self.executor.driver:
                    raise RuntimeError("Driver creation returned None")

                self.executor.driver.set_page_load_timeout(120)
                self.logger.info("Driver created successfully.")
                return True

            except WebDriverException as e:
                self.logger.error(f"WebDriver error: {type(e).__name__}. Retrying...")
                self.logger.debug(traceback.format_exc())
                
                if self.executor.driver:
                    self.executor.driver.quit()
                    
                self.record_error(bank_name="GLOBAL", exception=e, source="DRIVER")
                time.sleep(retry_delay)
        self.logger.critical("Failed to create driver after multiple attempts.")
        return False

    @log_exceptions(level="warning")
    def close_session(self):
        if self.executor.driver:
            self.executor.driver.quit()
            self.logger.info("Driver closed successfully.")

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
        self.logger.info(f"=== {bank_params['bank_type_code']}: {bank_name} ===")

        scraped_data = []
        try:
            self.executor.set_params(bank_params)
            
            timeout = bank_params.get("base_timeout",120)
            self.executor.get_website(timeout)
            data = self.executor.execute_blocks()
            
            # Check each action block for errors
            for block in data:
                if not block.get("data_present") and block.get("response"):
                    for err in block["response"]:
                        if "error_type" in err:
                            self.record_error(
                                bank_name,
                                exception=None,
                                source=f"ACTION:{block.get('action')}",
                                note=f"Action failed: {err.get('error_message')}",
                                severity="ERROR"
                            )
                            
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
    
    def build_metadata(self,ts: datetime) -> dict:
        date = ts.strftime("%d%m%y")
        time = ts.strftime("%H%M")

        return {
            "date": date,
            "start_time": time,
            "cfname": f"CACHE{date}T{time}.json",
            "pfname": f"PROCESS{date}T{time}.json"
        }
    
    def build_paths(self,runtime_path, metadata):
        date = metadata["date"]
        time = metadata["start_time"]

        return {
            #json cache/process
            "cache": os.path.join(runtime_path, metadata["cfname"]),
            "process": os.path.join(runtime_path, metadata["pfname"]),
            
            #comparison path
            "compare_json": os.path.join(runtime_path, f"COMPARE{date}T{time}.json"),
            "compare_xlsx": os.path.join(runtime_path, f"COMPARE{date}T{time}.xlsx"),
            
            #error
            "error_html": os.path.join(runtime_path, f"ERROR_SUMMARY_{date}T{time}.html"),
            
            #report_paths
            "pdf_archive": os.path.join(runtime_path, f"SCRAPE-REPORT-{date}T{time}.pdf"),
            "pdf_latest": os.path.join(runtime_path, "SCRAPE-REPORT.pdf"),
            "xlsx_archive": os.path.join(runtime_path, f"SCRAPE-REPORT-{date}T{time}.xlsx"),
            "xlsx_latest": os.path.join(runtime_path, "SCRAPE-REPORT.xlsx"),
        }

    def get_final_struct(self):
        final_dict = {
            "metadata": self.metadata,
            "registry": {},
            "records": []
        }
        
        return final_dict
            
    @staticmethod
    @log_exceptions(level="error")
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
    
    def record_error(self, bank_name, exception=None, source="BLOCK", note=None, severity="ERROR"):
        """
        Record structured errors or 'no data' notes for each bank.
        Args:
            bank_name (str): Name of the bank.
            exception (Exception, optional): Exception object if any.
            source (str): 'DRIVER', 'BLOCK', 'DATA', etc.
            note (str): Optional human-readable message.
            severity (str): 'ERROR' or 'INFO'
        """
        error_entry = {
            "bank": bank_name,
            "source": source,
            "severity": severity,
            "type": type(exception).__name__ if exception else "None",
            "message": str(exception) if exception else note,
            "note": note or "",
            "traceback": traceback.format_exc() if exception else "",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        self.errors.append(error_entry)

        # Logging appropriately
        if severity == "ERROR":
            self.logger.error(f"[{source}] [{bank_name}] {error_entry['type']}: {error_entry['message']}")
        else:
            self.logger.info(f"[{source}] [{bank_name}] {error_entry['note']}")

    def export_error_log(self):
        """Export human-readable text and HTML error summaries. Always returns file paths."""
        if not self.errors:
            self.logger.info("No errors or missing-data notes to export.")
            return None

        errors,infos = [e for e in self.errors if e["severity"] == "ERROR"],[e for e in self.errors if e["severity"] == "INFO"]
        # --- Build HTML Summary ---
        html_lines = [
            "<html><body style='font-family:Arial, sans-serif;'>",
            f"<h2> Scraper Execution Summary — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</h2>",
            "<hr>"
        ]

        if infos:
            html_lines.append("<h3 style='color:#c9a300;'>🟡 Banks With No Data</h3><ul>")
            for e in infos:  html_lines.append(f"<li><b>{e['bank']}</b> → {e['note']}</li>")
            html_lines.append("</ul>")

        if errors:
            html_lines.append("<h3 style='color:#b30000;'>🟥 Banks With Errors</h3><ul>")
            for e in errors: html_lines.append(f"<li><b>{e['bank']}</b> ({e['source']}) — {e['message']}</li>")
            html_lines.append("</ul>")

        html_lines.append("<hr><p style='font-size:10pt;color:#777;'>Auto-generated scraper error summary.</p></body></html>")

        if self.ERROR_HTML_PATH:
            Helper.save_text("\n".join(html_lines), self.ERROR_HTML_PATH)
        return self.ERROR_HTML_PATH

    @log_exceptions(level="critical", raise_error=True)
    def process_cache(self, final_dict):
        """Process, compare, and update cache files."""

        pipeline = {
            "primary": [
                ["normalize_df", "value", "norm_table", "table_html"],
                ["sha1", "value", "SHA_ONE", "pdf"],
            ],
            "secondary": [["sha1", "norm_table", "SHA_ONE"]],
        }
        
        paths = self.paths

        try:
            # ===== Stage 1: Process Current Cache =====
            self.logger.info("Starting cache processing pipeline...")
            processed_cache = self.operator.runner(final_dict, pipeline)
            baseline = deepcopy(processed_cache)

            Helper.save_json(processed_cache, paths["process"])
            self.logger.info(f"Processed cache saved at: {paths['process']}")

            # ===== Stage 2: Compare with Previous Cache =====
            try:
                if os.path.exists(self.PREV_SCRP_JSN):
                    old_data = Helper.load_json(self.PREV_SCRP_JSN)

                    if isinstance(old_data, dict):
                        self.logger.info("Loaded previous processed cache for comparison.")
                        comparison = self.operator.process_comparison(
                            old_data, processed_cache, key="SHA_ONE"
                        )

                        Helper.save_json(comparison, paths["compare_json"])
                        self.logger.debug(f"Comparison JSON saved: {paths['compare_json']}")

                        self.operator.generate_comparison_report(
                            comparison, paths["compare_xlsx"]
                        )
                        self.logger.debug(
                            f"Comparison report → {paths['compare_xlsx']}"
                        )

                        ots = old_data["metadata"]["pfname"].replace("PROCESS", "").replace(".json", "")
                        nts = processed_cache["metadata"]["pfname"].replace("PROCESS", "").replace(".json", "")
                        self.email_msg = f"Scraped between {ots} and {nts}"
                    else:
                        self.logger.warning("Invalid previous cache format. Skipping comparison.")
                else:
                    self.logger.warning("No previous cache found for comparison.")

            except Exception as e:
                self.logger.warning(f"Comparison failed: {type(e).__name__} - {e}")

            # ===== Stage 3: Update Baseline =====
            Helper.save_json(baseline, self.PREV_SCRP_JSN)
            self.logger.debug(
                f"Updated baseline processed cache for next run at: {self.PREV_SCRP_JSN}"
            )

            self.logger.info("Cache Process & Comparison Done.")

        except Exception as e:
            self.logger.error(f"process_cache failed: {type(e).__name__} - {e}")
            self.logger.debug(traceback.format_exc())
            raise
    
    def create_scrape_report(self, cache_data, report_type="pdf", rewrite=False):
      
        paths = self.paths

        if report_type.lower() in ("pdf", "both"):
            #write report in pdf
            self.reporter.build(cache_data, paths["pdf_archive"])
            self.logger.info(f"PDF archived at: {paths['pdf_archive']}")

            if rewrite or not os.path.exists(paths["pdf_latest"]):
                shutil.copy(paths["pdf_archive"], paths["pdf_latest"])

        if report_type.lower() in ("xlsx", "both"):
            #write report in excel
            self.reporter.write_excel_report(cache_data, paths["xlsx_archive"])
            self.logger.info(f"Excel archived at: {paths['xlsx_archive']}")

            if rewrite or not os.path.exists(paths["xlsx_latest"]):
                shutil.copy(paths["xlsx_archive"], paths["xlsx_latest"])
    
    def runner(self,bank_codes):
       
        final_dict  = self.get_final_struct()
        
        for code in bank_codes:
            if code not in self.CONFIG:
                self.logger.error(f"Code: {code} not found in config. Skipping...")
                continue

            bank_params = self.CONFIG[code]

            try:
                result = self.scrape_bank(bank_params)
                result = BankScraper.dedupe_responses(result)

                #Detect Banks with No Data
                scraped_blocks = result.get("scraped_data", [])
                if not scraped_blocks or all(
                    not block.get("data_present", False) for block in scraped_blocks
                ):
                    self.record_error(
                        bank_params.get("bank_name"),
                        source="DATA",
                        note="No data extracted — possible empty table or maintenance page.",
                        severity="INFO"
                    )

            except Exception as e:
                self.record_error(bank_params.get("bank_name"), e)
                result = {
                    "bank_code": code,
                    "scraped_data": [{"error": str(e)}]
                }

            final_dict["records"].append(result)

        # cache save is now explicit
        Helper.save_json(final_dict, self.paths["cache"])
        self.logger.info(f"Cache saved at: {self.paths['cache']}")

        self.close_session()
        self.logger.info("Closing Session !!")

        return final_dict
    
            
        