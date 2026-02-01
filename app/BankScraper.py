from app.action_executor import ActionExecutor
from app.operation_executor import  OperationExecutorLatest
from app.pdf_report import PDFReportBuilderPro
from selenium.common.exceptions import WebDriverException

# app/BankScraper.py
import traceback, time, hashlib, pprint, os, time, shutil
from copy import deepcopy
from datetime import datetime
from selenium.common.exceptions import WebDriverException
from app.logger import get_global_logger, log_exceptions
from app.utils import Helper
from app.constants import load_config

class BankScraper:
    """Main controller for orchestrating scraping per bank."""

    def __init__(self, runtime_path = "",session_latest_path = ""):
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
        
        self.RUNTIME_PATH = runtime_path#Helper.create_dir(OUTPUT_PATH, "session", f"session_{datetime.now().strftime('%y%m%d_%H%M')}")
        self.SESSION_LATEST = session_latest_path #Helper.create_dir(SESSION_ROOT, "session_latest")
        
        self.pdf_path = None
        self.xls_path = None
        self.prev_path = os.path.join(self.SESSION_LATEST, "PROCESS_LATEST.json")
        
        self.compare_path = None
        self.cache_path = None
        self.process_path = None
        self.comparison_xlsx = None
        self.email_msg = None #post comparison
        self.error_html_path = None
        
        # self.final_dict = 

        

    # =====================================================
    # LIFECYCLE METHODS
    # =====================================================

    @log_exceptions(level="critical", return_value=False)
    def start_session(self, headless=False,minimized = False, retries=3, retry_delay=10):
        """Initialize Selenium driver with retries."""
        for attempt in range(retries):
            try:
                self.logger.notice(f"Attempt {attempt + 1} to create driver...")
                self.executor.create_uc_driver(headless=headless, minimized=minimized)
                if not self.executor.driver:
                    raise RuntimeError("Driver creation returned None")

                self.executor.driver.set_page_load_timeout(50)
                self.logger.save("Driver created successfully.")
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
            self.logger.info("Closing browser session...")
            self.executor.driver.quit()
            # shutil.rmtree(self.executor.profile_dir, ignore_errors=True)
            self.logger.save("Driver closed successfully.")

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
            
            timeout = bank_params.get("base_timeout",40)
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

    def get_final_struct(self):
        """Create a fresh data structure for current run."""
        now = datetime.now()
        date, timestamp = now.strftime("%d%m%y"), now.strftime("%H%M")
        
        final_dict = {
            "metadata": {
                "date": date,
                "start_time": timestamp,
                "cfname": f"CACHE{date}T{timestamp}.json",
                "pfname": f"PROCESS{date}T{timestamp}.json"
            },
            "registry": {},
            "records": []
        }
        
        self.compare_path = os.path.join(self.RUNTIME_PATH, f"COMPARE{date}T{timestamp}.json")
        self.cache_path = os.path.join(self.RUNTIME_PATH, final_dict["metadata"]["cfname"])
        self.process_path = os.path.join(self.RUNTIME_PATH, final_dict["metadata"]["pfname"])
        self.comparison_xlsx = os.path.join(self.RUNTIME_PATH, f"COMPARE{date}T{timestamp}.xlsx")
        
        self.error_html_path =  os.path.join(self.RUNTIME_PATH, f"ERROR_SUMMARY_{date}T{timestamp}.html")
        
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
            self.logger.info("✅ No errors or missing-data notes to export.")
            return None

        errors,infos = [e for e in self.errors if e["severity"] == "ERROR"],[e for e in self.errors if e["severity"] == "INFO"]

        # --- Build TEXT Summary ---
        # text_lines = ["📘 SCRAPER EXECUTION SUMMARY", f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}","=" * 70, ""]

        # if infos:
        #     text_lines.append("🟡 BANKS WITH NO DATA (INFO):")
        #     for e in infos:
        #         text_lines.append(f" - {e['bank']} → {e['note']} [{e['timestamp']}]")

        # if errors:
        #     text_lines.append("\n🟥 BANKS WITH ERRORS:")
        #     for e in errors:
        #         text_lines.append(f" - {e['bank']} | {e['source']} | {e['type']} → {e['message']} [{e['timestamp']}]")

        # --- Build HTML Summary ---
        html_lines = [
            "<html><body style='font-family:Arial, sans-serif;'>",
            f"<h2>📘 Scraper Execution Summary — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</h2>",
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

        if self.error_html_path:
            Helper.save_text("\n".join(html_lines), self.error_html_path)
        return self.error_html_path

    @log_exceptions(level="critical", raise_error=True)
    def process_cache(self, final_dict):
        """Process, compare, and update cache files."""
    
        # save_path = os.path.join(self.RUNTIME_PATH, final_dict["metadata"]["pfname"])
        pipeline = {
            "primary": [
                ["normalize_df", "value", "norm_table", "table_html"],
                ["sha1", "value", "SHA_ONE", "pdf"],
            ],
            "secondary": [["sha1", "norm_table", "SHA_ONE"]],
        }

        try:
            # ===== Stage 1: Process Current Cache =====
            self.logger.notice("Starting cache processing pipeline...")
            processed_cache = self.operator.runner(final_dict, pipeline)
            baseline = deepcopy(processed_cache)

            Helper.save_json(processed_cache, self.process_path)
            self.logger.save(f"Processed cache saved at: {self.process_path}")

            # ===== Stage 2: Compare with Previous Cache =====
            try:
                if os.path.exists(self.prev_path):
                    old_data = Helper.load_json(self.prev_path)
                    if isinstance(old_data, dict):
                        self.logger.notice("Loaded previous processed cache for comparison.")
                        comparison = self.operator.process_comparison(
                            old_data, processed_cache, key="SHA_ONE"
                        )

                        Helper.save_json(comparison, self.compare_path)
                        self.logger.debug(f"Comparison JSON saved: {self.compare_path}")

                        self.operator.generate_comparison_report(comparison, self.comparison_xlsx)
                        self.logger.debug(f"Comparison report → {self.comparison_xlsx}")
                        
                        #write email msg
                        ots = old_data["metadata"]["pfname"].replace("PROCESS", "").replace(".json", "")
                        nts = processed_cache["metadata"]["pfname"].replace("PROCESS", "").replace(".json", "")
                        self.email_msg = f"Scraped between {ots} and {nts}"
                    else:
                        self.logger.warning("Invalid previous cache format. Skipping comparison.")
                else:
                    self.logger.warning("No previous cache found for comparison.")
            except Exception as e:
                self.logger.warning(f"Comparison failed: {type(e).__name__} - {e}")

            # ===== Stage 3: Update Baseline for Next Run =====
            Helper.save_json(baseline, self.prev_path)
            self.logger.debug(f"Updated baseline processed cache for next run at: {self.prev_path}")

            # ===== Stage 4: Success Log =====
            self.logger.info("Cache processing and comparison completed successfully.")
            return None

        except Exception as e:
            self.logger.error(f"process_cache failed: {type(e).__name__} - {e}")
            self.logger.debug(traceback.format_exc())
            return None
    
    def create_scrape_report(
        self, 
        cache_data, 
        report_type="pdf", 
        report_name="SCRAPE-REPORT", 
        rewrite=False,
        rw_path = None
    ):
        """Generate scrape report in PDF, Excel, or both."""
        if not report_type:
            self.logger.info("As Instruced, not generating report.")
            return None

        report_type = report_type.lower()
        timestamp = datetime.now().strftime('%y%m%d_%H%M')
        report_new = f"{report_name}-{timestamp}"

        # ===== PDF =====
        if report_type in ("pdf", "both"):
            pdf_archive_path = os.path.join(self.RUNTIME_PATH, f"{report_new}.pdf")
            pdf_latest_path = os.path.join(self.RUNTIME_PATH, f"{report_name}.pdf")

            self.reporter.build(cache_data, pdf_archive_path)
            self.logger.save(f"PDF report archived at: {pdf_archive_path}")

            if rewrite or not os.path.exists(pdf_latest_path):
                shutil.copy(pdf_archive_path, pdf_latest_path)
                self.logger.save(f"PDF report updated at: {pdf_latest_path}")

        # ===== Excel =====
        if report_type in ("xlsx", "both"):
            xls_archive_path = os.path.join(self.RUNTIME_PATH, f"{report_new}.xlsx")
            xls_latest_path = os.path.join(self.RUNTIME_PATH, f"{report_name}.xlsx")

            self.reporter.write_excel_report(cache_data, xls_archive_path)
            self.logger.save(f"Excel report archived at: {xls_archive_path}")

            if rewrite or not os.path.exists(xls_latest_path):
                if rw_path: xls_latest_path = rw_path
                
                shutil.copy(xls_archive_path, xls_latest_path)
                self.logger.save(f"Excel report updated at: {xls_latest_path}")

        return pdf_archive_path if report_type in ("pdf", "both") else None, \
            xls_archive_path if report_type in ("xlsx", "both") else None
        
    def runner(self,bank_codes):
        
        final_dict = self.get_final_struct()
        for code in bank_codes:
            if code not in self.CONFIG:
                self.logger.error(f"Code: {code} not found in config. Skipping...")
                continue
            bank_params = self.CONFIG[code]
            
            try:
                result = self.scrape_bank(bank_params)
                result = BankScraper.dedupe_responses(result)
                
                # 🟡 Detect banks with no data in any scrape
                scraped_blocks = result.get("scraped_data", [])
                if not scraped_blocks or all(not block.get("data_present", False) for block in scraped_blocks):
                    self.record_error(
                        bank_params.get("bank_name"),
                        source="DATA",
                        note="No data extracted — possible empty table or maintenance page.",
                        severity="INFO"
                    )
                
            except Exception as e:
                self.record_error(bank_params.get("bank_name"), e)
                result = {"bank_code": code, "scraped_data": [{"error": str(e)}]}
            final_dict["records"].append(result)
        
        #cache + save
        Helper.save_json(final_dict, self.cache_path)
        self.logger.save(f"Cache saved at: {self.cache_path}")

        self.close_session()
        self.logger.save(f"Closing Session !!")
        return final_dict

    
            
        