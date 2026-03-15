from app.action_executor import ActionExecutor
from app.operation_executor import  OperationExecutorLatest
from app.pdf_report import PDFReportBuilderPro
from selenium.common.exceptions import WebDriverException #type: ignore

# app/BankScraper.py
import traceback, time, hashlib, os, time, shutil, threading
from copy import deepcopy
from datetime import datetime
from app.logger import get_global_logger, log_exceptions
from app.utils import Helper
from app.constants import session_dir, create_dir

class ScraperHandler:
    """Main controller for orchestrating scraping per bank."""

    def __init__(self, config):
        """
        Args:
            executor: Instance of ActionExecutor
            operator: Optional instance of OperationExecutor (for post ops)
            logger: Shared logger instance
        """
        #logs
        self.logger = get_global_logger()
        
        #date time
        self.datetime = datetime.now()
        self.date = self.datetime.strftime("%d%m%y")
        self.time = self.datetime.strftime("%H%M")
        
        
        #paths
        self.session_dir = session_dir()
        self.runtime_path = create_dir(self.session_dir, f"SESSION{self.datetime.strftime()}")
        self.recent_session = create_dir(self.session_dir, "SESSION_LATEST")
        
        #contract
        self.METADATA = config['ATTRIBUTE']
        self.CONTRACTS = config["CONTRACTS"]
        self.ENGINE_BUCKETS = {
            "soup": list(),
            "selenium":list()
        }
        
        #output + metadata
        self.CACHE_METADATA = {
            "date":  self.date,
            "start_time":self.time,
            "cname": f"CACHE{ self.date}T{self.time}.json",
            "pname": f"PROCS{ self.date}T{self.time}.json"
            
        }
        self.OUTPUT_JSON = {
            "metadata": self.CACHE_METADATA,
            "registry":{},
            "records":[]
        }
        
    # =====================================================
    # UTILITY + STATIC METHODS
    # =====================================================
    
    
    def path_builder(self, rpath:str)->dict:
        
        pass
    
    
    
    @log_exceptions(level="critical", return_value=False)
    def ReadContracts(self)->None:
        
        for contract in self.CONTRACTS:
            
            if contract["engine"] == "soup":
                self.ENGINE_BUCKETS["soup"].append(contract)
            
            elif contract["engine"] == "selenium":
                self.ENGINE_BUCKETS["selenium"].append(contract)
            
            else:
                self.logger.info(f"UNKNOWN ENGINE:{contract["contract_name"]} -> {contract["engine"]}")
                self.logger.info("Skipping Contract")
        
    @log_exceptions(level="critical", return_value=False)
    def RunContracts(self):

        strategy = self.METADATA.get("strategy", ["soup", "selenium"])
        self.logger.info(f"Execution Strategy: {strategy}")

        engine_results = [] #output of both contracts
        lock = threading.Lock()

        def worker(engine, contracts): #worker function
            results = self._run_engine(engine, contracts)
            if results:
                with lock:
                    engine_results.extend(results)

        if "thread" in strategy:  # parallel mode
            self.logger.info("Running engines in parallel mode")

            threads = []
            for engine, contracts in self.ENGINE_BUCKETS.items():

                if not contracts:
                    continue
                t = threading.Thread(target=worker,args=(engine, contracts))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

        else:  # sequential mode
            self.logger.info("Running engines sequentially")

            for engine in strategy:

                contracts = self.ENGINE_BUCKETS.get(engine, [])

                if not contracts:
                    self.logger.info(f"No contracts for engine: {engine}")
                    continue

                results = self._run_engine(engine, contracts)

                if results:
                    engine_results.extend(results)

        # aggregate results into output
        self.OUTPUT_JSON["records"].extend(engine_results)
    
    def _run_engine(self, engine_name, contracts):

        if not contracts:
            self.logger.info(f"No contracts for engine: {engine_name}")
            return

        self.logger.info(f"Starting engine: {engine_name}")
        self.logger.info(f"Contracts assigned: {len(contracts)}")

        try:
            #Section to perform various works related to either using Selenium/Soup
            #if else statements used to further extend it to RSS or other programs

            if engine_name == "soup":
                engine = SoupEngine(self.METADATA) #type: ignore

            elif engine_name == "selenium":
                engine = SeleniumEngine(self.METADATA) #type: ignore

            else:
                self.logger.warning(f"Unknown engine: {engine_name}")
                return

            results = engine.run(contracts)

            self.logger.info(f"Engine {engine_name} completed")

            return results

        except Exception as e:
            self.logger.error(f"Engine {engine_name} failed: {e}")
            self.logger.debug(traceback.format_exc())
            
            
            
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