import traceback
from selenium.common.exceptions import WebDriverException  # type: ignore

from app.action_executor import ActionExecutor
from app.logger import get_global_logger, log_exceptions


class SeleniumEngine:

    def __init__(self, metadata: dict):
        self.logger = get_global_logger()
        self.metadata = metadata

        # executor handles selenium actions
        self.executor = ActionExecutor()

    # -----------------------------------------------------
    # DRIVER LIFECYCLE
    # -----------------------------------------------------

    @log_exceptions(level="critical", return_value=False)
    def start_session(self):

        retries = 3
        retry_delay = 5

        for attempt in range(retries):

            try:
                self.logger.info(f"[SeleniumEngine] Creating driver (Attempt {attempt+1})")

                self.executor.create_uc_driver(
                    headless=self.metadata.get("headless", False),
                    minimized=self.metadata.get("minimize_selenium", True)
                )

                if not self.executor.driver:
                    raise RuntimeError("Driver creation returned None")

                self.executor.driver.set_page_load_timeout(
                    self.metadata.get("hero_timeout", 120)
                )

                self.logger.info("[SeleniumEngine] Driver created successfully")
                return True

            except WebDriverException as e:

                self.logger.error(f"Driver creation failed: {type(e).__name__}")
                self.logger.debug(traceback.format_exc())

        self.logger.critical("Failed to initialize Selenium driver")
        return False

    def close_session(self):

        try:
            if self.executor.driver:
                self.executor.driver.quit()
                self.logger.info("[SeleniumEngine] Driver closed")

        except Exception as e:
            self.logger.warning(f"Driver close failed: {e}")

    # -----------------------------------------------------
    # MAIN ENGINE
    # -----------------------------------------------------

    def run(self, contracts: list):

        results = []

        if not self.start_session():
            self.logger.critical("SeleniumEngine cannot start without driver")
            return results

        for contract in contracts:

            try:

                result = self.process_contract(contract)

                if result:
                    results.append(result)

            except Exception as e:

                self.logger.error(
                    f"[SeleniumEngine] Contract failed: {contract.get('contract_name')}"
                )
                self.logger.debug(traceback.format_exc())

        self.close_session()

        return results

    # -----------------------------------------------------
    # CONTRACT PROCESSING
    # -----------------------------------------------------

    def process_contract(self, contract: dict):

        cname = contract.get("contract_name")

        self.logger.info(f"=== Selenium Contract → {cname} ===")

        scraped_data = []

        try:
            self.executor.set_params(contract)
            timeout = contract.get("hero_timeout", 120)
            self.executor.get_website(timeout)
            data = self.executor.execute_blocks()
            scraped_data.extend(data)

        except Exception as e:

            self.logger.error(
                f"[SeleniumEngine] Failed for {cname}: {type(e).__name__}"
            )
            self.logger.debug(traceback.format_exc())

            scraped_data = [{
                "error_type": type(e).__name__,
                "error_message": str(e),
                "error_from": "SeleniumEngine.process_contract"
            }]

        finally:

            try:
                if self.executor.driver:
                    self.executor.driver.delete_all_cookies()
            except Exception:
                pass

        return {
            "bank_name": cname,
            "bank_code": contract.get("contract_code"),
            "base_url": contract.get("hero_url"),
            "scraped_data": scraped_data
        }