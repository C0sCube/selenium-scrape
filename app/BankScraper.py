from app.action_executor import ActionExecutor
import time

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
    def get_final_struct(today,file_name):
        return {
            "metadata": {
                "program": "main.py",
                "timestamp": today,
                "config": "params_table.json5",
                "filename": file_name
            },
            "records": []
        }
    
    def run(self):
        try:
            self.executor.create_uc_driver()
            self.logger.info("Driver Created.")
            self.executor.driver.get(self.bank_params["base_url"])
            self.logger.notice("Page fetched successfully.")

            self.logger.notice(f"Scraping From Bank: {self.scrape_data["bank_name"]}")
            
            for idx, block in enumerate(self.bank_params["blocks"]):
                self.logger.info(f"Running Block: {idx}")
                data = self.executor.execute_blocks(block)
                self.scrape_data["scraped_data"].extend(data)

        except Exception as e:
            self.logger.error(f"Error scraping {self.bank_params['bank_name']}: {e}")
            self.scrape_data["scraped_data"] = [{"Error": str(e)}]
            
        finally:
            self.executor.driver.quit()
            time.sleep(5)
        return self.scrape_data
