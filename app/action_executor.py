from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver 
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import re, os, time, logging
from io import StringIO


from app.operation_executor import *
from app.utils import *

class ActionExecutor:
    def __init__(self,logger=None, params=None, paths=None, base_window=None):
        self.logger = logger or logging.getLogger(__name__)
        self.PARAMS = params or {}
        self.PATHS = paths or {}
        self.data = {}

        self.DATE = datetime.now()
        self.OUTPUT_PATH = os.path.join(paths["output"],paths["folders"]["data"],self.DATE.strftime("%d%m%Y"),params["bank_name"])
        os.makedirs(self.OUTPUT_PATH, exist_ok=True)

        # Create driver if not provided
        self.driver = self._create_driver()
        self._attach_headers()
        self.window_stack = [base_window or self.driver.current_window_handle]

    def _create_driver(self):
        options = webdriver.ChromeOptions()
        for opt in self.PARAMS.get("chrome_options", []):
            options.add_argument(opt)

        user_agent = self.PARAMS.get("headers", {}).get("User-Agent")
        if user_agent:
            options.add_argument(f'user-agent={user_agent}')
        
        return webdriver.Chrome(options=options)
    
    def _attach_headers(self):
        self.driver.execute_cdp_cmd("Network.enable", {})
        headers = self.PARAMS.get("headers", {})
        if headers:
            self.driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
                "headers": headers
            })

    def get_by(self, by_string):
        return {
            "css": By.CSS_SELECTOR,
            "xpath": By.XPATH,
            "id": By.ID,
            "name": By.NAME,
            "class": By.CLASS_NAME,
            "tag": By.TAG_NAME,
        }.get(by_string.lower(), By.CSS_SELECTOR)

    def get_condition(self, wait_type, by, value):
        cond_map = {
            "clickable": EC.element_to_be_clickable,
            "visible": EC.visibility_of_element_located,
            "present": EC.presence_of_element_located,
            "invisible": EC.invisibility_of_element_located,
            "attached": EC.element_to_be_selected
        }

        locator = (self.get_by(by), value)
        condition_func = cond_map.get(wait_type)
        if not condition_func:
            raise ValueError(f"Unknown wait condition: {wait_type}")
        return condition_func(locator)

    def execute(self, _action_: dict):
        
        SNAPSHOT = Helper.generate_code()
        SCRAPE_DATA = {}
        
        #get
        by = self.get_by(_action_.get("by", "css"))
        value = _action_.get("value")
        
        #time
        wait = _action_.get("wait", 0)
        timeout = _action_.get("timeout", 10)
        wait_until = _action_.get("wait_until")
        
        #action
        action_type = _action_.get("action")
        
        #name html_name, table_name
        #export_format
        
        #extract val
        # keys = _action_.get("keys")
        attribute = _action_.get("attribute")
        scrape_fields = _action_.get("scrape_fields")
        
        #save
        consolidate_save = _action_.get("consolidate_save", False)
        multiple = _action_.get("multiple",False)
        
        #new window
        new_window = _action_.get("new_window", False)
        return_to_base = _action_.get("return_to_base", False)
    
        url = _action_.get("url","")
        
        if wait:
            time.sleep(wait)
        
        try:
            # Logging
            self.logger.info(f"Performing _action_: {action_type} on {value}")
            elem = None
            if wait_until:
                condition = self.get_condition(wait_until, by, value)
                elem = WebDriverWait(self.driver, timeout).until(condition)
            else:
                elem = self.driver.find_element(by, value)
        except Exception as e:
            if _action_.get("skip_if_not_found"):
                self.logger.warning(f"Element not found, skipping: {value}")
                return
            else:
                raise e

        
        if action_type == "click":
            
            elem.click()
            if new_window:
                WebDriverWait(self.driver, timeout).until(lambda d: len(d.window_handles) > len(self.window_stack))
                new_tab = [h for h in self.driver.window_handles if h not in self.window_stack][0]
                self.driver.switch_to.window(new_tab)
                self.window_stack.append(new_tab)
            
        elif action_type == "download":
            file_url = elem.get_attribute("href")
            if not file_url:
                try:
                    link_elem = elem.find_element(By.TAG_NAME, "a")
                    file_url = link_elem.get_attribute("href")
                except:
                    file_url = None

            if file_url and file_url.lower().endswith(".pdf"):
                import requests
                cookies = {c['name']: c['value'] for c in self.driver.get_cookies()}
                r = requests.get(file_url, cookies=cookies)
                if r.status_code == 200:
                    output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["downloads"])
                    file_path = os.path.join(output_dir, os.path.basename(file_url))
                    with open(file_path, "wb") as f:
                        f.write(r.content)
                    self.logger.info(f"Downloaded PDF to {file_path}")
                else:
                    self.logger.error(f"Failed to download file: {file_url}")
            else:
                elem.click()
                self.logger.info("Triggered click for file download.")

        elif action_type == "html":
            html_name = _action_.get('html_name', 'html')
            elements = self.driver.find_elements(by, value) if multiple else [self.driver.find_element(by, value)] 
            
            cleaned_content,content_scrape = [],{}
            for idx, elem in enumerate(elements):
                html_content = elem.get_attribute("outerHTML")
                if not html_content:
                    self.logger.warning(f"No HTML content found for element: {value}")
                    continue
                
                cleaned_content.append(html_content)
                content_scrape.update({f"{html_name}_{idx}":html_content})
            
            #save data
            output_dir = Helper.create_dirs(self.OUTPUT_PATH,["save_html"])
            OperationExecutor.save_tables_html(
                cleaned_content,
                output_dir=output_dir,
                output_file=f"{html_name}.html",
                separator="<br><hr><br>" if consolidate_save else None
            )
            
            self.logger.info(f"Saved {len(cleaned_content)} table(s) to HTML in: {output_dir}")
            SCRAPE_DATA.update({SNAPSHOT:{"action":action_type,"webpage":self.driver.current_url,"response":content_scrape}})
        
        elif action_type == "table":
            table_name = _action_.get("table_name","table")
            elements = self.driver.find_elements(by, value) if multiple else [self.driver.find_element(by, value)]

            cleaned_tables, table_scrape = [], {}
            for idx, elem in enumerate(elements):
                raw_html = elem.get_attribute("outerHTML")
                soup = BeautifulSoup(raw_html, "html.parser")
                
                for th in soup.find_all("th"):
                    th.name = "td"
                    
                for tag in soup.find_all(True):
                    if tag.name not in ("td", "tr"):
                        tag.unwrap() 
                    else:
                        for attr in list(tag.attrs):
                            if attr not in {"rowspan", "colspan"}:
                                del tag.attrs[attr]

                cleaned_tables.append(str(soup))
                table_scrape[f"table_{idx}"] = str(soup)

            # export format + save
            export_format = _action_.get("export_format", None)  # default to Excel

            if export_format in ["excel", "both"]:
                output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["save_excel"])
                OperationExecutor.save_tables_to_excel(
                    cleaned_tables,
                    output_dir=output_dir,
                    output_file=f"{table_name}.xlsx",
                    separator=consolidate_save
                )

                self.logger.info(f"Saved {len(cleaned_tables)} table(s) to single Excel file: {output_dir}")
                
            if export_format in ["html", "both"]:
                output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["save_html"])
                OperationExecutor.save_tables_html(
                    cleaned_tables,
                    output_dir=output_dir,
                    output_file=f"{table_name}.html",
                    separator="<br><hr><br>" if consolidate_save else None
                )
                
                self.logger.info(f"Saved {len(cleaned_tables)} table(s) to HTML in: {output_dir}")

            SCRAPE_DATA.update({SNAPSHOT:{"action":action_type,"webpage":self.driver.current_url,"response":table_scrape}})
            
        #scrape data
        elif action_type == "scrape":
            print(f"Scraping Using By={by}")
            elements = self.driver.find_elements(self.get_by(by), value)

            data_container = {}
            for elem in elements:
                if scrape_fields:
                    results = {}
                    for key, sub_selector in scrape_fields.items():
                        
                        if "|||" in sub_selector:
                            sub_selector,by = sub_selector.split("|||")
                        try:
                            sub_elem = elem.find_element(self.get_by(by), sub_selector)
                            if sub_elem:
                                text = sub_elem.text.strip()
                                if not text: #Hidden Text
                                    print("Trying extract using textContent .")
                                    text = sub_elem.get_attribute("textContent").strip()
                                
                                if not text:
                                    print("Trying extract using innerHTML .")
                                    text = sub_elem.get_attribute("innerHTML").strip()
                                    
                                results[key] = text
                                
                        except Exception as e:
                            self.logger.warning(f"Missing field '{key}': {e}")
                            results[key] = None
                    data_container.update(results)

                elif attribute:
                    data = elem.get_attribute(attribute)
                    self.logger.info(f"Scraped attribute {attribute}: {data}")
                    data_container.update({attribute: data})

                else:
                    data_container.update({"text": elem.text.strip()})

            self.logger.info(f"Scraped data: {data_container.keys()}")
            import pprint
            
            pprint.pprint(data_container)
            # SCRAPE_DATA.update({SNAPSHOT:data_container})
            SCRAPE_DATA.update({SNAPSHOT:{"action":action_type,"webpage":self.driver.current_url,"response":data_container}})
            

        #open website
        elif action_type == "website":
            try:
                self.logger.info(f"Redirecting to webpage {url}")
                self.driver.get(url)
            except Exception as e:
                self.logger.error(f"Unable to redirect")
                
        #take a screenshot
        elif action_type == "screenshot":
            
            path = os.path.join(self.OUTPUT_PATH,f"{_action_.get('screenshot_name', 'screenshot')}.png")
            self.driver.save_screenshot(path)
            self.logger.info(f"Saved screenshot: {path}")
            
        # elif action_type == "send_keys":
        #     elem.clear()
        #     elem.send_keys(keys)
        
        # elif _action_ == "click_all":
        #     elements = self.driver.find_elements(by, value)
        #     for el in elements:
        #         self.river.execute_script("arguments[0].click();", el)
        #         time.sleep(0.5)
                
        elif  not action_type:
            
            self.logger.info(f"Checked presence of element: {by}={value}")

        else:
            pass


        if return_to_base:
            self.logger.info(f"Returning to Base Window.")
            if len(self.window_stack) > 1:
                self.driver.close()
                self.window_stack.pop()
                self.driver.switch_to.window(self.window_stack[-1])

        return SCRAPE_DATA

    def execute_blocks(self, block: list):  
        block_data = {}
        for idx,_action_ in enumerate(block):
            data = self.execute(_action_)
            if data:
                block_data.update(data)
        time_stamp = self.DATE.strftime("%Y-%m-%d %H:%M")
        return block_data,time_stamp
    
    def perform_action():
        pass