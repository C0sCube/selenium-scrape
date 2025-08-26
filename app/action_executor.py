from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver 
from bs4 import BeautifulSoup
from datetime import datetime

import re, os, time, logging ,pprint, requests, base64
from io import StringIO
import undetected_chromedriver as uc

from app.operation_executor import *
from app.utils import *

class ActionExecutor:
    def __init__(self,logger=None, params=None, paths=None):
        self.logger = logger or logging.getLogger(__name__)
        self.PARAMS = params or {}
        self.PATHS = paths or {}
        self.data = {}

        self.DATE = datetime.now()
        self.OUTPUT_PATH = Helper.create_dir(paths["output"],paths["folders"]["data"],self.DATE.strftime("%d%m%Y"),params["bank_name"])

        self.driver = None
        self.window_stack = None
    
    def create_driver(self):
        options = Options()
        options.add_argument(r"--no-sandbox")
        options.add_argument(r"--disable-dev-shm-usage")
        options.add_argument(r"--disable-gpu")
        options.add_argument(rf"--user-data-dir={self.PATHS['profile_path']}")
        options.add_argument(rf"--profile-directory={self.PATHS['profile_name']}")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])

        driver_path = self.PATHS.get("driver_path")
        service = Service(driver_path)
        self.driver = webdriver.Chrome(service=service, options=options)
        
        self.window_stack = [self.driver.current_window_handle]
        self.__attach_headers()
        return self.driver
    
    def create_uc_driver(self):
        options = uc.ChromeOptions()
        # options.add_argument(f"--user-data-dir={profile_path}")
        # options.add_argument(f"--profile-directory={profile_dir}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-extensions")
        
        self.driver = uc.Chrome(options=options)
        # time.sleep(2)
        # self.driver.get("https://www.cogencis.com/")
        time.sleep(2)
        
        self.window_stack = [self.driver.current_window_handle]
        return self.driver
    
    def __attach_headers(self):
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
    
    def __perform_action(self,action_type = None):
        action_map = {
            "click": lambda: self._action_click(),
            "html": self._action_html_scrape,
            "table": self._action_table_scrape,
            "scrape": self._action_text_scrape,
            "website": self._action_redirect,
            "screenshot": self._action_screenshot,
            "download": self._action_download,
            "pdf": self._action_pdf,
            "tablist": self._action_tab_list,
        }

        if not action_type:
            self.logger.info(f"Checked presence of element: {self.BY}={self.VALUE}")
            return

        action = action_map.get(action_type)
        if action:
            result = action()
            if result is not None:
                return result
        else:
            self.logger.warning(f"Unknown action type: {self.ACTION_TYPE}")
        
    def execute(self, _action_: dict):
        self.ACTION_TYPE = _action_.get("action",None)
        
        #get
        self.BY = self.get_by(_action_.get("by", "css"))
        self.VALUE = _action_.get("value")
        self.URL = _action_.get("url","https://tinyurl.com/nothing-borgir")
        
        #time
        self.DEFAULT_WAIT = _action_.get("time", 2)
        self.TIMEOUT = _action_.get("timeout", 20)
        self.WAIT_UNTIL = _action_.get("wait_until")
        self.WAIT_BY = _action_.get("wait_by", self.BY) #dependent
        self.WAIT_VALUE = _action_.get("wait_value", self.VALUE) #dependent
            
        #name 
        self.table_name = _action_.get("table_name","table")
        self.html_name = _action_.get('html_name', 'html')
        self.screenshot_name = _action_.get('screenshot_name', 'screenshot')
        self.pdf_name = _action_.get('pdf_name', 'webpage_pdf')
        self.export_format = _action_.get("export_format", None)  # default to Excel
        self.LOG_MESSAGE = _action_.get("log_message", "Log Msg For Action Not Attached.")
        
        #field
        self.ATTRIBUTE = _action_.get("attribute")
        self.SCRAPE_FIELDS = _action_.get("scrape_fields")
        
        self.RUN_FUNCTION = _action_.get("execute",None)
        self.TAB_NAME = None
        self.TAB_FIELDS = _action_.get("imp_tabs",[])
        
        #save
        self.CONSOLIDATE_SAVE = _action_.get("consolidate_save", False)
        self.MULTIPLE = _action_.get("multiple",False)
        
        #page pdf
        self.LANDSCAPE = _action_.get("landscape",False)
        self.PRINT_BACKGROUND = _action_.get("print_background",False)
        
        #window
        self.NEW_WINDOW = _action_.get("new_window", False)
        self.RETURN_TO_BASE = _action_.get("return_to_base", False)
        
        time.sleep(random.uniform(0, self.DEFAULT_WAIT))
        
        
        #element; The Which gets loaded as default
        self.ELEMENT = None
        try:
            self.logger.info(f"Performing _action_: {self.ACTION_TYPE} on {self.VALUE}")
            
            if self.WAIT_UNTIL:
                condition = self.get_condition(self.WAIT_UNTIL, self.WAIT_BY, self.WAIT_VALUE)
                self.ELEMENT = WebDriverWait(self.driver, self.TIMEOUT).until(condition)
            else:
                self.ELEMENT = self.driver.find_element(self.BY, self.VALUE)
        except Exception:
            self.logger.warning(f"Element not found: {self.BY}={self.VALUE}. Skipping this action.")
            return self._generate_packet({"skip":"Element Not Found Hence Skipped."}) # skip quietly

        content = None
        try:
            
            content = self.__perform_action(action_type = self.ACTION_TYPE)
            # if self.ACTION_TYPE == "click":
            #     self._action_click()
            # elif self.ACTION_TYPE == "html":
            #     content = self._action_html_scrape()
            # elif self.ACTION_TYPE == "table":
            #     content = self._action_table_scrape()
            # elif self.ACTION_TYPE == "scrape":
            #     content = self._action_text_scrape()
            # elif self.ACTION_TYPE == "website":
            #     self._action_redirect()
            # elif self.ACTION_TYPE == "screenshot":
            #     self._action_screenshot()
            # elif self.ACTION_TYPE == "download":
            #     content = self._action_download()
            # elif self.ACTION_TYPE == "pdf":
            #     self._action_pdf()
            # elif self.ACTION_TYPE == "tablist":
            #     self._action_tab_list()
            # elif not self.ACTION_TYPE:
            #     self.logger.info(f"Checked presence of element: {self.BY}={self.VALUE}")
        except Exception as e:
            self.logger.error(f"Error executing action '{self.ACTION_TYPE}': {e}", exc_info=True)
            return self._generate_packet({"Error": e}) # skip further execution

        if self.RETURN_TO_BASE:
            self.logger.info("Returning to Base Window.")
            if len(self.window_stack) > 1:
                self.driver.close()
                self.window_stack.pop()
                self.driver.switch_to.window(self.window_stack[-1])

        return self._generate_packet(content)

    # ===================== ACTION =====================
    
    #DOM-Scrape Actions
    def _action_text_scrape(self)->dict:
        self.logger.info(f"Scraping Using BY={self.BY} and VALUE={self.VALUE}")
        elements = self.driver.find_elements(self.BY, self.VALUE)

        data_container = {}
        for elem in elements:
            if self.SCRAPE_FIELDS:
                results = {}
                for key, sub_selector in self.SCRAPE_FIELDS.items():
                    
                    if "|||" in sub_selector:
                        sub_selector,BY = sub_selector.split("|||")
                    try:
                        sub_elem = elem.find_element(self.get_by(BY), sub_selector)
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

            elif self.ATTRIBUTE:
                data = elem.get_attribute(self.ATTRIBUTE)
                self.logger.info(f"Scraped attribute {self.ATTRIBUTE}: {data}")
                data_container.update({self.ATTRIBUTE: data})

            else:
                data_container.update({"text": elem.text.strip()})

        self.logger.info(f"Scraped Content:\n{pprint.pformat(data_container)}")
        return data_container
    
    def __find_nearest_preceding_label(self,table):
        current = table
        while True:
            try:
                parent = current.find_element(By.XPATH, "..")
                siblings = parent.find_elements(By.XPATH, "preceding-sibling::*")
                for sibling in reversed(siblings):
                    tag = sibling.tag_name.lower()
                    text = sibling.text.strip()
                    if tag not in ["div", "br", "hr"] and text:
                        return text
                current = parent
            except:
                break
        return "No label found"
    
    # def __find_nearest_preceding_label(self, table):
    #     current = table
    #     while True:
    #         try:
    #             inner_candidates = current.find_elements(By.XPATH, ".//*")
    #             for elem in inner_candidates:
    #                 tag = elem.tag_name.lower()
    #                 text = elem.text.strip()
    #                 if tag not in ["div", "br", "hr"] and text:
    #                     return text
    
    #             parent = current.find_element(By.XPATH, "..")
    #             siblings = parent.find_elements(By.XPATH, "preceding-sibling::*")
    #             for sibling in reversed(siblings):
    #                 tag = sibling.tag_name.lower()
    #                 text = sibling.text.strip()
    #                 if tag not in ["div", "br", "hr"] and text:
    #                     return text

    #             # Step 3: Move up the tree
    #             current = parent
    #         except:
    #             break
    #     return "No label found"
    
    # def __find_nearest_preceding_label(self, table):
    #     current = table
    #     max_depth = 5
    #     depth = 0

    #     while depth < max_depth:
    #         try:
    #             # Step 1: Check inside current element for likely label tags
    #             inner_candidates = current.find_elements(
    #                 By.XPATH, ".//label | .//span | .//strong | .//p | .//h1 | .//h2 | .//h3"
    #             )
    #             for elem in inner_candidates:
    #                 text = elem.text
    #                 if text and text.strip():
    #                     return text.strip()

    #             # Step 2: Check preceding siblings of the parent
    #             parent = current.find_element(By.XPATH, "..")
    #             siblings = parent.find_elements(By.XPATH, "preceding-sibling::*")
    #             for sibling in reversed(siblings):
    #                 tag = sibling.tag_name.lower()
    #                 text = sibling.text
    #                 if tag not in ["div", "br", "hr"] and text and text.strip():
    #                     return text.strip()

    #             # Step 3: Move up the tree
    #             current = parent
    #             depth += 1

    #         except Exception:
    #             break

    #     return "No label found"
    
    def _action_table_scrape(self)->dict:
        self.logger.info(f"Scraping Using BY={self.BY} and VALUE={self.VALUE}")
        elements = self.driver.find_elements(self.BY, self.VALUE) if self.MULTIPLE else [self.driver.find_element(self.BY, self.VALUE)]
        
        #mandatory filter
        if self.BY == By.CSS_SELECTOR:
            elements = [elem for elem in elements if elem.tag_name.lower() == "table"]


        self.logger.info(f"Total Elements Found By={self.BY} and Value={self.VALUE} are {len(elements)}")
        
        cleaned_tables, return_content = [], {}
        for idx, elem in enumerate(elements):
            header = self.__find_nearest_preceding_label(elem)
            print(f"Table: {idx} has header:: {header}")
            raw_html = elem.get_attribute("outerHTML")
            
            raw_html = Helper.apply_sub(raw_html, r'<th\b', '<td', ignore_case=True)
            raw_html = Helper.apply_sub(raw_html, r'</th\b', '</td', ignore_case=True)
            
            #tbody
            raw_html = re.sub(r"<thead\b",r"<tbody",raw_html, re.IGNORECASE)
            raw_html = re.sub(r"</thead\b",r"</tbody",raw_html, re.IGNORECASE)
            
            #other tags
            raw_html = Helper.apply_sub(raw_html, r"</?(?:strong|sup|b|p|br)(?:\s+[^>]*)?>",ignore_case=True)
            raw_html = Helper.apply_sub(raw_html,r'[#*@\n\t]+', ignore_case=True)
            raw_html = Helper.apply_sub(raw_html,r"<tr[^>]*>\s*(?:&nbsp;|\u00A0|\s)*</tr>", ignore_case=True)
            raw_html = Helper._normalize_whitespace(raw_html)

            soup = BeautifulSoup(raw_html, "html.parser")
            ALLOWED = {"rowspan", "colspan"}
            for tag in soup.find_all(True):
                for attr in list(tag.attrs):
                    if attr not in ALLOWED:
                        del tag.attrs[attr]
            cleaned_tables.append(str(soup))
            # return_content[f"table_{idx}"] = {"name":f"table_{idx}","header":str(header),"value":str(soup)}
            return_content[f"table_{idx}"] = str(soup)

        # export format + save

        if self.export_format in ["excel", "both"]:
            output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["save_excel"])
            OperationExecutor.save_tables_to_excel(cleaned_tables,output_dir=output_dir,
                output_file=f"{self.table_name}.xlsx",
                consolidate_save=self.CONSOLIDATE_SAVE
            )
            self.logger.save(f"Saved {len(cleaned_tables)} table(s) to single Excel file: {output_dir}")
            
        if self.export_format in ["html", "both"]:
            output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["save_html"])
            OperationExecutor.save_tables_html(cleaned_tables,output_dir=output_dir,
                output_file=f"{self.table_name}.html",
                separator="<br><hr><br>" if self.CONSOLIDATE_SAVE else None
            )
            self.logger.save(f"Saved {len(cleaned_tables)} table(s) to HTML in: {output_dir}")
        
        return return_content
    
    def _action_html_scrape(self)->dict:
        self.logger.info(f"Scraping Using BY={self.BY} and VALUE={self.VALUE}")
        elements = self.driver.find_elements(self.BY, self.VALUE) if self.MULTIPLE else [self.driver.find_element(self.BY, self.VALUE)] 
            
        cleaned_content,content_scrape = [],{}
        for idx, elem in enumerate(elements):
            html_content = elem.get_attribute("outerHTML")
            if not html_content:
                self.logger.warning(f"No HTML content found for element: {self.VALUE}")
                continue
            
            cleaned_content.append(html_content)
            content_scrape.update({f"{self.html_name}_{idx}":html_content})
        
        #save data
        output_dir = Helper.create_dirs(self.OUTPUT_PATH,["save_html"])
        OperationExecutor.save_tables_html(
            cleaned_content,
            output_dir=output_dir,
            output_file=f"{self.html_name}.html",
            separator="<br><hr><br>" if self.CONSOLIDATE_SAVE else None
        )
        
        self.logger.save(f"Saved {len(cleaned_content)} table(s) to HTML in: {output_dir}")
        
        return content_scrape
    
    def _action_tab_list(self)->dict:
        self.logger.info(f"Tab List Loop Using BY={self.BY} and VALUE={self.VALUE}")
        tabList = self.driver.find_elements(self.BY, self.VALUE)  #if self.MULTIPLE else [self.driver.find_element(self.BY, self.VALUE)]
        self.logger.info(f"Total Elements Found By={self.BY} and Value={self.VALUE} are {len(tabList)}")
        
        # if self.BY == By.CSS_SELECTOR:
        #     tabList = [elem for elem in tabList if elem.tag_name.lower() == "table"]
            
        cleaned_content,content_scrape = [],{}
        for tab in tabList:
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab)
                time.sleep(0.5)
                ActionChains(self.driver).move_to_element(tab).perform()
                
                WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable(tab))
                
                print(tab.get_attribute("innerText"))
                tab.click()
                time.sleep(1)
                
                
                
            except Exception as e:
                print(f"Failed to click tab: {e}")
            
        return content_scrape
        

    #Non-Data Actions
    def _action_screenshot(self):
        try:
            path = Helper.create_path(self.OUTPUT_PATH,f"{self.screenshot_name}.png")
            result = self.driver.execute_cdp_cmd("Page.captureScreenshot", {
                "captureBeyondViewport": True,
                "fromSurface": True
            })
            with open(path, "wb") as f:
                f.write(base64.b64decode(result['data']))
                
            self.logger.save(f"Saved screenshot: {path}")
        
        except Exception as e:
            self.logger.error(f"Failed to save screenshot: {str(e)}")
        
    def __download_file(self, file_url, output_dir, idx, extension):
        cookies = {c['name']: c['value'] for c in self.driver.get_cookies()}
        r = requests.get(file_url, cookies=cookies)
        if r.status_code == 200:
            file_path = os.path.join(output_dir, os.path.basename(file_url))
            file_data = r.content
            encoded_data = base64.b64encode(file_data).decode("utf-8")

            Helper.write_binary_file(file_path, file_data)
            self.logger.info(f"Downloaded {extension.upper()} to {file_path}")
            return {f"{extension}_{idx}": encoded_data}
        else:
            self.logger.error(f"Failed to download {extension.upper()}: {file_url}")
            return {}
    
    def _action_download(self):
        
        elements = self.driver.find_elements(self.BY, self.VALUE) if self.MULTIPLE else [self.driver.find_element(self.BY, self.VALUE)]
        self.logger.info(f"Total Elements Found By={self.BY} and Value={self.VALUE} are {len(elements)}")
        
        scrape_data = {}
        for idx, elem in enumerate(elements):
            self.driver.execute_script("arguments[0].scrollIntoView(true);", elem)
            file_url = elem.get_attribute("href")

            if not file_url:
                try:
                    link_elem = elem.find_element(By.TAG_NAME, "a")
                    file_url = link_elem.get_attribute("href")
                except:
                    file_url = None

            if file_url:
                output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["downloads"])
                if file_url.lower().endswith(".pdf"):
                    scrape_data.update(self.__download_file(file_url, output_dir, idx, "pdf"))
                elif file_url.lower().endswith(".csv"):
                    scrape_data.update(self.__download_file(file_url, output_dir, idx, "csv"))
                elif file_url.lower().endswith(".docx"):
                    scrape_data.update(self.__download_file(file_url, output_dir, idx, "docx"))
                else:
                    elem.click()
                    self.logger.info("Triggered click for file download.")
            else:
                self.logger.warning(f"No valid URL found for element at index {idx}")

        return scrape_data

    def _action_redirect(self):
        try:
            self.logger.info(f"Redirecting to webpage {self.URL}")
            self.driver.get(self.URL)
        except Exception as e:
            self.logger.error(f"Unable to redirect: {e}")
               
    def _action_click(self): 
        try:
            self.ELEMENT.click()
            if self.NEW_WINDOW:
                WebDriverWait(self.driver, self.TIMEOUT).until(lambda d: len(d.window_handles) > len(self.window_stack))
                new_tab = [h for h in self.driver.window_handles if h not in self.window_stack][0]
                self.driver.switch_to.window(new_tab)
                self.window_stack.append(new_tab)
            self.logger.info(f"Clicking Element")
        except Exception as e:
            self.logger.error(f"Failed to Click Element: {str(e)}")
            
    def _action_pdf(self):
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            result = self.driver.execute_cdp_cmd("Page.printToPDF", {
                "printBackground": self.PRINT_BACKGROUND,
                "landscape": self.LANDSCAPE
                })

            #save + output
            pdf_data = result['data']
            file_path = Helper.create_path(self.OUTPUT_PATH, f"{self.pdf_name}.pdf")
            Helper.write_binary_file(file_path, base64.b64decode(pdf_data)) #decoded as chromeDevTool returns base64 coded data
            self.logger.info(f"Saved printed PDF to {file_path}")

        except Exception as e:
            self.logger.error(f"Failed to print page to PDF: {str(e)}")
    
    #Packet Functions    
    def _generate_packet(self,content):
        return {
            "action":self.ACTION_TYPE,
            "uid": Helper.generate_uid(),
            "timestamp":datetime.now().strftime("%d%m%Y %H:%M:%S"),
            "webpage": self.driver.current_url,
            "data_present":bool(content and not any(k in content for k in ["skip", "error"])),
            "log_message":self.LOG_MESSAGE,
            "response":content if content else None
        }
    
    def __generate_response_packet(self, *content):
        pass
    
    def execute_blocks(self, block: list):  
        block_data = []
        for idx,_action_ in enumerate(block):
            data = self.execute(_action_)
            if data:
                block_data.append(data)
        return block_data
    