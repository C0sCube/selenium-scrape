from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver 
from bs4 import BeautifulSoup
from datetime import datetime, date
from urllib.parse import urlencode
# from io import StringIO
from urllib.parse import urljoin, urlparse

import re, os, time, logging ,pprint, requests, base64, traceback, random,hashlib
import undetected_chromedriver as uc
from app.utils import Helper
# from app.action_config import ActionConfig
from app.constants import *

class ActionExecutor:
    def __init__(self,logger=None, params=None, paths=None):
        self.logger = logger or logging.getLogger(__name__)
        self.PARAMS = params or {}
        self.PATHS = paths or {}
        self.data = {}

        self.DATE = datetime.now()
        self.OUTPUT_PATH = Helper.create_dir(paths["output"],paths["folders"]["data"],self.DATE.strftime("%Y-%m-%d"),params["bank_name"])

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
        options.add_argument("--disable-extensions")
        
        #preferential download
        options.add_experimental_option("prefs", {
            "download.default_directory": self.OUTPUT_PATH,
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        
        self.driver = uc.Chrome(options=options)
        
        width,height = self.PARAMS["intial_window_size"]
        self.driver.set_window_size(width,height)
        time.sleep(random.uniform(0.5, 2.5))
        
        self.window_stack = [self.driver.current_window_handle]
        return self.driver
    
    def __attach_headers(self):
        self.driver.execute_cdp_cmd("Network.enable", {})
        headers = self.PARAMS.get("headers", {})
        if headers:
            self.driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
                "headers": headers
            })
        
    def execute(self, _action_: dict):
        self.ACTION_TYPE = _action_.get("action",None)
        #get
        self.BY = self.__get_by(_action_.get("by", "css"))
        self.VALUE = _action_.get("value")
        self.URL = _action_.get("url","https://tinyurl.com/nothing-borgir")
        
        #weblink header
        self.WEBLINKS = _action_.get("web_links",None)
        self.WEBLINKS_HEADER = _action_.get("web_link_headers",[])
        
        #time
        self.DEFAULT_WAIT = _action_.get("default_wait", 2)
        self.TIMEOUT = _action_.get("timeout", 15)
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
        
        self.CLEAN_TABLE = _action_.get("clean_table",True)
        
        #field
        self.ATTRIBUTE = _action_.get("attribute")
        self.SCRAPE_FIELDS = _action_.get("scrape_fields")
        self.FOLLOW_UP_ACTIONS =  _action_.get("steps")
        
        self.RUN_FUNCTION = _action_.get("execute",None)
        self.ALLOWED_TABS = _action_.get("allowed_tabs",[])
        
        #save
        self.CONSOLIDATE_SAVE = _action_.get("consolidate_save", False)
        self.MULTIPLE = _action_.get("multiple",False)
        self.FILE_SAVE = _action_.get("file_save",False)
        
        #page pdf
        self.LANDSCAPE = _action_.get("landscape",False)
        self.PRINT_BACKGROUND = _action_.get("print_background",False)
        
        #window
        self.NEW_WINDOW = _action_.get("new_window", False)
        self.RETURN_TO_BASE = _action_.get("return_to_base", False)
        
        time.sleep(random.uniform(self.DEFAULT_WAIT/2, self.DEFAULT_WAIT))
        
        
        #element; The Which gets loaded as default
        self.ELEMENT = None
        try:
            self.logger.notice(f"Performing _action_: {self.ACTION_TYPE} on {self.VALUE}")
            
            if self.WAIT_UNTIL:
                condition = self.__get_condition(self.WAIT_UNTIL, self.WAIT_BY, self.WAIT_VALUE)
                WebDriverWait(self.driver, self.TIMEOUT).until(condition)
        
            self.ELEMENT = self.driver.find_element(self.BY, self.VALUE)
            content = self.__perform_action(action_type = self.ACTION_TYPE)
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            self.logger.error(f"Error in self.execute: [{error_type}] {error_msg}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            return self.__generate_packet([{"error_type": error_type, "error_message": error_msg, "error_from":"ActionExecutor.execute"}]) # skip further execution

        if self.NEW_WINDOW:
            self.logger.info("Switching to NEW WINDOW (latest handle).")
            self.driver.switch_to.window(self.driver.window_handles[-1])
            self.window_stack.append(self.driver.current_window_handle)

        if self.RETURN_TO_BASE:
            self.logger.info("Returning to Base Window.")
            if len(self.window_stack) > 1:
                self.driver.close()
                self.window_stack.pop()
                self.driver.switch_to.window(self.window_stack[-1])
        return self.__generate_packet(content) if content else self.__generate_packet([{"error_type": "NoneType", "error_message": "No content extracted from action.","error_from":"ActionExecutor.execute"}])

    def __perform_action(self,action_type = None):
        action_map = {
            "click": self.clickElem,
            "click_save":self.clickSave,
            "html": self.htmlScrape,
            "table": self.tablScrape,
            "scrape": self.textScrape,
            "website": self.webRedir,
            "download": self.downloadElem,
            "pdf": self.genPdf,
            "redir_pdf":self.genPdf,
            "screenshot": self.genSst,
            "tablist": self.tabList,
            "weblist":self.webList,
            "http":self.httpRequest,
            "manual":self.manualAction,
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
        
    def __get_by(self, by_string):
        mapping = {
            "css": By.CSS_SELECTOR,
            "xpath": By.XPATH,
            "id": By.ID,
            "name": By.NAME,
            "class": By.CLASS_NAME,
            "tag": By.TAG_NAME,
            "txt":By.LINK_TEXT
        }
        return mapping.get(by_string.lower(), By.CSS_SELECTOR)

    def __get_condition(self, wait_type, by, value):
        cond_map = {
            "clickable": EC.element_to_be_clickable,
            "visible": EC.visibility_of_element_located,
            "present": EC.presence_of_element_located,
            "invisible": EC.invisibility_of_element_located,
            "attached": EC.element_to_be_selected
        }

        locator = (self.__get_by(by), value)
        condition_func = cond_map.get(wait_type)
        if not condition_func:
            raise ValueError(f"Unknown wait condition: {wait_type}")
        return condition_func(locator)
    
     #Packet Functions
     
    def __generate_packet(self, content):
        packet = {
            "action": self.ACTION_TYPE,
            "uid": Helper.generate_uid(),
            "timestamp": datetime.now().strftime("%d%m%Y %H:%M:%S"),
            "webpage": self.driver.current_url,
            "data_present": not any(
                key in entity for entity in content
                for key in ["status", "error_type", "error_message", "error"]
            ),
            "log_message": self.LOG_MESSAGE,
            "response_count": len(content),
            "response": content if content else None
        }

        if self.ACTION_TYPE == "tablist":
            packet["tab_found"] = getattr(self, "TABS_FOUND", [])
            packet["follow_ups"] = [step.get("action") for step in getattr(self, "FOLLOW_UP_ACTIONS", [])if "action" in step]
        
        if self.ACTION_TYPE == "weblist":
            packet["web_links"] = getattr(self, "WEBLINKS", [])
            packet["follow_ups"] = [step.get("action") for step in getattr(self, "FOLLOW_UP_ACTIONS", [])if "action" in step]

        return packet
    
    def __generate_resp_packet(self, name = "",header="",value = None,type = ""):
        return {
            "name":name,
            "title":header,
            "value":value,
            "type":type,
            "data_present": bool(value),
            "hash": hashlib.sha256(value.encode("utf-8")).hexdigest() if value else None
        }
    
    def __scroll_to_bottom(self):
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    
    # ===================== ACTION =====================
    
    #DOM-Scrape Actions
    def textScrape(self)->dict: #Have to write this better
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
    
    def tablScrape(self)->list:
        self.logger.info(f"Scraping Using BY={self.BY} and VALUE={self.VALUE}")
        elements = self.driver.find_elements(self.BY,self.VALUE) if self.MULTIPLE else [self.driver.find_element(self.BY, self.VALUE)]
            
        if self.BY == By.CSS_SELECTOR: #mandatory filter
            elements = [elem for elem in elements if elem.tag_name.lower() == "table"]
        self.logger.info(f"Total Elements Found By={self.BY} and Value={self.VALUE} are {len(elements)}")
        
        scrape_content,cleaned_tables = [],[]
        for idx, elem in enumerate(elements):
            header = ActionExecutorHelper._find_preceding_texts_(elem)
            self.logger.info(f"Table: {idx} has header:: {header}")
            
            raw_html = elem.get_attribute("outerHTML")
            final_html = ActionExecutorHelper._clean_raw_table_html_(raw_html) if self.CLEAN_TABLE else raw_html
            
            cleaned_tables.append(final_html)
            scrape_content.append(self.__generate_resp_packet(name=f"{self.table_name}_{idx}",value=final_html,header=header,type="table_html"))
        return scrape_content
    
    # __find_preceding_texts moved to dump.ipynb

    def htmlScrape(self)->list:
        self.logger.info(f"Scraping Using BY={self.BY} and VALUE={self.VALUE}")
        elements = self.driver.find_elements(self.BY, self.VALUE) if self.MULTIPLE else [self.driver.find_element(self.BY, self.VALUE)] 
            
        cleaned_content = []
        scrape_content = []
        for idx, elem in enumerate(elements):
            html_content = elem.get_attribute("outerHTML")
            if not html_content:
                self.logger.warning(f"No HTML content found for element: {self.VALUE}")
                continue
            
            cleaned_content.append(html_content)
            scrape_content.append(self.__generate_resp_packet(name=f"{self.html_name}_{idx}",value=html_content,header="HTML DOESNT HAVE HEADER",type="html"))

        # if self.FILE_SAVE:
        #     for idx,content in enumerate(cleaned_content):
        #         html_path = os.path.join(self.OUTPUT_PATH,f"html_content_{idx}.html")
        #         with open(html_path,"w") as f:
        #             f.write(content)
            
        #     self.logger.info(f"Saved Html Data.") 

        return scrape_content
    
    def tabList(self) -> list:
        self.logger.info(f"Tab List Loop Using BY={self.BY} and VALUE={self.VALUE}")
        tabList = self.driver.find_elements(self.BY, self.VALUE)
        self.logger.info(f"Total Elements Found By={self.BY} and Value={self.VALUE} are {len(tabList)}")
        
        scrape_content = []
        tab_names = []
        follow_ups = self.FOLLOW_UP_ACTIONS
        tablist_log = self.LOG_MESSAGE
        

        for idx, tab in enumerate(self.driver.find_elements(self.BY, self.VALUE)):
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab)
                ActionChains(self.driver).move_to_element(tab).perform()
                self.driver.execute_script("arguments[0].click();", tab)
                
                tabName = tab.get_attribute("innerText").strip()
                tab_names.append(tabName)
                self.logger.notice(f"Clicked Tab >> {tabName}")

                time.sleep(0.5)
                for step in follow_ups:
                    if step.get("wait_until"):
                        condition = self.__get_condition(step["wait_until"], step["by"], step["value"])
                        WebDriverWait(self.driver, step["timeout"]).until(condition)

                    result = self.execute(step)
                    if not result:
                        self.logger.warning(f"No result returned for step {step['action']} on tab[{idx}]={tabName}")
                        continue

                    step_content = result.get("response", [])
                    for packet in step_content:
                        packet["tabname"] = tabName if tabName else "NOT DEFINED"
                    scrape_content.extend(step_content)

            except Exception as e:
                self.logger.warning(f"Failed on tab[{idx}]: {e}")
                
        #reset val
        self.TABS_FOUND = tab_names
        self.ACTION_TYPE = "tablist"
        self.LOG_MESSAGE = tablist_log
        self.FOLLOW_UP_ACTIONS = follow_ups
        return scrape_content

        # i = 0
        # while True:
        #     try:
        #         tabs = self.driver.find_elements(self.BY, self.VALUE)
        #         if i >= len(tabs):
        #             break  # stop if no more tabs left

        #         tab = tabs[i]
        #         self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab)
        #         ActionChains(self.driver).move_to_element(tab).perform()
        #         self.driver.execute_script("arguments[0].click();", tab)

        #         time.sleep(0.5)
        #         for step in follow_ups:
        #             if step.get("wait_until"):
        #                 condition = self.__get_condition(step["wait_until"], step["by"], step["value"])
        #                 WebDriverWait(self.driver, step["timeout"]).until(condition)

        #             result = self.execute(step)
        #             if result:
        #                 pprint.pprint(result.get("response"))

        #         i += 1
        #     except Exception as e:
        #         self.logger.warning(f"Failed on tab[{i}]: {e}")
        #         i += 1

    def webList(self)->list:
        self.logger.info(f"Performing weblist action of {len(self.WEBLINKS)} website(s)")
        scrape_content = []
        follow_ups = self.FOLLOW_UP_ACTIONS
        tablist_log = self.LOG_MESSAGE
        
        if not self.WEBLINKS:
            self.logger.error(f"No urls attatched for weblist function to perform.")
            return scrape_content

        elif isinstance(self.WEBLINKS,list):
            weblinks = self.WEBLINKS
        
        elif isinstance(self.WEBLINKS,dict):
            base_url = self.WEBLINKS["base_url"]
            params = self.WEBLINKS["params"]
            weblinks = ActionExecutorHelper.build_multiple_urls(base_url,params)
        
        
        weblink_headers = []
        if self.WEBLINKS_HEADER:
            weblink_headers = self.WEBLINKS_HEADER.split("||")
        
        for idx, url in enumerate(weblinks):
            try:
                self.driver.get(url)
                self.logger.notice(f"Redirecting to: {url}")
                for step in follow_ups:
                    if step.get("wait_until"):
                        condition = self.__get_condition(step["wait_until"], step["by"], step["value"])
                        WebDriverWait(self.driver, step["timeout"]).until(condition)
                        result = self.execute(step)
                        
                    if not result:
                        self.logger.warning(f"No result returned for step {step['action']}")
                        continue
                    
                    step_content = result.get("response", [])
                    if weblink_headers:
                        web_link_header = weblink_headers[idx]
                        for step in step_content:
                            # pprint.pprint(step)
                            if step.get("data_present"):
                                titles = step.get("title",[])
                                if titles:titles.append(web_link_header)
                                else: titles = [web_link_header]
                                step.update({"title":titles})
                    scrape_content.extend(step_content)
            
            except Exception as e:
                self.logger.warning(f"Failed on url:{url}:: {e}")
                         
        self.ACTION_TYPE = "tablist"
        self.LOG_MESSAGE = tablist_log
        self.FOLLOW_UP_ACTIONS = follow_ups
        return scrape_content

    
    def downloadElem(self):
        
        elements = self.driver.find_elements(self.BY, self.VALUE) if self.MULTIPLE else [self.driver.find_element(self.BY, self.VALUE)]
        self.logger.info(f"Total Elements Found By={self.BY} and Value={self.VALUE} are {len(elements)}")
        scrape_content = []
        
        #Helper
        for idx, elem in enumerate(elements):
            try:
                self.driver.execute_script("arguments[0].scrollIntoView(true);", elem)
                file_url = self.__extract_html_href(elem)

                if not file_url:
                    self.logger.warning(f"No valid URL at index {idx}")
                    continue

                file_url = urljoin(self.driver.current_url, file_url)
                file_type = ActionExecutorHelper._determine_file_type(file_url)

                if file_type:
                    try:
                        output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["downloads"])
                        file_content = self.__download_file(file_url, output_dir, idx, file_type)
                        scrape_content.append(self.__generate_resp_packet(name=f"{self.pdf_name}_{idx}",header=os.path.basename(urlparse(file_url).path),value=file_content,type=file_type))
                    except Exception as e:
                        self.logger.error(f"Download failed at index {idx}: {e}")
                # else:
                #     self.logger.info("Triggered click for file download.")
                #     elem.click()

            except Exception as e:
                self.logger.error(f"Error at index {idx}: {e}")
                
        return scrape_content
    
    def __extract_html_href(self,elem):
            url = elem.get_attribute("href")
            if not url:
                try:
                    link_elem = elem.find_element(By.TAG_NAME, "a")
                    url = link_elem.get_attribute("href")
                except:
                    url = None
            return url
    
    def __download_file(self, file_url, output_dir, idx, extension):
        cookies = {c['name']: c['value'] for c in self.driver.get_cookies()}
        r = requests.get(file_url, cookies=cookies, verify=False) #check this out
        self.logger.notice(f" `{extension}` GET Request Returned Status: {r.status_code}")
        
        
        encoded_data = ""
        if r.status_code == 200:

            parsed_url = urlparse(file_url)
            raw_filename = os.path.basename(parsed_url.path)
            safe_filename = Helper.sanitize_Win_filename(raw_filename)
    
            if not safe_filename: safe_filename = f"file_{idx}.{extension}"

            file_path = os.path.join(output_dir, safe_filename)
            file_data = r.content
            
            if len(file_data)>=MAX_REQUEST_BYTE_SIZE:
                self.logger.warning(f"Skipped {safe_filename} — size {len(file_data)} exceeds limit.")
                return encoded_data
            
            encoded_data = base64.b64encode(file_data).decode("utf-8")
            if self.FILE_SAVE:
                Helper.write_binary_file(file_path, file_data)
                self.logger.info(f"Downloaded {extension.upper()} to {file_path}")
                
        else:
            self.logger.error(f"Failed to download {extension.upper()}, returning empty str.")
        
        return encoded_data
    
    def manualAction(self, _timeout = MAX_DOWNLOAD_TIMEOUT, _wait = MAX_DOWNLOAD_WAIT):
        self.logger.info("Waiting for user to download PDF...")
        file_path,ext = ActionExecutorHelper._wait_for_download(self.OUTPUT_PATH, timeout=_timeout)
        time.sleep(_wait)
        scrape_content = []
        if file_path:
            with open(file_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")
                scrape_content.append(self.__generate_resp_packet(name=f"{self.pdf_name}",header=os.path.basename(file_path),value=encoded,type=ext))
        else:
            self.logger.warning("No valid downloaded file found within timeout.")

        return scrape_content
      
    def genSst(self):
        try:
            scrape_content = []
            result = self.driver.execute_cdp_cmd("Page.captureScreenshot", {"captureBeyondViewport": True,"fromSurface": True})
            
            encoded_data = result['data']
            if self.FILE_SAVE:
                file_path = Helper.create_path(self.OUTPUT_PATH, f"{self.pdf_name}-{Helper.generate_uid()}.png")
                Helper.write_binary_file(file_path, base64.b64decode(encoded_data))
                self.logger.save(f"Saved screenshot to {file_path}")
            
            scrape_content.append(self.__generate_resp_packet(name=f"{self.pdf_name}",header="",value=encoded_data,type=self.ACTION_TYPE))
        except Exception as e:
            self.logger.warning(f"Failed to save screenshot: {str(e)}")
        
        return scrape_content

    def genPdf(self):
        try:
            if self.ACTION_TYPE == "redir_pdf":
                self.driver.switch_to.window(self.driver.window_handles[-1])
                self.driver.maximize_window()
                self.driver.execute_script("document.body.style.zoom='100%'")
                time.sleep(2)

            self.__scroll_to_bottom()
            
            scrape_content = []
            width = self.driver.execute_script("return document.body.scrollWidth")
            height = self.driver.execute_script("return document.body.scrollHeight")

            dpi = 96
            paper_width = width / dpi
            paper_height = height / dpi

            #"landscape": self.LANDSCAPE
            result = self.driver.execute_cdp_cmd("Page.printToPDF", {
                "printBackground": self.PRINT_BACKGROUND,                                                     
                "paperWidth": paper_width,
                "paperHeight": paper_height,
            })

            #save + output
            encoded_data = result['data']
            if self.FILE_SAVE:
                file_path = Helper.create_path(self.OUTPUT_PATH, f"{self.pdf_name}-{Helper.generate_uid()}.pdf")
                Helper.write_binary_file(file_path, base64.b64decode(encoded_data))
                self.logger.save(f"Saved printed PDF to {file_path}")
            
            scrape_content.append(self.__generate_resp_packet(name=f"{self.pdf_name}",header="",value=encoded_data,type=self.ACTION_TYPE))

        except Exception as e:
            self.logger.warning(f"Failed to print page to PDF: {str(e)}")
        
        return scrape_content
    
    def webRedir(self):
        try:
            self.logger.info(f"Redirecting to webpage {self.URL}")
            self.driver.get(self.URL)
        except Exception as e:
            self.logger.error(f"Unable to redirect: {e}")
               
    def clickElem(self): 
        try:
            self.ELEMENT.click()
            # if self.NEW_WINDOW:
            #     WebDriverWait(self.driver, self.TIMEOUT).until(lambda d: len(d.window_handles) > len(self.window_stack))
            #     new_tab = [h for h in self.driver.window_handles if h not in self.window_stack][0]
            #     self.driver.switch_to.window(new_tab)
            #     self.window_stack.append(new_tab)
            self.logger.info(f"Clicking Element")
        except Exception as e:
            self.logger.error(f"Failed to Click Element: {str(e)}")
        
    def clickSave(self):
        try:
            scrape_content = []
            self.logger.info(f"Clicking Element")
            # self.ELEMENT.click()
            self.driver.execute_script("arguments[0].click();", self.ELEMENT)
            
            file_path,ext = ActionExecutorHelper._wait_for_download(self.OUTPUT_PATH, timeout=self.TIMEOUT)
            if file_path:
                with open(file_path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode("utf-8")
                scrape_content.append(self.__generate_resp_packet(name=f"{self.pdf_name}",header=os.path.basename(file_path),value=encoded,type=ext))
                self.logger.save("Saved downloaded file in cache.")
            else:
                self.logger.warning("No valid downloaded file found within timeout. Empty cache")

        except Exception as e:
            self.logger.error(f"Failed to Click Element: {str(e)}")
        
        return scrape_content 
            
    def httpRequest(self):
        self.logger.info("Performing GET REQUEST for attached website.")
        file_url = self.URL
        file_type = self.export_format or "dat"
        output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["downloads"])
        scrape_content = []
        try:
            file_content = self.__download_file(file_url, output_dir, 0, file_type)
            scrape_content.append(self.__generate_resp_packet(name=f"{self.pdf_name}",header=os.path.basename(urlparse(file_url).path),value=file_content,type=file_type))
        except Exception as e:
            self.logger.warning(f"Failed to perform http request: {str(e)}")
            
        return scrape_content
    
    def execute_blocks(self, block: list):  
        block_data = []
        generic_actions = GENERIC_ACTION_CONFIG
        self.logger.notice(f"Total Action(s) {len(block)}")
        for _,_action_ in enumerate(block):
            
            if isinstance(_action_,str):
                if _action_ not in generic_actions:
                    self.logger.warning(f" {_action_} not part of generic_action_keys. Skipping.")
                else:
                    _action_ = generic_actions.get(_action_)   
            data = self.execute(_action_)
            if data:
                block_data.append(data)
        return block_data



#Internal Helper
class ActionExecutorHelper:
    
    def __init__(self):
        pass
    
    @staticmethod
    def _find_preceding_texts_(table, n=2):
        texts = []
        current = table
        label_tags = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "strong", "a", "span","div"}
        MAX_TEXT_LENGTH = 350
        while len(texts) < n:
            try:
                parent = current.find_element(By.XPATH, "..")
                siblings = parent.find_elements(By.XPATH, "preceding-sibling::*")
                for sib in reversed(siblings):
                    if sib.tag_name.lower() in ["table", "br", "hr"]:
                        continue
                    
                    if sib.find_elements(By.TAG_NAME,"table"):
                        continue
                    
                    if sib.tag_name.lower() not in label_tags:
                        continue
                    
                    if sib.tag_name.lower() == "div":
                        if not sib.find_elements(By.XPATH, ".//h1 | .//h2 | .//h3 | .//p | .//strong | .//a | .//span"):
                            continue

                    txt = sib.get_attribute("innerText").strip()
                    # txt = sib.text.strip()
                    txt = Helper._remove_tabspace(txt)
                    txt = Helper._normalize_whitespace(txt)
                    if txt and len(txt)<MAX_TEXT_LENGTH:
                        texts.append(txt)
                        if len(texts) == n:
                            return list(reversed(texts))
                current = parent
            except:
                break
        return list(reversed(texts)) if texts else ["No label found"]*n

    @staticmethod
    def _clean_raw_table_html_(rawr):
        rawr = Helper.apply_sub(rawr, r'<th\b', '<td', ignore_case=True)
        rawr = Helper.apply_sub(rawr, r'</th\b', '</td', ignore_case=True)
        
        #tbody
        rawr = re.sub(r"<thead\b",r"<tbody",rawr, re.IGNORECASE)
        rawr = re.sub(r"</thead\b",r"</tbody",rawr, re.IGNORECASE)
        
        #other tags
        rawr = Helper.apply_sub(rawr, r"</?(?:strong|sup|b|p|br)(?:\s+[^>]*)?>",ignore_case=True)
        rawr = Helper.apply_sub(rawr,r'[*@\n\t]+', ignore_case=True)
        rawr = Helper.apply_sub(rawr,r"<tr[^>]*>\s*(?:&nbsp;|\u00A0|\s)*</tr>", ignore_case=True)
        rawr = Helper._normalize_whitespace(rawr)
        
        soup = BeautifulSoup(rawr, "html.parser")
        ALLOWED = {"rowspan", "colspan"}
        for tag in soup.find_all(True):
            for attr in list(tag.attrs):
                if attr not in ALLOWED:
                    del tag.attrs[attr]
        final_html = str(soup)
        return final_html
    
    @staticmethod
    def _determine_file_type(url):
        if ".pdf" in url: return "pdf"
        if url.endswith(".csv"): return "csv"
        if url.endswith(".docx"): return "docx"
        if url.endswith(".xlsx"): return "xlsx"
        return None
    
    @staticmethod
    def _wait_for_download(folder, timeout=30):
        start_time = time.time()
        initial_files = set(os.listdir(folder))

        while time.time() - start_time < timeout:
            current_files = set(os.listdir(folder))
            new_files = current_files - initial_files
            if new_files:
                for fname in new_files:
                    path = os.path.join(folder, fname)
                    if ext:=ActionExecutorHelper._determine_file_type(path):
                        return path, ext
            time.sleep(1)

        return None,None
    
    @staticmethod
    def build_multiple_urls(base_url, params):
        from itertools import product
        urls = []
        constant_params = {}
        list_params = {}
        
        for key, value in params.items():
            if key == "date":constant_params[key] = datetime.now().strftime(value)
            elif isinstance(value, str):constant_params[key] = value
            elif isinstance(value, list):list_params[key] = value

        if not list_params:
            query_string = urlencode(constant_params)
            urls.append(f"{base_url}?{query_string}")
            return urls

        keys = list(list_params.keys())
        values = list(list_params.values())
        for combo in product(*values):
            combo_dict = dict(zip(keys, combo))
            full_params = {**constant_params, **combo_dict}
            query_string = urlencode(full_params)
            urls.append(f"{base_url}?{query_string}")
        return urls



    