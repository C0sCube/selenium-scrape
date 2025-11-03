import time, traceback, random
from datetime import datetime
import undetected_chromedriver as uc
import pygetwindow as gw, time

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC

from app.logger import get_global_logger
from app.utils import Helper
from app.constants import *
from app.actions import (
    downloadElem,textScrape, htmlScrape, 
    clickSave,clickElem, genPdf,genSst, 
    webRedir, httpRequest, injectScript, apiGet,
    tabList, webList, manualAction,tablScrape
)


class ActionExecutor:
    def __init__(self):
        self.logger = get_global_logger()
        self.today = datetime.now()
        self.OUTPUT_PATH = Helper.create_dir(DATA_DIR,self.today.strftime("%Y-%m-%d"))
        self.data = {}
        self.driver = None
        self.window_stack = None
        self.PARAMS = None

        
        # ========== Locators and Conditions ==========
        
        self.locator_map = {
            "css": By.CSS_SELECTOR,
            "xpath": By.XPATH,
            "id": By.ID,
            "name": By.NAME,
            "class": By.CLASS_NAME,
            "tag": By.TAG_NAME,
            "txt": By.LINK_TEXT,
            "ptxt": By.PARTIAL_LINK_TEXT,
        }

        self.condition_map = {
            "clickable": EC.element_to_be_clickable,
            "visible": EC.visibility_of_element_located,
            "present": EC.presence_of_element_located,
            "invisible": EC.invisibility_of_element_located,
            "attached": EC.element_to_be_selected,
        }
        
        self.action_map = {
            "html": lambda: htmlScrape(self),
            "table": lambda: tablScrape(self),
            "scrape": lambda: textScrape(self),
            "pdf": lambda: genPdf(self),
            "screenshot": lambda: genSst(self),
            "click": lambda: clickElem(self), 
            "click_save": lambda: clickSave(self),
            "website": lambda: webRedir(self),
            "download": lambda: downloadElem(self),
            "redir_pdf": lambda: genPdf(self), 
            "tablist": lambda: tabList(self), 
            "weblist": lambda: webList(self), 
            "http": lambda: httpRequest(self),
            "manual": lambda: manualAction(self),
            "execute_script": lambda: injectScript(self),
            "api_get":lambda: apiGet(self),
        }
    
    def set_params(self,params):
        self.PARAMS = params
        self.OUTPUT_PATH = Helper.create_dir(DATA_DIR,self.today.strftime("%Y-%m-%d"),params['bank_name'],f"download_{datetime.now().strftime("%H%M")}")
        self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": self.OUTPUT_PATH
        })
        self.logger.info(f"Download Folder: {self.OUTPUT_PATH}")
    
    def create_uc_driver(self, headless=False, minimized=True):
        
        # --headless                         # Run Chrome in headless mode (no GUI)
        # --disable-gpu                     # Disable GPU hardware acceleration
        # --no-sandbox                      # Bypass OS security model (useful in Docker)
        # --disable-dev-shm-usage          # Avoid shared memory issues in containers
        # --start-maximized                # Start browser maximized
        # --window-size=1920,1080          # Set specific window size
        # --incognito                      # Launch in incognito mode
        # --disable-extensions             # Disable all Chrome extensions
        # --disable-blink-features=AutomationControlled  # Hide automation flags
        # --user-data-dir="path"           # Use custom Chrome user profile directory
        # --profile-directory="Profile 2"  # Specify profile folder inside user-data-dir
        # --remote-debugging-port=9222     # Enable remote debugging
        # --lang=en                        # Set browser language
        # --ignore-certificate-errors      # Skip SSL certificate errors
        # --disable-popup-blocking         # Allow popups
        # --disable-infobars               # Hide "Chrome is being controlled..." banner
        
        options = uc.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36")

        if headless:
            options.add_argument("--headless=new")

        options.add_experimental_option("prefs", {
            "download.default_directory": self.OUTPUT_PATH,
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        self.driver = uc.Chrome(options=options)
        width, height = 900, 700
        self.driver.set_window_size(width, height)
        
        # if minimized and not headless:
        #     try:
        #         # Instead of minimizing, place Chrome offscreen but keep it visible
        #         self.driver.set_window_size(900, 700)
        #         if window_position: self.driver.set_window_position(-3000, 0)
        #         self.logger.notice("Driver window hidden offscreen (visible, not minimized).")
        #     except Exception as e:
        #         self.logger.warning(f"Failed to hide window offscreen: {e}")
        #         self.window_stack = [self.driver.current_window_handle]
        #         return self.driver
        
        if minimized and not headless:
            try:
                time.sleep(1)
                title = self.driver.title or "data:,"
                for w in gw.getWindowsWithTitle(title):
                    w.minimize()
                    self.logger.notice("Chrome window minimized safely.")
                    break
            except Exception as e:
                self.logger.warning(f"Minimize failed, fallback to visible small window: {e}")
                self.driver.set_window_position(50, 50)
                self.driver.set_window_size(800, 600)

    
    def get_website(self, timeout = 60):
        if self.driver:
            try:
                self.driver.set_page_load_timeout(timeout)
                self.driver.get(self.PARAMS["base_url"])
                self.logger.info("Website Fetched.")
                WebDriverWait(self.driver, 10).until(lambda d: d.execute_script("return document.readyState") in ["interactive", "complete"])
                return
            except Exception:
                self.logger.warning("Page load hung — stopping manually.")
                self.driver.execute_script("window.stop();")
                return

        self.logger.warning(f"Driver not created. Couldnt get website.")
        return
    
    def execute(self, _action_: dict):
        self.__set_website_parameters(_action_)
        time.sleep(random.uniform(self.DEFAULT_WAIT / 1.2, self.DEFAULT_WAIT))
        self.ELEMENT = None

        try:
            self.logger.notice(f"Performing _action_: {self.ACTION_TYPE} on {self.VALUE}")

            if self.WAIT_UNTIL:

                cond = self.__get_condition(self.WAIT_UNTIL, self.WAIT_BY, self.WAIT_VALUE)
                try:
                    WebDriverWait(self.driver, self.TIMEOUT).until(cond)
                    # self.driver.execute_script("window.stop();")
                    # self.logger.info(f"Website Loading paused as {self.WAIT_VALUE} present in DOMME")
                
                except TimeoutException:
                    
                    page_state = self.driver.execute_script("return document.readyState")
                    html_len = len(self.driver.page_source)
                    console_logs = []
                    try:
                        console_logs = self.driver.get_log("browser")[-3:]
                    except Exception:
                        pass

                    self.logger.error(
                        f"[Timeout] Condition='{self.WAIT_UNTIL}', readyState='{page_state}', "
                        f"HTML length={html_len}, ConsoleLogs={console_logs}"
                    )
                    raise 

            self.ELEMENT = self.driver.find_element(self.BY, self.VALUE)
            content = self.__perform_action(action_type=self.ACTION_TYPE)

        except Exception as e:
            self.logger.error(f"Error in self.execute: [{type(e).__name__}] {str(e)}")
            self.logger.debug(f"Traceback:\n{traceback.format_exc()}")
            return self.__generate_packet([{
                "error_type": type(e).__name__,
                "error_message": str(e),
                "error_from": "ActionExecutor.execute"
            }])

        return self.__generate_packet(content) if content else self.__generate_packet([{
            "error_type": "NoneType",
            "error_message": "No content extracted.",
            "error_from": "ActionExecutor.execute"
        }])
    
    def __set_website_parameters(self,_action_:dict):
        
        self.ACTION_TYPE = _action_.get("action",None)
        #get
        self.BY = self.__get_by(_action_.get("by", "css"))
        self.VALUE = _action_.get("value")
        self.URL = _action_.get("url","https://tinyurl.com/nothing-borgir")
        
        #weblink header
        self.WEBLINKS = _action_.get("web_links",None)
        self.WEBLINKS_HEADER = _action_.get("web_link_headers",[])
        
        #cookies
        self.COOKIES = {c['name']: c['value'] for c in self.driver.get_cookies()}
        
        #headers
        self.HEADERS = dict(_action_.get("headers", {}))
        self.HEADERS.update({"User-Agent":self.driver.execute_script("return navigator.userAgent")})
        self.HEADERS.update({"Referer": self.driver.current_url})
        
        #API data
        
        self.BASE_API = _action_.get("base_api", None)  # can be a list or dict
        self.DOMAIN = _action_.get("domain", None)  # domain for cookies
        self.VERIFY_REQUEST = _action_.get("verify_request", True)
        self.API_RULE = _action_.get("resp_structure",{})
        self.NULL_RESPONSE = _action_.get("null_response",{})
        
        #time
        self.DEFAULT_WAIT = _action_.get("default_wait", 2)
        self.TIMEOUT = _action_.get("timeout", 15)
        self.WAIT_UNTIL = _action_.get("wait_until")
        self.WAIT_BY = _action_.get("wait_by", self.BY) #dependent
        self.WAIT_VALUE = _action_.get("wait_value", self.VALUE) #dependent
        self.WAIT_TIMEOUT = _action_.get("wait_timeout", self.TIMEOUT)
            
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
        
        #script
        self.SCRIPT_KEY = _action_.get("script_key")
        self.SCRIPT = _action_.get("script")

        return
    
    def __perform_action(self,action_type = None):
        if not action_type:
            self.logger.info(f"Checked presence of element: {self.BY}={self.VALUE}")
            return

        action = self.action_map.get(action_type)
        if action:
            result = action()
            if result is not None:
                return result
        else:
            self.logger.warning(f"Unknown action type: {self.ACTION_TYPE}")
    
    def __get_condition(self, wait_type, by, value):
        condition_func = self.condition_map.get(wait_type.lower())
        if not condition_func:
            raise ValueError(f"Unknown wait condition: {wait_type}")

        locator = (self.locator_map.get(by.lower(), By.CSS_SELECTOR), value)
        return condition_func(locator)

    def __get_by(self, by_string):
        mapping = {
            "css": By.CSS_SELECTOR,
            "xpath": By.XPATH,
            "id": By.ID,
            "name": By.NAME,
            "class": By.CLASS_NAME,
            "tag": By.TAG_NAME,
            "txt":By.LINK_TEXT,
            "ptxt":By.PARTIAL_LINK_TEXT
        }
        return mapping.get(by_string.lower(), By.CSS_SELECTOR)
     
    def __generate_packet(self, content):
        packet = {
            "action": self.ACTION_TYPE,
            "uid": Helper.generate_uid(),
            "timestamp": datetime.now().strftime("%d%m%Y %H:%M"),
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

    #BLOCK EXECUTION
    def execute_blocks(self):
        block_data = []
        generic_actions = GENERIC_ACTIONS

        block = self.PARAMS["blocks"]
        self.logger.notice(f"Total Action(s) {len(block)}")

        for _, _action_ in enumerate(block):
            data = None

            if isinstance(_action_, str):
                action_key, *content = _action_.split("||")
                if action_key not in generic_actions:
                    self.logger.warning(f"{action_key} not part of generic_action_keys. Skipping.")
                    continue

                do_action = generic_actions[action_key].copy()

                if action_key == "action_website":
                    do_action.update({"url": content[0]})
                elif action_key == "action_download":
                    by, value, multiple, wait_until = content
                    do_action.update({
                        "by": by,
                        "value": value,
                        "multiple": bool(multiple),
                        "wait_until": wait_until
                    })

            elif isinstance(_action_, dict):
                do_action = _action_

            else:
                self.logger.warning(f"Unsupported action format: {_action_}")
                continue

            data = self.execute(do_action)
            if data:
                block_data.append(data)

        return block_data




    