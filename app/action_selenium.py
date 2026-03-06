import time, traceback, random
import subprocess
import re
from datetime import datetime
import pygetwindow as gw #type: ignore
import undetected_chromedriver as uc #type: ignore
from selenium.webdriver.common.by import By #type: ignore
from selenium.webdriver.support.ui import WebDriverWait #type: ignore
from selenium.common.exceptions import TimeoutException #type: ignore
from selenium.webdriver.support import expected_conditions as EC #type: ignore

from app.logger import get_global_logger
from app.utils import Helper
from app.constants import *
from app.actions import (
    downloadElem,textScrape, htmlScrape, selectList,
    clickSave,clickElem, genPdf,genSst, 
    webRedir, httpRequest, injectScript, apiGet,
    tabList, webList, manualAction,tablScrape, inputAction,
    repList, dummyTable
)


class ActionSelenium:
    def __init__(self):
        self.logger = get_global_logger()
        self.today = datetime.now()
        self.OUTPUT_PATH = None
        self.data = {}
        self.driver = None
        self.window_stack = None
        self.PARAMS = None
        self.GENERIC_ACTIONS = load_gen_config()

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

        # ========== Locators and Conditions ==========
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
            "text": lambda: textScrape(self),
            "pdf": lambda: genPdf(self),
            "screenshot": lambda: genSst(self),
            "click": lambda: clickElem(self), 
            "click_save": lambda: clickSave(self),
            "website": lambda: webRedir(self),
            "download": lambda: downloadElem(self),
            "redir_pdf": lambda: genPdf(self), 
            "tablist": lambda: tabList(self), 
            "weblist": lambda: webList(self), 
            "replist": lambda: repList(self),
            "http": lambda: httpRequest(self),
            "manual": lambda: manualAction(self),
            "execute_script": lambda: injectScript(self),
            "api_get":lambda: apiGet(self),
            "select":lambda: selectList(self),
            "input":lambda: inputAction(self),
            "dummy": lambda: dummyTable(self)
        }
        
    def create_uc_driver(self, headless=False, minimized=True):
        
        # --headless                       # Run Chrome in headless mode (no GUI)
        # --disable-gpu                    # Disable GPU hardware acceleration
        # --no-sandbox                     # Bypass OS security model (useful in Docker)
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
        
        # chrome_major = self.get_chrome_major_version()
        chrome_major = 144

        options = uc.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_major}.0.0.0 Safari/537.36")
        
        if headless: options.add_argument("--headless=new")

        options.add_experimental_option("prefs", {
            "download.default_directory": self.OUTPUT_PATH,
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        

        self.driver = uc.Chrome(
            options=options,
            version_main=chrome_major
        )
        self.driver.set_window_size(900, 700)
        self.driver.set_window_position(-800, 100)
        if minimized and not headless:
            try:
                time.sleep(1)    
                title = self.driver.title or "data:,"
                for w in gw.getWindowsWithTitle(title):
                    w.minimize()
                    self.logger.info("Chrome window minimized safely.")
                    break
            except Exception as e:
                self.logger.warning(f"Minimize failed, fallback to visible small window: {e}")


    def _restore_window(self):
        """Restore or bring Chrome to front if minimized/tiny."""
        try:
            self.logger.info("Restoring Chrome window...")
            self.driver.set_window_size(900, 700)
            self.driver.set_window_position(-700, 200)
            title = self.driver.title or "data:,"
            for w in gw.getWindowsWithTitle(title):
                w.activate()
                break
            time.sleep(0.5)
            self.logger.info("Chrome restored to visible window.")
        except Exception as e:
            self.logger.warning(f"Could not restore window: {e}")

    def _minimize_window(self):
        """Minimize or shrink Chrome after action completes."""
        try:
            self.logger.info("Minimizing Chrome window...")
            title = self.driver.title or "data:,"
            for w in gw.getWindowsWithTitle(title):
                w.minimize()
                break
            time.sleep(0.3)
            self.logger.info("Chrome minimized again.")
        except Exception as e:
            # Fallback to shrink if minimize fails
            try:
                self.driver.set_window_size(250, 200)
                self.driver.set_window_position(10, 10)
                self.logger.info("Fallback: Chrome shrunk to 250x200.")
            except Exception as e2:
                self.logger.warning(f"Could not minimize or shrink: {e2}")
   