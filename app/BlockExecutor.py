
from datetime import datetime

from app.logger import get_global_logger, log_exceptions
from app.utils import Helper
from app.constants import *
from app.actions import (
    downloadElem,textScrape, htmlScrape, selectList,
    clickSave,clickElem, genPdf,genSst, 
    webRedir, httpRequest, injectScript, apiGet,
    tabList, webList, manualAction,tablScrape, inputAction,
    repList, dummyTable
)



class BlockExecutor:
    
    def __init__(self,runtime):
        
        #generic
        self.runtime = runtime
        self.LOGGER = get_global_logger()
        self.PARAMS = runtime.CONTRACT_PARAMS
        
        
        #selenium
        self.SELENIUM_DRIVER = runtime.DRIVER
        self.SELECTOR_MAP = runtime.SELENIUM_SELECTOR_MAP
        self.LOCATOR_MAP = runtime.SELENIUM_CONDITION_MAP
        
        #soup
        
        
        #actions (selenium + soup)
        self.GENERIC_ACTIONS = load_gen_config()
        self.ACTION_MAP = {
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
        
        
    def _generate_packet(self, content):
      
        packet = {
            "action": self.ACTION_TYPE,
            "uid": Helper.generate_uid(),
            "timestamp": datetime.now().strftime("%d%m%Y %H:%M"),
            "webpage": self.SELENIUM_DRIVER.current_url,
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

    def _perform_action(self,action_type = None):
        if not action_type:
            self.LOGGER.info(f"Checked presence of element: {self.BY}={self.VALUE}")
            return

        action = self.ACTION_MAP.get(action_type)
        if action:
            result = action()
            if result is not None:
                return result
        else:
            self.LOGGER.warning(f"Unknown action type: {self.ACTION_TYPE}")
            
    def _set_website_parameters(self, _action_:dict):
         
        DEFAULT_SELENIUM_WAIT = 3
        DEFAULT_SELENIUM_TIMEOUT = 15
        DEFAULT_THROTTLE = 3
    
        self.ACTION_TYPE = _action_.get("action",None)
        #get
        self.BY = self.SELECTOR_MAP(_action_.get("by", "css"))
        self.VALUE = _action_.get("value")
        self.URL = _action_.get("url","https://tinyurl.com/nothing-borgir")
        
        #weblink header
        self.WEBLINKS = _action_.get("web_links",None)
        self.WEBLINKS_HEADER = _action_.get("web_link_headers",[])
        
        # selenium specific
        self.COOKIES = {c['name']: c['value'] for c in self.SELENIUM_DRIVER.get_cookies()}
        self.HEADERS = dict(_action_.get("headers", {}))
        self.HEADERS.update({
            "User-Agent":self.SELENIUM_DRIVER.execute_script("return navigator.userAgent"),
            "Referer": self.SELENIUM_DRIVER.current_url
        })
        
        #bs4 soup specific
        self.SOUP_COOKIES = {c['name']: c['value'] for c in self.SELENIUM_DRIVER.get_cookies()}
        self.SOUP_HEADERS = dict(_action_.get("headers", {}))
        self.SOUP_HEADERS.update({
            "User-Agent":"",
            "Referer": ""
        })
 
        
        #API specific
        self.BASE_API = _action_.get("base_api", None)  # can be a list or dict
        self.DOMAIN = _action_.get("domain", None)  # domain for cookies
        self.VERIFY_REQUEST = _action_.get("verify_request", True)
        self.API_RULE = _action_.get("resp_structure",{})
        self.NULL_RESPONSE = _action_.get("null_response",{})
        self.THROTTLE = _action_.get("throttle",DEFAULT_THROTTLE)
        
        #toggle minimize
        self.MINIMIZE_TOGGLE = _action_.get("minimize_toggle",False)
        self.PAGE_SCROLL = _action_.get("scroll",False)
        
        #time
        self.DEFAULT_WAIT = _action_.get("default_wait",DEFAULT_SELENIUM_WAIT)
        self.TIMEOUT = _action_.get("timeout", DEFAULT_SELENIUM_TIMEOUT)
        self.WAIT_UNTIL = _action_.get("wait_until")
        self.WAIT_BY = _action_.get("wait_by", self.BY) #dependent
        self.WAIT_VALUE = _action_.get("wait_value", self.VALUE) #dependent
        self.WAIT_TIMEOUT = _action_.get("wait_timeout", self.TIMEOUT)
            
        #name 
        self.table_name = _action_.get("table_name","table")
        self.html_name = _action_.get('html_name', 'html')
        self.screenshot_name = _action_.get('screenshot_name', 'screenshot')
        self.pdf_name = _action_.get('pdf_name', 'web_pdf')
      
        self.LOG_MESSAGE = _action_.get("log_message", "NA")
        
        self.CLEAN_TABLE = _action_.get("clean_table",True)
        self.REQUIRE_TABLE_TITLE = _action_.get("require_title",True)
        
        
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

    def execute_blocks(self):
        block_data = []
        generic_actions = self.GENERIC_ACTIONS

        block = self.PARAMS["blocks"]
        self.LOGGER.info(f"Total Action(s) {len(block)}")

        for _, _action_ in enumerate(block):
            
            excecution_data = None
            perform_action  = None
            
            #================== FETCH ACTION SECTION ==================
            
            if isinstance(_action_, str): #derived from generic_actions config
                
                if _action_ =="QUARANTINE": #early exit if block quarantined
                    self.LOGGER.warning(f"BLOCK IS QUARANTINED.")
                    block_data.append(self._generate_packet({
                        "error_type": "quarantine",
                        "error_message": "Block Quarantined.",
                        "error_from": "ActionExecutor.execute_blocks"
                    }))
                    return block_data
                
                action_key, *content = _action_.split("||") #action_type, action_config
                
                if action_key not in generic_actions:
                    self.LOGGER.warning(f"{action_key} not part of generic_action_keys. Skipping.")
                    continue

                perform_action = generic_actions[action_key].copy() #the real action fetched
                
                #TYPE OF GENERIC ACTIONS + UPDATE THE CONFIG BAED ON INSTRUCTIONS
                # note: write dummy action later
                
                if action_key == "action_website":
                    
                    perform_action.update({"url": content[0]})
                    
                elif action_key == "action_download":
                    
                    by, value, multiple, wait_until,minimize_toggle,scroll = content
                    perform_action.update({
                        "by": by, 
                        "value": value, 
                        "multiple": bool(multiple), 
                        "wait_until": wait_until, 
                        "minimize_toggle":bool(minimize_toggle), 
                        "scroll":bool(scroll) 
                    })
                    
                #note: rest actions dont need user specific config and hence are just
                #"action_screenshot", "action_pdf","action_table"

            elif isinstance(_action_, dict):
                
                perform_action = _action_ # normal key value pair action in config

            else:
                self.LOGGER.warning(f"Unsupported action format: {_action_}")
                continue
            
            
            #================== FETCH DATAS SECTION ==================
            
            self._set_website_parameters(perform_action) #action specific config setter
            
            excecution_data = self.execute(perform_action) # execute the actions
            if excecution_data:
                block_data.append(excecution_data)

        return block_data

