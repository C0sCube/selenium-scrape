# app/actions/__init__.py
from .html_action import htmlScrape
from .click_action import clickElem
from .website_action import webRedir
from .pdf_action import genPdf
from .screenshot_action import genSst
from .http_action import httpRequest
from .script_action import injectScript
from .tablist_action import tabList
from .weblist_action import webList
from .manual_action import manualAction
from .table_action import tablScrape
from .text_action import textScrape
from .clicksave_action import clickSave
from .download_action import downloadElem

__all__ = [
    "htmlScrape", "clickElem", "webRedir",
    "httpRequest", "tablScrape","textScrape", 
    "downloadElem","genPdf", "genSst",
    "injectScript","tabList","webList",
    "manualAction", "clickSave"
]
