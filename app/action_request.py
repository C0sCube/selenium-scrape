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



class ActionRequest:
    def __init__(self):
        self.logger = get_global_logger()
        self.today = datetime.now()
        self.OUTPUT_PATH = None
    
    pass