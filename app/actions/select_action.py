from app.logger import get_global_logger
from app.actions.helper import ActionHelper
import itertools

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select



def selectList(executor):
    logger = get_global_logger()
    driver = executor.driver
    follow_ups = executor.FOLLOW_UP_ACTIONS or []
    select_params = executor.SELECT_PARAMS
    
    logger.info(f"SelectList via BY={executor.BY} and VALUE={executor.VALUE}")
    select_elements = driver.find_elements(executor.BY, executor.VALUE)
    logger.info(f"Total tab elements found: {len(select_elements)}")
    
    options = select_params.get("options",[])
    
    #build query
    select_query = {}
    for idx, param,select_item in enumerate(zip(options,select_elements)):
        
        _by,_value,_type,_opts = param.split("|")
        load_select = None
        
        
        
        if _by == "def" and _value == "def":
            load_select = Select(select_item)
        else:
            load_select = driver.find_element(_by,_value)
            load_select = Select(load_select)

        
        if _type == "value":
            select_query[idx] = _opts
        
        if _type == "option":
            select_query[idx] = list(load_select.options)
        
        if _type == "default":
            select_query[idx] = load_select.first_selected_option
        
    
        select_values = [select_query[i] for i in sorted(select_query.keys())]
        all_combinations = list(itertools.product(*select_values))
        

        
        