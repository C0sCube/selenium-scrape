# app/actions/tablist_action.py
import time
from selenium.webdriver.support.ui import WebDriverWait
from app.logger import get_global_logger
from selenium.webdriver import ActionChains

def tabList(executor):
    """
    Loop through tabs or buttons, click each, 
    and perform follow-up scraping steps inside each tab.
    """
    logger = get_global_logger()
    driver = executor.driver
    follow_ups = executor.FOLLOW_UP_ACTIONS or []
    tablist_log = executor.LOG_MESSAGE

    logger.info(f"Tab List Loop Using BY={executor.BY} and VALUE={executor.VALUE}")
    tab_elements = driver.find_elements(executor.BY, executor.VALUE)
    logger.info(f"Total tab elements found: {len(tab_elements)}")

    scrape_content = []
    tab_names = []

    for idx, tab in enumerate(tab_elements):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab)
            ActionChains(driver).move_to_element(tab).perform()
            driver.execute_script("arguments[0].click();", tab)

            tab_name = tab.get_attribute("innerText").strip() or f"Tab_{idx}"
            tab_names.append(tab_name)
            logger.notice(f"Clicked Tab → {tab_name}")

            time.sleep(0.5)
            for step in follow_ups:
                if step.get("wait_until"):
                    condition = executor._ActionExecutor__get_condition(
                        step["wait_until"], step["by"], step["value"]
                    )
                    WebDriverWait(driver, step["timeout"]).until(condition)

                result = executor.execute(step)
                if not result:
                    logger.warning(f"No result returned for step {step['action']} on tab [{idx}]={tab_name}")
                    continue

                step_content = result.get("response", [])
                for packet in step_content:
                    packet["tabname"] = tab_name
                scrape_content.extend(step_content)

        except Exception as e:
            logger.warning(f"Failed tab [{idx}]={tab.get_attribute('innerText') or 'unknown'}: {e}")

    # reset values
    executor.TABS_FOUND = tab_names
    executor.ACTION_TYPE = "tablist"
    executor.LOG_MESSAGE = tablist_log
    executor.FOLLOW_UP_ACTIONS = follow_ups

    return scrape_content
