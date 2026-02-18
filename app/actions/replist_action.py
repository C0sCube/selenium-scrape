# app/actions/replist_action.py
import traceback
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver import ActionChains
from app.logger import get_global_logger
from selenium.common.exceptions import StaleElementReferenceException


def repList(executor):
    logger = get_global_logger()
    driver = executor.driver

    follow_ups = executor.FOLLOW_UP_ACTIONS or []
    tablist_log = executor.LOG_MESSAGE

    scrape_content = []
    tab_names = []

    logger.info(
        f"Tab List Loop Using BY={executor.BY} and VALUE={executor.VALUE}"
    )

    # Initial count only (elements WILL be re-fetched)
    total_tabs = len(driver.find_elements(executor.BY, executor.VALUE))
    logger.info(f"Total tab elements found: {total_tabs}")

    for idx in range(total_tabs):
        try:
            # 🔁 ALWAYS re-locate elements after postback
            rep_elements = driver.find_elements(executor.BY, executor.VALUE)
            tab = rep_elements[idx]

            tab_name = tab.text.strip() or f"Tab_{idx}"
            tab_names.append(tab_name)

            # logger.info(f"Processing tab [{idx}]={tab_name}")

            # Scroll + hover (important for ASP.NET)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", tab)
            ActionChains(driver).move_to_element(tab).perform()

            driver.execute_script("arguments[0].click();", tab)
            WebDriverWait(driver, 10).until( lambda d: d.execute_script("return document.readyState") == "complete")

            # -----------------------------
            # Execute follow-up actions
            # -----------------------------
            for step in follow_ups:
                if step.get("wait_until"):
                    condition = executor._ActionExecutor__get_condition(
                        step["wait_until"],
                        step["by"],
                        step["value"]
                    )
                    WebDriverWait(driver, step["timeout"]).until(condition)

                result = executor.execute(step)
                if not result:
                    logger.warning(
                        f"No result for step {step.get('action')} on tab {tab_name}"
                    )
                    continue

                step_content = result.get("response", [])
                for packet in step_content:
                    packet["tabname"] = tab_name
                    packet["title"].append(f"TabName: {tab_name}")

                scrape_content.extend(step_content)

            # ✅ Return to list page state (postback again)
            driver.back()

            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

        except StaleElementReferenceException:
            logger.warning(f"Stale element at tab index {idx}, retrying")
            continue

        except Exception as e:
            logger.error(f"Failed tab [{idx}]={tab_name}: {e}")
            logger.error(traceback.format_exc())

    # Reset executor state
    executor.TABS_FOUND = tab_names
    executor.ACTION_TYPE = "replist"
    executor.LOG_MESSAGE = tablist_log
    executor.FOLLOW_UP_ACTIONS = follow_ups

    return scrape_content