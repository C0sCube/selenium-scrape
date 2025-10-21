# app/actions/weblist_action.py
import time
from selenium.webdriver.support.ui import WebDriverWait
from app.logger import get_global_logger
from app.actions.helper import ActionHelper

def webList(executor):
    """
    Visit multiple web links sequentially, run follow-up scraping steps for each,
    and attach web_link_headers if available.
    """
    logger = get_global_logger()
    driver = executor.driver
    follow_ups = executor.FOLLOW_UP_ACTIONS or []
    tablist_log = executor.LOG_MESSAGE
    scrape_content = []

    weblinks = []
    if isinstance(executor.WEBLINKS, list):
        weblinks = executor.WEBLINKS
    elif isinstance(executor.WEBLINKS, dict):
        base_url = executor.WEBLINKS.get("base_url")
        params = executor.WEBLINKS.get("params", {})
        weblinks = ActionHelper.build_multiple_urls(base_url, params)
    else:
        logger.error("No valid 'WEBLINKS' found for weblist action.")
        return scrape_content

    headers = []
    if executor.WEBLINKS_HEADER:
        headers = executor.WEBLINKS_HEADER.split("||")

    logger.info(f"Performing weblist action on {len(weblinks)} website(s).")

    for idx, url in enumerate(weblinks):
        try:
            driver.get(url)
            logger.notice(f"Navigating to → {url}")

            for step in follow_ups:
                if step.get("wait_until"):
                    condition = executor._ActionExecutor__get_condition(
                        step["wait_until"], step["by"], step["value"]
                    )
                    WebDriverWait(driver, step["timeout"]).until(condition)
                result = executor.execute(step)
                if not result:
                    logger.warning(f"No result for step {step['action']} on url {url}")
                    continue

                step_content = result.get("response", [])
                # Attach web_link_header
                if headers and idx < len(headers):
                    web_link_header = headers[idx]
                    for item in step_content:
                        if item.get("data_present"):
                            titles = item.get("title", [])
                            if isinstance(titles, str):
                                titles = [titles]
                            titles.append(web_link_header)
                            item["title"] = titles

                scrape_content.extend(step_content)

        except Exception as e:
            logger.warning(f"Failed to process URL [{idx}]={url}: {e}")

    executor.ACTION_TYPE = "weblist"
    executor.LOG_MESSAGE = tablist_log
    executor.FOLLOW_UP_ACTIONS = follow_ups
    return scrape_content
