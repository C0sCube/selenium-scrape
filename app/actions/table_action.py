# app/actions/table_action.py
from app.logger import get_global_logger
from app.actions.helper import ActionHelper
from app.utils import Helper
from selenium.webdriver.common.by import By


def tablScrape(executor):
    """
    Scrape one or more <table> elements from the current page.
    Automatically detects preceding headers and optionally cleans HTML.
    """
    logger = get_global_logger()
    driver = executor.driver

    logger.info(f"Scraping tables Using BY={executor.BY} and VALUE={executor.VALUE}")

    # --- find elements ---
    try:
        if executor.MULTIPLE:
            elements = driver.find_elements(executor.BY, executor.VALUE)
        else:
            elements = [driver.find_element(executor.BY, executor.VALUE)]
    except Exception as e:
        logger.error(f"Failed to locate table elements: {e}")
        return []

    # --- sanity filter ---
    if executor.BY == By.CSS_SELECTOR:
        elements = [elem for elem in elements if elem.tag_name.lower() == "table"]

    logger.info(f"Total tables found: {len(elements)}")

    scrape_content = []

    for idx, elem in enumerate(elements):
        try:
            # --- find header(s) above table ---
            header_texts = ActionHelper._find_preceding_texts(elem)
            logger.info(f"Table {idx} header: {header_texts}")

            # --- extract HTML ---
            raw_html = elem.get_attribute("outerHTML")

            # --- clean HTML if configured ---
            if executor.CLEAN_TABLE:
                final_html = ActionHelper._clean_raw_table_html(raw_html)
            else:
                final_html = raw_html

            # --- form response packet ---
            scrape_content.append(
                ActionHelper.generate_resp_packet(
                    name=f"{executor.table_name}_{idx}",
                    header=header_texts,
                    value=final_html,
                    type="table_html",
                )
            )

        except Exception as e:
            logger.warning(f"Error scraping table[{idx}]: {type(e).__name__} - {e}")

    if not scrape_content:
        logger.warning("No valid table content extracted.")
    return scrape_content
