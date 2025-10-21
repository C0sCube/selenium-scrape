# app/actions/text_action.py
from app.logger import get_global_logger
from selenium.common.exceptions import NoSuchElementException
from app.actions.helper import ActionHelper


def textScrape(executor):
    """
    Scrapes visible text or specified subfields from element(s) on the page.

    Supports:
    - direct text extraction
    - attribute scraping (via executor.ATTRIBUTE)
    - field mapping (via executor.SCRAPE_FIELDS)
    """

    logger = get_global_logger()
    driver = executor.driver
    by = executor.BY
    value = executor.VALUE
    fields = executor.SCRAPE_FIELDS
    attr = executor.ATTRIBUTE

    logger.info(f"Scraping text using BY={by}, VALUE={value}")

    try:
        elements = driver.find_elements(by, value)
    except Exception as e:
        logger.error(f"Failed to locate text elements: {type(e).__name__} - {e}")
        return []

    scrape_content = []

    for elem in elements:
        data = {}

        # --- if specific fields to extract ---
        if fields:
            for key, sub_selector in fields.items():
                try:
                    # check if format like "selector|||BY"
                    if "|||" in sub_selector:
                        sub_selector, sub_by = sub_selector.split("|||")
                    else:
                        sub_by = "css"

                    sub_by = executor._ActionExecutor__get_by(sub_by)
                    sub_elem = elem.find_element(sub_by, sub_selector)

                    # text extraction fallback chain
                    text_value = sub_elem.text.strip()
                    if not text_value:
                        text_value = sub_elem.get_attribute("textContent") or ""
                    if not text_value:
                        text_value = sub_elem.get_attribute("innerHTML") or ""

                    data[key] = text_value.strip()

                except NoSuchElementException:
                    logger.warning(f"Field '{key}' not found under element.")
                    data[key] = None
                except Exception as e:
                    logger.warning(f"Error scraping field '{key}': {type(e).__name__} - {e}")
                    data[key] = None

        # --- if attribute scraping ---
        elif attr:
            try:
                val = elem.get_attribute(attr)
                logger.info(f"Scraped attribute {attr}: {val}")
                data[attr] = val
            except Exception as e:
                logger.warning(f"Failed to scrape attribute {attr}: {e}")
                data[attr] = None

        # --- else: plain text scraping ---
        else:
            text_value = elem.text.strip()
            if not text_value:
                text_value = elem.get_attribute("textContent") or ""
            data["text"] = text_value.strip()

        scrape_content.append(
            ActionHelper.generate_resp_packet(
                name=f"text_{executor.html_name}",
                header=f"Scraped via {by}={value}",
                value=data,
                type="text",
            )
        )

    if not scrape_content:
        logger.warning("No text data extracted from page.")
    else:
        logger.info(f"Total text packets generated: {len(scrape_content)}")

    return scrape_content
