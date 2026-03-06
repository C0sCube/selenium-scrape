# app/actions/text_action.py
from app.logger import get_global_logger
from selenium.common.exceptions import NoSuchElementException #type: ignore
from app.actions.helper import ActionHelper
import pandas as pd

def textScrape(executor):

    logger = get_global_logger()
    driver = executor.driver
    by = executor.BY
    value = executor.VALUE
    fields = executor.SCRAPE_FIELDS
    scrape_content = [["instance","field","value"]]

    
    for key, fetch_data in fields.items():
        
        by,value = fetch_data.split("||")
        
        by = executor.locator_map.get(by,"css")
        logger.info(f"Fetching via: {by} and value:{value}")

        elements = driver.find_elements(by, value) if executor.MULTIPLE else [driver.find_element(by, value)]

        for idx, element in enumerate(elements):
            txt = element.get_attribute("textContent")
            
            scrape_content.append([idx,key,txt])
            # scrape_content.append(
            #     ActionHelper.generate_resp_packet(
            #         name=f"text_{executor.html_name}",
            #         header=f"{driver.current_url}",
            #         value=txt,
            #         type="text",
            #     )
            # )

    df = pd.DataFrame(scrape_content[1:], columns=scrape_content[0])
    html_string = df.to_html(index = False)
    final_content = [
        ActionHelper.generate_resp_packet(
            name=f"text_{executor.html_name}",
            header=f"{driver.current_url}",
            value=html_string,
            type="table_html",
        )  
    ]
    
    return final_content


# for elem in elements:
#     data = {}

#     # --- if specific fields to extract ---
#     if fields:
#         for key, sub_selector in fields.items():
#             try:
#                 # check if format like "selector|||BY"
#                 if "|||" in sub_selector:
#                     sub_selector, sub_by = sub_selector.split("|||")
#                 else:
#                     sub_by = "css"

#                 sub_by = executor._ActionExecutor__get_by(sub_by)
#                 sub_elem = elem.find_element(sub_by, sub_selector)

#                 # text extraction fallback chain
#                 text_value = sub_elem.text.strip()
#                 if not text_value:
#                     text_value = sub_elem.get_attribute("textContent") or ""
#                 if not text_value:
#                     text_value = sub_elem.get_attribute("innerHTML") or ""

#                 data[key] = text_value.strip()

#             except NoSuchElementException:
#                 logger.warning(f"Field '{key}' not found under element.")
#                 data[key] = None
#             except Exception as e:
#                 logger.warning(f"Error scraping field '{key}': {type(e).__name__} - {e}")
#                 data[key] = None

#     # --- if attribute scraping ---
#     elif attr:
#         try:
#             val = elem.get_attribute(attr)
#             logger.info(f"Scraped attribute {attr}: {val}")
#             data[attr] = val
#         except Exception as e:
#             logger.warning(f"Failed to scrape attribute {attr}: {e}")
#             data[attr] = None

#     # --- else: plain text scraping ---
#     else:
#         text_value = elem.text.strip()
#         if not text_value:
#             text_value = elem.get_attribute("textContent") or ""
#         data["text"] = text_value.strip()