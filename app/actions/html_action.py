from app.logger import get_global_logger
from app.actions.helper import ActionHelper

def htmlScrape(executor):
    logger = get_global_logger()
    driver = executor.driver
    elements = driver.find_elements(executor.BY, executor.VALUE) if executor.MULTIPLE else [driver.find_element(executor.BY, executor.VALUE)]

    scrape_content = []
    for idx, elem in enumerate(elements):
        html = elem.get_attribute("outerHTML")
        if not html:
            logger.warning(f"No HTML found for {executor.VALUE}")
            continue
        scrape_content.append(ActionHelper.generate_resp_packet(
            name=f"{executor.html_name}_{idx}",
            header="HTML Content",
            value=html,
            type="html"
        ))
    return scrape_content
