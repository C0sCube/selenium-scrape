# app/actions/download_action.py
import os
import base64
import requests
from urllib.parse import urljoin, urlparse
from selenium.webdriver.common.by import By
from app.utils import Helper
from app.logger import get_global_logger
from app.constants import MAX_REQUEST_BYTE_SIZE
from app.actions.helper import ActionHelper


def downloadElem(executor):
    """
    Download linked files (PDF, XLSX, DOCX, etc.) found by a given locator.
    Returns a list of encoded response packets.
    """
    driver = executor.driver
    logger = get_global_logger()

    elements = (
        driver.find_elements(executor.BY, executor.VALUE)
        if executor.MULTIPLE
        else [driver.find_element(executor.BY, executor.VALUE)]
    )
    logger.info(f"Found {len(elements)} element(s) for selector: {executor.BY}={executor.VALUE}")

    scrape_content = []

    for idx, elem in enumerate(elements):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elem)

            file_url = _extract_href(elem)
            if not file_url:
                logger.warning(f"No valid href found at index {idx}")
                continue

            full_url = urljoin(driver.current_url, file_url)
            file_type = ActionHelper._determine_file_type(full_url)
            logger.info(f"[{idx}] URL: {full_url} | Type: {file_type or 'unknown'}")

            if not file_type:
                logger.warning(f"Skipping unsupported file type: {full_url}")
                continue

            # --- Download file ---
            encoded_data = _download_file(executor, full_url, idx, file_type)
            if not encoded_data:
                logger.warning(f"No data returned for {full_url}")
                continue

            file_name = os.path.basename(urlparse(full_url).path)
            scrape_content.append(
                ActionHelper.generate_resp_packet(
                    name=f"{executor.pdf_name}_{idx}",
                    header=file_name,
                    value=encoded_data,
                    type=file_type,
                )
            )
            logger.save(f"Downloaded and cached: {file_name}")

        except Exception as e:
            logger.error(f"Error processing element [{idx}]: {type(e).__name__} - {e}")

    return scrape_content


# -------------------------------
# Internal helpers
# -------------------------------
def _extract_href(elem):
    """Return the href from an element or its nested <a> tag."""
    url = elem.get_attribute("href")
    if url:
        return url

    try:
        link_elem = elem.find_element(By.TAG_NAME, "a")
        return link_elem.get_attribute("href")
    except Exception:
        return None


def _download_file(executor, file_url, idx, extension):

    logger = get_global_logger()
    driver = executor.driver

    try:
        cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
        response = requests.get(file_url, cookies=cookies, verify=False, timeout=40)
        logger.notice(f"[{idx}] GET {extension.upper()} → Status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Failed to download ({response.status_code}) {file_url}")
            return ""

    except Exception as e:
        logger.error(f"Request failed for {file_url}: {type(e).__name__} - {e}")
        return ""


    parsed_url = urlparse(file_url)
    raw_filename = os.path.basename(parsed_url.path)
    safe_filename = Helper.sanitize_Win_filename(raw_filename) or f"file_{idx}.{extension}"
    file_path = os.path.join(executor.OUTPUT_PATH, safe_filename)

    file_data = response.content
    file_size = len(file_data)

    if file_size >= MAX_REQUEST_BYTE_SIZE:
        logger.warning(f"Skipped {safe_filename} — size {file_size} bytes exceeds limit.")
        return ""
    
    if executor.FILE_SAVE:
        try:
            Helper.write_binary_file(file_path, file_data)
            logger.save(f"Saved {extension.upper()} → {file_path}")
        except Exception as e:
            logger.warning(f"Failed to save file {file_path}: {e}")


    return base64.b64encode(file_data).decode("utf-8")
