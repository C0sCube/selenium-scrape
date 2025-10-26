# app/actions/http_action.py
import os
import base64
import requests
from urllib.parse import urlparse
from app.utils import Helper
from app.logger import get_global_logger
from app.constants import MAX_REQUEST_BYTE_SIZE
from app.actions.helper import ActionHelper


def httpRequest(executor):
    """
    Perform a direct HTTP GET download (e.g., PDF, CSV, XLSX, DAT).
    Saves file optionally and returns base64-encoded packet.
    """
    logger = get_global_logger()
    file_url = executor.URL
    file_type = executor.export_format or "dat"
    output_dir = Helper.create_dir(executor.OUTPUT_PATH)

    logger.info(f"Starting HTTP download: {file_url}")

    try:
        file_name, encoded_data = download_file(
            executor, file_url, output_dir, 0, file_type
        )

        if not encoded_data:
            logger.warning(f"No content returned for {file_url}")
            return []

        return [
            ActionHelper.generate_resp_packet(
                name=executor.pdf_name,
                header=file_name,
                value=encoded_data,
                type=file_type,
            )
        ]

    except Exception as e:
        logger.error(f"HTTP download failed: {type(e).__name__} - {e}")
        return []

# ----------------------------
# Internal helper
# ----------------------------
def download_file(executor, file_url, output_dir, idx, extension):
    """
    Helper for downloading and optionally saving a file.
    Returns (file_name, encoded_data)
    """
    logger = get_global_logger()
    driver = executor.driver

    try:
        cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
        response = requests.get(file_url, cookies=cookies, verify=False, timeout=40)
        logger.notice(f"[{idx}] GET {extension.upper()} → Status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Failed to fetch {file_url} (Status {response.status_code})")
            return "", ""

    except Exception as e:
        logger.error(f"Request failed for {file_url}: {type(e).__name__} - {e}")
        return "", ""

    file_data = response.content
    file_size = len(file_data)
    logger.info(f"Received {file_size} bytes from {file_url}")

    if file_size >= MAX_REQUEST_BYTE_SIZE:
        logger.warning(f"File too large ({file_size} bytes) — skipped.")
        return "", ""

    # --- determine file name ---
    parsed_url = urlparse(file_url)
    raw_filename = os.path.basename(parsed_url.path)
    safe_filename = Helper.sanitize_Win_filename(raw_filename) or f"file_{idx}.{extension}"
    file_path = os.path.join(output_dir, safe_filename)

    if executor.FILE_SAVE:
        try:
            Helper.write_binary_file(file_path, file_data)
            logger.save(f"Saved {extension.upper()} → {file_path}")
        except Exception as e:
            logger.warning(f"Failed to save file {file_path}: {e}")

    encoded_data = base64.b64encode(file_data).decode("utf-8")
    return safe_filename, encoded_data





# def download_file(executor, file_url, output_dir, idx, extension):
#     """
#     Helper for downloading and optionally saving a file.
#     Returns (file_name, encoded_data)
#     """
#     logger = get_global_logger()
#     driver = executor.driver

#     try:
#         # Extract cookies + browser fingerprint from Selenium
#         cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
#         user_agent = driver.execute_script("return navigator.userAgent;")
#         referer = executor.PARAMS.get("base_url", file_url)

#         headers = {
#             "User-Agent": user_agent,
#             "Accept": "text/html,application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#             "Accept-Encoding": "gzip, deflate, br",
#             "Accept-Language": "en-US,en;q=0.9",
#             "Referer": referer,
#             "Connection": "keep-alive",
#             "Upgrade-Insecure-Requests": "1",
#         }

#         session = requests.Session()
#         response = session.get(file_url, headers=headers, cookies=cookies, timeout=60, verify=False)
#         logger.notice(f"[{idx}] GET {extension.upper()} → Status: {response.status_code}")

#         if response.status_code != 200:
#             logger.error(f"Failed to fetch {file_url} (Status {response.status_code})")
#             return "", ""

#     except Exception as e:
#         logger.error(f"Request failed for {file_url}: {type(e).__name__} - {e}")
#         return "", ""

#     file_data = response.content
#     file_size = len(file_data)
#     logger.info(f"Received {file_size} bytes from {file_url}")

#     if file_size >= MAX_REQUEST_BYTE_SIZE:
#         logger.warning(f"File too large ({file_size} bytes) — skipped.")
#         return "", ""

#     # --- determine file name ---
#     from urllib.parse import urlparse
#     parsed_url = urlparse(file_url)
#     raw_filename = os.path.basename(parsed_url.path)
#     safe_filename = Helper.sanitize_Win_filename(raw_filename) or f"file_{idx}.{extension}"
#     file_path = os.path.join(output_dir, safe_filename)

#     if executor.FILE_SAVE:
#         try:
#             Helper.write_binary_file(file_path, file_data)
#             logger.save(f"Saved {extension.upper()} → {file_path}")
#         except Exception as e:
#             logger.warning(f"Failed to save file {file_path}: {e}")

#     encoded_data = base64.b64encode(file_data).decode("utf-8")
#     return safe_filename, encoded_data
