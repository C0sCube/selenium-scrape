# app/actions/manual_action.py
import os, time, base64
from app.logger import get_global_logger
from app.actions.helper import ActionHelper
from app.constants import MAX_DOWNLOAD_TIMEOUT, MAX_DOWNLOAD_WAIT


def manualAction(executor):
    """
    Waits for the user to manually download a file (PDF, XLSX, etc.)
    and then encodes that file as base64 for storage.
    """

    logger = get_global_logger()
    driver = executor.driver
    output_path = executor.OUTPUT_PATH
    pdf_name = executor.pdf_name

    logger.info("Waiting for manual file download...")
    scrape_content = []

    try:
        # Record initial folder state
        initial_files = set(os.listdir(output_path))
        logger.debug(f"Initial files: {initial_files}")

        # Wait for new download (using helper)
        file_path, ext = ActionHelper._wait_for_download(
            output_path,
            initial_files,
            timeout=MAX_DOWNLOAD_TIMEOUT
        )
        time.sleep(MAX_DOWNLOAD_WAIT)

        if file_path:
            with open(file_path, "rb") as f:
                encoded_data = base64.b64encode(f.read()).decode("utf-8")

            logger.save(f"Detected manual download: {os.path.basename(file_path)}")

            scrape_content.append(
                ActionHelper.generate_resp_packet(
                    name=f"{pdf_name}",
                    header=os.path.basename(file_path),
                    value=encoded_data,
                    type=ext
                )
            )
        else:
            logger.warning("No valid downloaded file found within timeout.")

    except Exception as e:
        logger.error(f"Manual action failed: {type(e).__name__} - {e}")

    return scrape_content
