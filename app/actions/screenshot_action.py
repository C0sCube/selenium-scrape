# app/actions/screenshot_action.py
import base64
from app.logger import get_global_logger
from app.utils import Helper
from app.actions.helper import ActionHelper

def genSst(executor):
    """Capture full-page screenshot via Chrome DevTools Protocol."""
    logger = get_global_logger()
    try:
        driver = executor.driver
        result = driver.execute_cdp_cmd("Page.captureScreenshot", {
            "captureBeyondViewport": True,
            "fromSurface": True
        })

        encoded_data = result.get("data", "")
        if not encoded_data:
            logger.warning("No screenshot data returned.")
            return []

        if executor.FILE_SAVE:
            file_path = Helper.create_path(
                executor.OUTPUT_PATH,
                f"{executor.pdf_name}-{Helper.generate_uid()}.png"
            )
            Helper.write_binary_file(file_path, base64.b64decode(encoded_data))
            logger.save(f"Saved screenshot to {file_path}")

        return [ActionHelper.generate_resp_packet(
            name=executor.pdf_name,
            header="",
            value=encoded_data,
            type="screenshot"
        )]

    except Exception as e:
        logger.warning(f"Screenshot failed: {type(e).__name__} - {e}")
        return []
