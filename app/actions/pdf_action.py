import base64, time
from app.utils import Helper
from app.logger import get_global_logger
from app.actions.helper import ActionHelper

def genPdf(executor):
    """Prints current page to PDF (used for redir_pdf or pdf actions)."""
    logger = get_global_logger()
    try:
        driver = executor.driver
        if executor.ACTION_TYPE == "redir_pdf":
            driver.switch_to.window(driver.window_handles[-1])
            driver.maximize_window()
            driver.execute_script("document.body.style.zoom='100%'")
            time.sleep(2)

        executor._ActionExecutor__scroll_to_bottom()

        result = driver.execute_cdp_cmd("Page.printToPDF", {
            "printBackground": executor.PRINT_BACKGROUND,
        })
        encoded_data = result.get("data", "")
        if executor.FILE_SAVE:
            file_path = Helper.create_path(
                executor.OUTPUT_PATH,
                f"{executor.pdf_name}-{Helper.generate_uid()}.pdf"
            )
            Helper.write_binary_file(file_path, base64.b64decode(encoded_data))
            logger.save(f"Saved printed PDF to {file_path}")
        return [ActionHelper.generate_resp_packet(
            name=executor.pdf_name,
            header="",
            value=encoded_data,
            type="pdf"
        )]
    except Exception as e:
        logger.warning(f"Failed to print page to PDF: {e}")
        return []
