from app.logger import get_global_logger
import time

def clickElem(executor):
    """Clicks an element already stored in executor.ELEMENT."""
    logger = get_global_logger()
    try:
        executor.ELEMENT.click()
        logger.info("Clicked element successfully.")
        time.sleep(1.5)
    except Exception as e:
        logger.error(f"Failed to click element: {type(e).__name__} - {e}")
    return []
