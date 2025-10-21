from app.logger import get_global_logger

def webRedir(executor):
    """Navigate to the specified URL in executor.URL."""
    logger = get_global_logger()
    try:
        logger.info(f"Redirecting to website {executor.URL}")
        executor.driver.get(executor.URL)
    except Exception as e:
        logger.error(f"Unable to redirect: {type(e).__name__} - {e}")
    return []
