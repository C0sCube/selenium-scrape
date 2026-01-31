import os, time, base64
from app.logger import get_global_logger
from app.actions.helper import ActionHelper

def inputAction(executor):
    logger = get_global_logger()
    driver = executor.driver
    
    scrape_content = []
    try:
        
        value = input("Enter the data:")
        
        scrape_content.append(
            ActionHelper.generate_resp_packet(
                name=f"manualinput",
                value=value,
            )
        )
    except Exception as e:
        logger.error(f"Manual action failed: {type(e).__name__} - {e}")

    return scrape_content
    # pass