# app/actions/click_save_action.py
import os, time, base64
from selenium.webdriver import ActionChains
from app.logger import get_global_logger
from app.actions.helper import ActionHelper

def clickSave(executor):
    """
    Clicks an element and captures any resulting file download
    (e.g., PDF opened in new tab or direct download).
    """

    logger = get_global_logger()
    driver = executor.driver
    element = executor.ELEMENT
    output_path = executor.OUTPUT_PATH
    pdf_name = executor.pdf_name
    timeout = executor.TIMEOUT

    logger.info("Starting click-save action...")

    scrape_content = []

    try:
        # --- Step 1: prepare folder snapshot ---
        initial_files = set(os.listdir(output_path))
        logger.debug(f"Initial files in folder: {initial_files}")

        # --- Step 2: scroll and click element ---
        driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'center'});", element)
        time.sleep(0.5)

        initial_tabs = driver.window_handles
        logger.info("Performing click (expecting possible new tab)...")

        try:
            ActionChains(driver).move_to_element(element).pause(0.1).click().perform()
        except Exception as e:
            logger.warning(f"ActionChains click failed ({e}); using JS click fallback.")
            driver.execute_script("arguments[0].click();", element)

        time.sleep(1)

        # --- Step 3: detect new tab ---
        new_tabs = driver.window_handles
        if len(new_tabs) > len(initial_tabs):
            new_tab = list(set(new_tabs) - set(initial_tabs))[0]
            driver.switch_to.window(new_tab)
            logger.notice(f"Switched to new tab: {driver.current_url}")
        else:
            pdf_url = element.get_attribute("href")
            if pdf_url:
                logger.warning("No new tab opened — navigating directly to href instead.")
                driver.get(pdf_url)
                logger.notice(f"Navigated directly to: {pdf_url}")
            else:
                logger.warning("No href found on clicked element.")

        current_url = driver.current_url
        if current_url.lower().endswith(".pdf"):
            logger.notice(f"Detected direct PDF URL: {current_url}")
            driver.get(current_url)

        # --- Step 4: wait for download completion ---
        file_path, ext = ActionHelper._wait_for_download(
            output_path,
            initial_files,
            timeout=timeout
        )

        if file_path:
            with open(file_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")

            scrape_content.append(
                ActionHelper.generate_resp_packet(
                    name=f"{pdf_name}",
                    header=os.path.basename(file_path),
                    value=encoded,
                    type=ext,
                )
            )
            logger.save(f"Saved downloaded file: {os.path.basename(file_path)}")
        else:
            logger.warning("No valid downloaded file detected in timeout.")

    except Exception as e:
        logger.error(f"click_save_action failed: {type(e).__name__} - {e}")

    return scrape_content
