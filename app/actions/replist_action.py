from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from app.logger import get_global_logger


def repList(executor):
    logger = get_global_logger()
    driver = executor.driver
    follow_ups = executor.FOLLOW_UP_ACTIONS or []
    scrape_content = []

    logger.info(
        f"RepList (Generic) Using BY={executor.BY} and VALUE={executor.VALUE}"
    )

    base_url = driver.current_url

    # --------------------------------------------------
    # STEP 1 — Snapshot navigation commands
    # --------------------------------------------------
    elements = driver.find_elements(executor.BY, executor.VALUE)
    logger.info(f"Found {len(elements)} elements")

    targets = []

    for idx, elem in enumerate(elements):
        try:
            parent_a = elem.find_element(By.XPATH, "ancestor::a")
        except:
            parent_a = elem

        href = parent_a.get_attribute("href")
        onclick = parent_a.get_attribute("onclick")

        if href:
            href = href.strip()

            if href.startswith("javascript:"):
                targets.append({
                    "type": "js",
                    "value": href.replace("javascript:", "")
                })
            else:
                targets.append({
                    "type": "url",
                    "value": href
                })

        elif onclick:
            targets.append({
                "type": "js",
                "value": onclick
            })

        else:
            targets.append({
                "type": "click"
            })

    logger.info(f"Captured {len(targets)} navigation targets")

    # --------------------------------------------------
    # STEP 2 — Execute each navigation
    # --------------------------------------------------
    for idx, target in enumerate(targets):

        try:
            logger.notice(f"Processing Item {idx}")

            old_dom = driver.page_source

            if target["type"] == "url":
                driver.get(target["value"])

            elif target["type"] == "js":
                driver.execute_script(target["value"])

            elif target["type"] == "click":
                elements = driver.find_elements(executor.BY, executor.VALUE)
                if idx < len(elements):
                    driver.execute_script("arguments[0].click();", elements[idx])

            # Wait for page change
            WebDriverWait(driver, executor.TIMEOUT).until(
                lambda d: d.page_source != old_dom
            )

            # Follow-up steps
            for step in follow_ups:
                result = executor.execute(step)
                if not result:
                    continue

                for packet in result.get("response", []):
                    packet["tabname"] = f"Item_{idx}"
                    scrape_content.append(packet)

        except Exception as e:
            logger.error(f"Failed processing index {idx}: {e}")

        finally:
            # Return to base cleanly
            try:
                driver.get(base_url)
                WebDriverWait(driver, executor.TIMEOUT).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except:
                logger.warning(f"Failed returning to base after index {idx}")

    executor.ACTION_TYPE = "replist"
    return scrape_content
