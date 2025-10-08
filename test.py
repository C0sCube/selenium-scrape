import os
import re
import time
import base64
import pandas as pd
from playwright.sync_api import sync_playwright

# ---------- CONFIG ----------
BASE_URL = "https://www.iciciprulife.com/fund-performance/all-products-fund-performance-details.html"
PDF_FOLDER = "INSR_PDF"
XLS_FILE = "INSURANCE_MASTER_XLS.xlsx"
BATCH_SIZE = 20
COOLDOWN = 30

os.makedirs(PDF_FOLDER, exist_ok=True)


# ---------- HELPERS ----------
def clean_text(text: str):
    text = text.strip()
    text = re.sub(r'[\\/*?:"<>|/]', "_", text.lower())
    return text

def save_pdf_from_page(page, filename: str):
    try:
        pdf_bytes = page.pdf(format="A4", print_background=True)
        path = os.path.join(PDF_FOLDER, f"{filename}.pdf")
        with open(path, "wb") as f:
            f.write(pdf_bytes)
        print(f"Saved PDF: {path}")
    except Exception as e:
        print(f"Failed to save PDF for {filename}: {e}")

def extract_tables_to_excel(page, content: dict, writer):
    try:
        html = page.content()
        dfs = pd.read_html(html)
        for df in dfs:
            df.insert(0, "url", content["url"])
            df.insert(1, "ins", content["name"])
            df.insert(2, "sfin", content["sfin"])
            sheet = content["name"][:31]
            df.to_excel(writer, sheet_name=sheet, index=False)
        print(f"Saved Excel sheet: {content['name']}")
    except Exception as e:
        print(f"Failed to save tables for {content['name']}: {e}")

# ---------- MAIN SCRAPER ----------
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    print("Loading main page...")
    page.goto(BASE_URL, wait_until="networkidle")
    page.wait_for_function("document.querySelectorAll('#filterlist li').length > 0")

    # Click last filter item
    page.locator("#filterlist li:last-child").scroll_into_view_if_needed()
    page.click("#filterlist li:last-child", force=True)
    page.wait_for_timeout(2000)

    fund_links = page.locator("#fund-perform-table a")
    total_links = fund_links.count()
    print(f"Total links found: {total_links}")

    excelWriter = pd.ExcelWriter(XLS_FILE, engine="xlsxwriter")

    for batch_start in range(0, total_links, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_links)
        print(f"\nProcessing batch {batch_start // BATCH_SIZE + 1} ({batch_end - batch_start} links)")

        for i in range(batch_start, batch_end):
            link = fund_links.nth(i)
            href = link.get_attribute("href")

            if not href:
                continue

            try:
                page2 = context.new_page()
                page2.goto(href, wait_until="networkidle")

                # Try clicking "Show More"
                try:
                    page2.click("#showMore1", timeout=3000)
                except:
                    pass

                # Wait for companies table
                page2.wait_for_selector("#companies-table", timeout=10000)

                # Modify style for readable PDF
                page2.add_style_tag(content="""
                    html, body { background:white !important; color:#111 !important; }
                    * { color:#111 !important; background:white !important; border-color:#111 !important; }
                """)

                header_text = page2.locator("h1").inner_text()
                if "\n" in header_text:
                    file_name, sfin = header_text.split("\n", 1)
                else:
                    file_name, sfin = header_text, "N/A"

                content = {
                    "url": page2.url,
                    "name": clean_text(file_name),
                    "sfin": sfin.strip()
                }

                print(f"[{i+1}] Processing: {content['name']}")

                # Save PDF
                save_pdf_from_page(page2, file_name)

                # Save tables
                extract_tables_to_excel(page2, content, excelWriter)

            except Exception as e:
                print(f"[{i+1}] Failed: {e}")
            finally:
                page2.close()

        if batch_end < total_links:
            print(f"Cooling down for {COOLDOWN} seconds...")
            time.sleep(COOLDOWN)

    excelWriter.close()
    browser.close()

print("\nAll tasks completed successfully.")
