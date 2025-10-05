from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto("https://canarabank.bank.in/pages/deposit-interest-rates", wait_until="networkidle")

    # Get the full HTML after the page is stable
    html = page.content()
    browser.close()

soup = BeautifulSoup(html, "html.parser")
tables = soup.find_all("table")

print(f"Found {len(tables)} tables")

for i, table in enumerate(tables, start=1):
    print(f"\n--- Table {i} ---")
    for row in table.find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        print(cells)


