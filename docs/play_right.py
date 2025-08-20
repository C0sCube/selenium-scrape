from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch_persistent_context(
        user_data_dir=r"C:\SeleniumProfiles\Profile17",
        channel="chrome",
        headless=False,
        args=["--profile-directory=Profile 17"]
    )
    page = browser.new_page()
    page.goto("https://www.cogencis.com/")
    print(page.title())
    browser.close()
