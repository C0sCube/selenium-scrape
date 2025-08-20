from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import re, os, time, logging, pprint, requests, base64, random

from app.operation_executor import *
from app.utils import *


class ActionExecutor:
    """
    Playwright-backed clone of your Selenium ActionExecutor.
    - Preserves public API so main.py can stay largely unchanged.
    - Exposes .driver.get(url) and .driver.quit() by aliasing to class methods,
      so existing calls like executor.driver.get(...) keep working.
    """

    def __init__(self, logger=None, params=None, paths=None, base_window=None):
        self.logger = logger or logging.getLogger(__name__)
        self.PARAMS = params or {}
        self.PATHS = paths or {}
        self.data = {}

        self.DATE = datetime.now()
        self.OUTPUT_PATH = Helper.create_dir(
            paths["output"], paths["folders"]["data"], self.DATE.strftime("%d%m%Y"), params["bank_name"]
        )

        # Playwright internals
        self._pw = None
        self.context = None
        self.page = None

        # Maintain a window/page stack compatible with your existing flow
        self.window_stack = [base_window]

        # Back-compat shim so main.py can still call executor.driver.get()/quit()
        # We simply point .driver to self, and implement get()/quit() methods below
        self.driver = self

    # ===================== DRIVER LIFECYCLE =====================
    def create_driver(self):
        """Launch a persistent Chrome context using the provided profile path/name.
        Mirrors your Selenium options (profile reuse, headers, etc.).
        """
        try:
            self._pw = sync_playwright().start()

            profile_dir = self.PATHS.get("profile_path")  # e.g. C:\\SeleniumProfiles
            profile_name = self.PATHS.get("profile_name")  # e.g. Profile17

            # Extra HTTP headers (sent with every request)
            extra_headers = self.PARAMS.get("headers", {}) or {}

            # Optional UA override: prefer dedicated Playwright param
            user_agent = None
            if extra_headers.get("User-Agent"):
                user_agent = extra_headers.get("User-Agent")
                # It's okay to leave it in extra_headers too; Playwright will prioritize the explicit user_agent param.

            args = [
                f"--profile-directory={profile_name}",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-default-apps",
                "--disable-infobars",
                "--disable-extensions",
                "--disable-popup-blocking",
            ]

            self.context = self._pw.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                channel="chrome",              # use actual Chrome
                headless=False,
                args=args,
                user_agent=user_agent,
                accept_downloads=True,
                extra_http_headers=extra_headers if extra_headers else None,
            )

            self.page = self.context.new_page()
            self.window_stack = [self.page]

            # Return self.driver for back-compat if someone expects a WebDriver-like object
            return self.driver

        except FileNotFoundError:
            raise FileNotFoundError(
                f"Profile path not found at {self.PATHS.get('profile_path')}. Please check the path."
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create Playwright context: {e}")

    def get(self, url: str):
        """Back-compat shim for main.py: executor.driver.get(url) -> page.goto(url)."""
        if not self.page:
            raise RuntimeError("Playwright page not initialized. Call create_driver() first.")
        self.logger.info(f"Navigating to {url}")
        self.page.goto(url, wait_until="domcontentloaded")

    def quit(self):
        """Back-compat shim for main.py: executor.driver.quit()"""
        try:
            if self.context:
                self.context.close()
        finally:
            if self._pw:
                self._pw.stop()

    # ===================== SELECTOR HELPERS =====================
    def _css_escape(self, s: str) -> str:
        # Minimal escape for IDs that may contain colon or spaces
        return re.sub(r"([^a-zA-Z0-9_-])", r"\\\\\\1", s)

    def _build_selector(self, by_string: str, value: str) -> str:
        by = (by_string or "css").lower()
        if by == "css":
            return value
        if by == "xpath":
            return f"xpath={value}"
        if by == "id":
            return f"#{self._css_escape(value)}"
        if by == "name":
            return f"[name=\"{value}\"]"
        if by == "class":
            # support multiple classes separated by spaces
            classes = ".".join([c for c in value.strip().split() if c])
            return f".{classes}"
        if by == "tag":
            return value
        # default to css
        return value

    def _locator(self, by_string: str, value: str):
        sel = self._build_selector(by_string, value)
        return self.page.locator(sel)

    def _wait_for(self, by_string: str, value: str, wait_type: str, timeout_sec: int):
        locator = self._locator(by_string, value)
        state_map = {
            "clickable": "visible",      # Playwright click waits for actionable state anyway
            "visible": "visible",
            "present": "attached",
            "invisible": "hidden",
            "attached": "attached",
        }
        state = state_map.get((wait_type or "").lower())
        if not state:
            raise ValueError(f"Unknown wait condition: {wait_type}")
        locator.wait_for(state=state, timeout=timeout_sec * 1000)
        return locator

    # ===================== CORE EXECUTION =====================
    def execute(self, _action_: dict):
        self.ACTION_TYPE = _action_.get("action", None)

        # basic
        self.BY_STRING = _action_.get("by", "css")
        self.VALUE = _action_.get("value")
        self.URL = _action_.get("url", "https://tinyurl.com/nothing-borgir")

        # timing
        self.DEFAULT_WAIT = _action_.get("wait", 2)
        self.TIMEOUT = _action_.get("timeout", 20)
        self.WAIT_UNTIL = _action_.get("wait_until")
        self.WAIT_BY_STRING = _action_.get("wait_by", self.BY_STRING)
        self.WAIT_VALUE = _action_.get("wait_value", self.VALUE)

        # names
        self.table_name = _action_.get("table_name", "table")
        self.html_name = _action_.get("html_name", "html")
        self.screenshot_name = _action_.get("screenshot_name", "screenshot")
        self.pdf_name = _action_.get("pdf_name", "webpage_pdf")
        self.export_format = _action_.get("export_format", None)
        self.LOG_MESSAGE = _action_.get("log_message", "Log Msg For Action Not Attached.")

        # fields
        self.ATTRIBUTE = _action_.get("attribute")
        self.SCRAPE_FIELDS = _action_.get("scrape_fields")

        # save flags
        self.CONSOLIDATE_SAVE = _action_.get("consolidate_save", False)
        self.MULTIPLE = _action_.get("multiple", False)

        # page pdf flags
        self.LANDSCAPE = _action_.get("landscape", False)
        self.PRINT_BACKGROUND = _action_.get("print_background", False)

        # window mgmt
        self.NEW_WINDOW = _action_.get("new_window", False)
        self.RETURN_TO_BASE = _action_.get("return_to_base", False)

        time.sleep(random.uniform(0, max(0, float(self.DEFAULT_WAIT))))

        # 1) Resolve target locator (and optional wait)
        locator = None
        try:
            self.logger.info(f"Performing _action_: {self.ACTION_TYPE} on {self.VALUE}")
            if self.WAIT_UNTIL:
                locator = self._wait_for(self.WAIT_BY_STRING, self.WAIT_VALUE, self.WAIT_UNTIL, self.TIMEOUT)
            elif self.VALUE:
                locator = self._locator(self.BY_STRING, self.VALUE)
            else:
                locator = None  # actions like 'website' may not need a locator
        except PWTimeoutError:
            self.logger.warning(f"Element wait timed out: {self.WAIT_BY_STRING}={self.WAIT_VALUE}. Skipping this action.")
            return self._generate_packet({"skip": "Element Wait Timed Out Hence Skipped."})
        except Exception as e:
            self.logger.warning(f"Element not found: {self.BY_STRING}={self.VALUE}. Skipping this action. ({e})")
            return self._generate_packet({"skip": "Element Not Found Hence Skipped."})

        # 2) Execute the action
        content = None
        try:
            if self.ACTION_TYPE == "click":
                self._action_click(locator)
            elif self.ACTION_TYPE == "html":
                content = self._action_html_scrape(locator)
            elif self.ACTION_TYPE == "table":
                content = self._action_table_scrape(locator)
            elif self.ACTION_TYPE == "scrape":
                content = self._action_text_scrape(locator)
            elif self.ACTION_TYPE == "website":
                self._action_redirect()
            elif self.ACTION_TYPE == "screenshot":
                self._action_screenshot()
            elif self.ACTION_TYPE == "pdf":
                self._action_page_pdf()
            elif not self.ACTION_TYPE:
                self.logger.info(f"Checked presence of element: {self.BY_STRING}={self.VALUE}")
        except Exception as e:
            self.logger.error(f"Error executing action '{self.ACTION_TYPE}': {e}", exc_info=True)
            return self._generate_packet({"error": str(e)})

        # 3) Window handling
        if self.RETURN_TO_BASE:
            self.logger.info("Returning to Base Window.")
            if len(self.window_stack) > 1:
                try:
                    top = self.window_stack.pop()
                    top.close()
                finally:
                    self.page = self.window_stack[-1]

        return self._generate_packet(content)

    # ===================== ACTIONS =====================
    def _action_text_scrape(self, locator) -> dict:
        self.logger.info(f"Scraping Using BY={self.BY_STRING} and VALUE={self.VALUE}")
        data_container = {}

        # Resolve target elements (one or many)
        items = []
        if locator is None:
            self.logger.warning("No locator provided for text scrape.")
            return {}
        count = locator.count()
        if self.MULTIPLE:
            items = [locator.nth(i) for i in range(count)]
        else:
            if count == 0:
                self.logger.warning("No elements found for text scrape.")
                return {}
            items = [locator.first]

        for elem in items:
            if self.SCRAPE_FIELDS:
                results = {}
                for key, sub_selector in self.SCRAPE_FIELDS.items():
                    try:
                        by = "css"
                        sel = sub_selector
                        if "|||" in sub_selector:
                            sel, by = sub_selector.split("|||", 1)
                        sub_loc = elem.locator(self._build_selector(by, sel))

                        text = None
                        try:
                            text = sub_loc.inner_text(timeout=self.TIMEOUT * 1000).strip()
                        except Exception:
                            # fallback to textContent / innerHTML
                            try:
                                text = (sub_loc.text_content(timeout=self.TIMEOUT * 1000) or "").strip()
                            except Exception:
                                text = (sub_loc.evaluate("el => el.innerHTML").strip() if sub_loc.count() else None)

                        results[key] = text
                    except Exception as e:
                        self.logger.warning(f"Missing field '{key}': {e}")
                        results[key] = None
                data_container.update(results)

            elif self.ATTRIBUTE:
                try:
                    val = elem.get_attribute(self.ATTRIBUTE, timeout=self.TIMEOUT * 1000)
                except Exception:
                    val = None
                self.logger.info(f"Scraped attribute {self.ATTRIBUTE}: {val}")
                data_container.update({self.ATTRIBUTE: val})

            else:
                try:
                    txt = elem.inner_text(timeout=self.TIMEOUT * 1000).strip()
                except Exception:
                    txt = (elem.text_content() or "").strip()
                data_container.update({"text": txt})

        self.logger.info(f"Scraped keys: {list(data_container.keys())}")
        pprint.pprint(data_container)
        return data_container

    def _action_table_scrape(self, locator) -> dict:
        self.logger.info(f"Scraping Using BY={self.BY_STRING} and VALUE={self.VALUE}")

        if locator is None:
            self.logger.warning("No locator provided for table scrape.")
            return {}

        # Collect target elements
        locators = []
        count = locator.count()
        if self.MULTIPLE:
            locators = [locator.nth(i) for i in range(count)]
        else:
            if count == 0:
                self.logger.warning("No elements found for table scrape.")
                return {}
            locators = [locator.first]

        # Filter to <table> tags if BY is css (mimic Selenium behavior)
        filtered = []
        for l in locators:
            try:
                tag = l.evaluate("el => el.tagName")
                if self.BY_STRING.lower() == "css":
                    if str(tag).lower() == "table":
                        filtered.append(l)
                else:
                    filtered.append(l)
            except Exception:
                filtered.append(l)

        cleaned_tables, table_scrape = [], {}
        for idx, elem in enumerate(filtered):
            try:
                raw_html = elem.evaluate("el => el.outerHTML")
            except Exception:
                raw_html = None
            if not raw_html:
                continue

            # Clean HTML similarly to your Selenium version
            raw_html = Helper.apply_sub(raw_html, r"<th\\b", "<td", ignore_case=True)
            raw_html = Helper.apply_sub(raw_html, r"</th\\b", "</td", ignore_case=True)
            raw_html = Helper.apply_sub(raw_html, r"</?(?:strong|sup|b|p|br)(?:\\s+[^>]*)?>", ignore_case=True)
            raw_html = Helper.apply_sub(raw_html, r"[#*@\n\t]+", ignore_case=True)
            raw_html = Helper._normalize_whitespace(raw_html)

            soup = BeautifulSoup(raw_html, "html.parser")
            ALLOWED = {"rowspan", "colspan"}
            for tag in soup.find_all(True):
                for attr in list(tag.attrs):
                    if attr not in ALLOWED:
                        del tag.attrs[attr]

            cleaned = str(soup)
            cleaned_tables.append(cleaned)
            table_scrape[f"table_{idx}"] = cleaned

        # Export
        if cleaned_tables:
            if self.export_format in ["excel", "both"]:
                output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["save_excel"])
                OperationExecutor.save_tables_to_excel(
                    cleaned_tables,
                    output_dir=output_dir,
                    output_file=f"{self.table_name}.xlsx",
                    consolidate_save=self.CONSOLIDATE_SAVE,
                )
                self._log_save(f"Saved {len(cleaned_tables)} table(s) to single Excel file: {output_dir}")

            if self.export_format in ["html", "both"]:
                output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["save_html"])
                OperationExecutor.save_tables_html(
                    cleaned_tables,
                    output_dir=output_dir,
                    output_file=f"{self.table_name}.html",
                    separator="<br><hr><br>" if self.CONSOLIDATE_SAVE else None,
                )
                self._log_save(f"Saved {len(cleaned_tables)} table(s) to HTML in: {output_dir}")

        return table_scrape

    def _action_html_scrape(self, locator) -> dict:
        self.logger.info(f"Scraping Using BY={self.BY_STRING} and VALUE={self.VALUE}")
        if locator is None:
            self.logger.warning("No locator provided for HTML scrape.")
            return {}

        # Resolve target elements
        items = []
        count = locator.count()
        if self.MULTIPLE:
            items = [locator.nth(i) for i in range(count)]
        else:
            if count == 0:
                self.logger.warning("No elements found for HTML scrape.")
                return {}
            items = [locator.first]

        cleaned_content, content_scrape = [], {}
        for idx, elem in enumerate(items):
            try:
                html_content = elem.evaluate("el => el.outerHTML")
            except Exception:
                html_content = None
            if not html_content:
                self.logger.warning(f"No HTML content found for element: {self.VALUE}")
                continue

            cleaned_content.append(html_content)
            content_scrape.update({f"{self.html_name}_{idx}": html_content})

        # Save HTML bundle
        output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["save_html"])
        OperationExecutor.save_tables_html(
            cleaned_content,
            output_dir=output_dir,
            output_file=f"{self.html_name}.html",
            separator="<br><hr><br>" if self.CONSOLIDATE_SAVE else None,
        )
        self._log_save(f"Saved {len(cleaned_content)} element(s) HTML in: {output_dir}")

        return content_scrape

    def _action_screenshot(self):
        path = Helper.create_path(self.OUTPUT_PATH, f"{self.screenshot_name}.png")
        self.page.screenshot(path=path, full_page=True)
        self._log_save(f"Saved screenshot: {path}")

    def _action_page_pdf(self):
        try:
            # Scroll to bottom to ensure lazy content loads (mimic your Selenium loop)
            last_height = self.page.evaluate("() => document.body.scrollHeight")
            while True:
                self.page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1.5)
                new_height = self.page.evaluate("() => document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # Playwright's PDF works on Chromium; may require headless in some versions
            file_path = Helper.create_path(self.OUTPUT_PATH, f"{self.pdf_name}.pdf")
            self.page.pdf(path=file_path, landscape=self.LANDSCAPE, print_background=self.PRINT_BACKGROUND)
            self.logger.info(f"Saved printed PDF to {file_path}")

        except Exception as e:
            self.logger.error(f"Failed to print page to PDF: {str(e)}")

    def _action_download(self):
        # NOTE: Not wired in the Playwright flow above; kept for parity if you add an 'action': 'download'
        locator = self._locator(self.BY_STRING, self.VALUE)
        if locator.count() == 0:
            self.logger.warning("Download element not found.")
            return

        # Try to click and intercept download via Playwright
        try:
            with self.page.expect_download(timeout=self.TIMEOUT * 1000) as dl_info:
                locator.first.click()
            download = dl_info.value

            output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["downloads"])
            suggested = download.suggested_filename
            file_path = os.path.join(output_dir, suggested)
            download.save_as(file_path)
            self.logger.info(f"Downloaded file to {file_path}")
            return
        except Exception:
            pass

        # Fallback: direct request using cookies (for PDFs etc.)
        try:
            href = locator.first.get_attribute("href")
        except Exception:
            href = None

        if href and href.lower().endswith(".pdf"):
            cookies = {c['name']: c['value'] for c in self.context.cookies()}
            r = requests.get(href, cookies=cookies)
            if r.status_code == 200:
                output_dir = Helper.create_dirs(self.OUTPUT_PATH, ["downloads"])
                file_path = os.path.join(output_dir, os.path.basename(href))
                with open(file_path, "wb") as f:
                    f.write(r.content)
                self.logger.info(f"Downloaded PDF to {file_path}")
            else:
                self.logger.error(f"Failed to download file: {href}")
        else:
            # As last resort, click
            locator.first.click()
            self.logger.info("Triggered click for file download.")

    def _action_redirect(self):
        try:
            self.logger.info(f"Redirecting to webpage {self.URL}")
            self.page.goto(self.URL, wait_until="domcontentloaded")
        except Exception as e:
            self.logger.error(f"Unable to redirect: {e}")

    def _action_click(self, locator):
        if locator is None:
            self.logger.warning("Click called without a locator; skipping.")
            return

        if self.NEW_WINDOW:
            try:
                with self.context.expect_page() as new_page_info:
                    locator.first.click(timeout=self.TIMEOUT * 1000)
                new_page = new_page_info.value
                self.page = new_page
                self.window_stack.append(new_page)
            except Exception as e:
                self.logger.error(f"Failed to handle NEW_WINDOW click: {e}")
                # Fallback: just click on current page
                locator.first.click(timeout=self.TIMEOUT * 1000)
        else:
            locator.first.click(timeout=self.TIMEOUT * 1000)
        self.logger.info(f"Clicked Element {self.VALUE}")

    # ===================== PACKET/UTIL =====================
    def _generate_packet(self, content):
        page_url = None
        try:
            page_url = self.page.url if self.page else None
        except Exception:
            page_url = None
        return {
            "action": self.ACTION_TYPE,
            "uid": Helper.generate_uid(),
            "timestamp": datetime.now().strftime("%d%m%Y %H:%M:%S"),
            "webpage": page_url,
            "data_present": bool(content and not any(k in content for k in ["skip", "error"])),
            "log_message": self.LOG_MESSAGE,
            "response": content if content else None,
        }

    def _log_save(self, msg: str):
        try:
            # Some custom loggers you use have .save(); fall back to .info()
            self.logger.save(msg)
        except Exception:
            self.logger.info(msg)

    # ===================== PUBLIC BATCH =====================
    def execute_blocks(self, block: list):
        block_data = []
        for idx, _action_ in enumerate(block):
            data = self.execute(_action_)
            if data:
                block_data.append(data)
        time_stamp = self.DATE.strftime("%Y-%m-%d %H:%M")
        return block_data, time_stamp

    def perform_action():
        pass
