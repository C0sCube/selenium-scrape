import os, re, time
from datetime import datetime, timedelta
from urllib.parse import urlencode
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from app.utils import Helper
from itertools import product

class ActionHelper:
    
    def __init__(self):
        pass
    
    # @staticmethod
    # def _find_preceding_texts(table, n=2):
    #     texts = []
    #     current = table
    #     label_tags = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "strong", "a", "span","div"}
    #     MAX_TEXT_LENGTH = 350
    #     while len(texts) < n:
    #         try:
    #             parent = current.find_element(By.XPATH, "..")
    #             siblings = parent.find_elements(By.XPATH, "preceding-sibling::*")
    #             for sib in reversed(siblings):
    #                 if sib.tag_name.lower() in ["table", "br", "hr"]:
    #                     continue
                    
    #                 if sib.find_elements(By.TAG_NAME,"table"):
    #                     continue
                    
    #                 if sib.tag_name.lower() not in label_tags:
    #                     continue
                    
    #                 if sib.tag_name.lower() == "div":
    #                     if not sib.find_elements(By.XPATH, ".//h1 | .//h2 | .//h3 | .//p | .//strong | .//a | .//span"):
    #                         continue

    #                 txt = sib.get_attribute("innerText").strip()
    #                 # txt = sib.text.strip()
    #                 txt = Helper._remove_tabspace(txt)
    #                 txt = Helper._normalize_whitespace(txt)
    #                 if txt and len(txt)<MAX_TEXT_LENGTH:
    #                     texts.append(txt)
    #                     if len(texts) == n:
    #                         return list(reversed(texts))
    #             current = parent
    #         except:
    #             break
    #     return list(reversed(texts)) if texts else ["No label found"]*n

    @staticmethod
    def _clean_raw_table_html(rawr):
        rawr = Helper.apply_sub(rawr, r'<th\b', '<td', ignore_case=True)
        rawr = Helper.apply_sub(rawr, r'</th\b', '</td', ignore_case=True)
        
        #tbody
        rawr = re.sub(r"<thead\b",r"<tbody",rawr, re.IGNORECASE)
        rawr = re.sub(r"</thead\b",r"</tbody",rawr, re.IGNORECASE)
        
        #other tags
        rawr = Helper.apply_sub(rawr, r"</?(?:strong|sup|b|p|br)(?:\s+[^>]*)?>",ignore_case=True)
        rawr = Helper.apply_sub(rawr,r'[*@\n\t]+', ignore_case=True)
        rawr = Helper.apply_sub(rawr,r"<tr[^>]*>\s*(?:&nbsp;|\u00A0|\s)*</tr>", ignore_case=True)
        rawr = Helper._normalize_whitespace(rawr)
        
        soup = BeautifulSoup(rawr, "html.parser")
        ALLOWED = {"rowspan", "colspan"}
        for tag in soup.find_all(True):
            for attr in list(tag.attrs):
                if attr not in ALLOWED:
                    del tag.attrs[attr]
        final_html = str(soup)
        
        empty = re.findall(r"<table[^>]*>\s*(?:&nbsp;|\u00A0|\s)*</table>", final_html, re.IGNORECASE)
        if empty:
            return ""
        return final_html
    
    @staticmethod
    def _determine_file_type(url):
        value = None
        if ".pdf" in url: value = "pdf"
        if url.endswith(".csv"): value = "csv"
        if url.endswith(".docx"): value = "docx"
        if url.endswith(".xlsx"): value = "xlsx"
        print(f"The File Type is : {value}")
        return value
    
    @staticmethod
    def _wait_for_download(folder, initial_files, timeout=30):
        start_time = time.time()
        print(f"[INFO] Watching folder: {folder}")
        print(f"[INFO] Initial files: {initial_files}")

        while time.time() - start_time < timeout:
            current_files = set(os.listdir(folder))
            new_files = current_files - initial_files

            for fname in new_files:
                path = os.path.join(folder, fname)

                if fname.endswith(".crdownload"):
                    print(f"[DEBUG] Skipping incomplete file: {fname}")
                    continue

                ext = ActionHelper._determine_file_type(path)
                if ext:
                    print(f"[INFO] Detected new file: {fname} with type: {ext}")
                    return path, ext

            time.sleep(2)

        print("[WARNING] Timeout reached — no valid file detected.")
        return None, None
    
    
    @staticmethod
    def build_date_value(value: str):
        today = datetime.now()
        # Case 1: range with ||N (days back)
        if "||" in value and "start=" not in value:
            fmt, days_back = value.split("||")
            days_back = int(days_back)
            content =  [
                (today - timedelta(days=i)).strftime(fmt)
                for i in range(days_back)
            ]
            print(content)
            return content
        # Case 2: explicit start/end
        if "||" in value and "start=" in value:
            fmt, range_part = value.split("||")
            parts = dict(p.split("=") for p in range_part.split(","))
            start = datetime.strptime(parts["start"], "%Y-%m-%d")
            end = datetime.strptime(parts["end"], "%Y-%m-%d")
            delta = (end - start).days
            return [
                (start + timedelta(days=i)).strftime(fmt)
                for i in range(delta + 1)
            ]

        # Case 3: just a single date
        return today.strftime(value)
    
    @staticmethod
    def build_multiple_urls(base_url, params):
        urls = []
        constant_params = {}
        list_params = {}

        for key, value in params.items():
            if key == "date":
                date_val = ActionHelper.build_date_value(value)
                if isinstance(date_val, list):
                    list_params[key] = date_val
                else:
                    constant_params[key] = date_val
            elif isinstance(value, str):
                constant_params[key] = value
            elif isinstance(value, list):
                list_params[key] = value

        if not list_params:
            query_string = urlencode(constant_params)
            urls.append(f"{base_url}?{query_string}")
            return urls

        keys = list(list_params.keys())
        values = list(list_params.values())
        for combo in product(*values):
            combo_dict = dict(zip(keys, combo))
            full_params = {**constant_params, **combo_dict}
            query_string = urlencode(full_params)
            urls.append(f"{base_url}?{query_string}")
        return urls

    
    @staticmethod
    def _find_preceding_texts(table, n=2, max_depth=5)->list:
        """
        Finds up to `n` pieces of readable text above a <table>.
        It climbs the DOM tree up to `max_depth` levels to find headings like <h2> or <p>.
        """

        texts = []
        current = table
        seen = set()

        LABEL_TAGS = {"h1","h2","h3","h4","h5","h6","p","strong","b","a","span","div"}
        MAX_TEXT_LENGTH = 350

        for depth in range(max_depth):  # climb up a few times
            try:
                # Move up one level
                parent = current.find_element(By.XPATH, "..")

                # Look at everything *before* this parent
                siblings = parent.find_elements(By.XPATH, "preceding-sibling::*")

                for sib in reversed(siblings):
                    if sib in seen: 
                        continue
                    seen.add(sib)

                    tag = sib.tag_name.lower()

                    # Skip junk or nested tables
                    if tag in ["table", "br", "hr"]:
                        continue
                    if sib.find_elements(By.TAG_NAME, "table"):
                        continue

                    # Only consider label-like elements
                    if tag not in LABEL_TAGS:
                        continue

                    # Extract visible text
                    txt = sib.get_attribute("innerText") or ""
                    txt = Helper._normalize_whitespace(Helper._remove_tabspace(txt))

                    if txt and len(txt) < MAX_TEXT_LENGTH:
                        texts.append(txt)
                        if len(texts) >= n:
                            return list(reversed(texts))

                # Go up again
                current = parent
            except Exception:
                break

        # Nothing found
        return list(reversed(texts)) if texts else []

    
    
    @staticmethod
    def generate_resp_packet(
        name="", 
        header="", 
        value=None, 
        type=""
    ): return {
        "name": name, 
        "title": header, 
        "value": value, 
        "type": type, 
        "data_present": bool(value)
    }

    @staticmethod
    def classify_navigation(element):
        href = element.get_attribute("href")
        onclick = element.get_attribute("onclick")

        if href:
            href = href.strip()

            if href.startswith("http"):
                return {"type": "absolute", "value": href}

            if href.startswith("/"):
                return {"type": "relative", "value": href}

            if href.startswith("javascript:"):
                return {"type": "javascript", "value": href.replace("javascript:", "")}

            if href.startswith("#"):
                return {"type": "anchor", "value": href}

            if href.startswith("data:"):
                return {"type": "data", "value": href}

            return {"type": "unknown_href", "value": href}

        if onclick:
            return {"type": "onclick", "value": onclick}

        return {"type": "click_only", "value": None}

    @staticmethod
    def perform_navigation(driver, nav_info, element=None):
        nav_type = nav_info["type"]
        value = nav_info["value"]

        if nav_type in ["absolute", "relative"]:
            driver.get(value)

        elif nav_type == "javascript":
            driver.execute_script(value)

        elif nav_type == "onclick":
            driver.execute_script(value)

        elif nav_type == "click_only":
            if element:
                driver.execute_script("arguments[0].click();", element)

        elif nav_type == "anchor":
            if element:
                driver.execute_script("arguments[0].scrollIntoView();", element)

        elif nav_type == "data":
            # handle base64 extraction logic here later
            pass

        else:
            if element:
                driver.execute_script("arguments[0].click();", element)

    @staticmethod
    def wait_for_dom_change(driver, old_html, timeout=10):
        from selenium.webdriver.support.ui import WebDriverWait

        WebDriverWait(driver, timeout).until(
            lambda d: d.page_source != old_html
        )
