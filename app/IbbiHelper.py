import json
import re
import pandas as pd
from bs4 import BeautifulSoup


class IbbiHelper:
    
    def __init__(self):
        pass

    @staticmethod
    def parse_html_table(html):
        """Parse one HTML table into DataFrame with Date, Subject, Remarks, Link."""
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:return None

        rows = []
        for tr in table.select("tbody tr"):
            cols = tr.find_all("td")
            if len(cols) < 3:
                print("There are less than three cols, skipping")
                continue

            date = cols[0].get_text(strip=True)
            remarks = cols[2].get_text(strip=True)

            a = cols[1].find("a")
            subject = a.get_text(" ", strip=True) if a else cols[1].get_text(strip=True)
            link = ""
            if a and a.get("onclick"):
                onclick = a["onclick"]
                if "newwindow1" in onclick:
                    start = onclick.find("'") + 1
                    end = onclick.rfind("'")
                    link = onclick[start:end]

            rows.append([date, subject, remarks, link])

        return pd.DataFrame(rows, columns=["Date", "Subject", "Remarks", "Link"])


    @staticmethod
    def cache_to_excel_report(cache_file,format_="path", excel_out="Orders_All.xlsx"):
        if format_ == "path":
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        if format_=="data":
            data = cache_file
        used_names = {}  # track counts per sheet name
        def clean_sheet_name(name, fallback="Sheet"):
            safe = re.sub(r'[\[\]\:\*\?\/\\]', "_", str(name))
            return safe[:31] if safe else fallback

        with pd.ExcelWriter(excel_out, engine="xlsxwriter") as writer:
            for scraped in data["records"][0]["scraped_data"]:
                if not scraped.get("data_present"):
                    continue

                action = scraped.get("action", "unknown")
                webpage = scraped.get("webpage", "")
                for resp in scraped.get("response", []):
                    if resp.get("type") != "table_html":
                        continue

                    df = IbbiHelper.parse_html_table(resp["value"])
                    if df is None or df.empty:
                        continue

                    titles = resp.get("title", [])
                    base_name = clean_sheet_name(titles[-1] if titles else action)

                    count = used_names.get(base_name, 0) + 1
                    used_names[base_name] = count
                    sheet_name = base_name if count == 1 else clean_sheet_name(f"{base_name}_{count}")


                    meta_rows = [["Action", action], ["Webpage", webpage]]
                    for i, t in enumerate(titles, start=1):
                        meta_rows.append([f"Header{i}", t])
                    meta = pd.DataFrame(meta_rows, columns=["Meta", "Value"])
                    meta.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0)

                    df_visible = df[["Date", "Subject", "Remarks","Link"]]
                    startrow = len(meta) + 2
                    df_visible.to_excel(writer, sheet_name=sheet_name, index=False, startrow=startrow)

                    # ws = writer.sheets[sheet_name]
                    # for i, (txt, url) in enumerate(zip(df["Subject"], df["Link"]), start=startrow+1):
                    #     if url:
                    #         ws.write_url(i, 1, url, string=txt)

        # print(f"Excel saved: {excel_out}")
        return excel_out


# Example usage
# path = r"C:\Users\kaustubh.keny\Projects\JSON25\scrape_output\cache\2025-09-23\25-09-23T10-08-01_cache.json"
# cache_to_excel(path, "Orders_All.xlsx")
