import os
import datetime
from datetime import datetime, timedelta, timezone
import requests, certifi
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import urllib3
import pandas as pd
import ftfy
import unidecode
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

governance_keywords = [
    "Fines", "Fraud", "Scandals", "Lawsuit", "Legal Proceedings", "Show Cause Notices",
    "Anti-Competitive Pricing", "Cartel Formation", "FDA Investigation",
    "Board Of Directors Resignation", "Bribery & Fraud", "Unfair Trade Practices",
    "Insider Trading", "Corporate Espionage", "Shareholders Disputes",
    "Lobbying", "whistleblower", "FIR", "Violation", "Penalty", "Settlement", "Cyber attack", "Misleading", "Arbitration"
]

social_keywords = [
    "Child Labour", "Human Rights Violation", "Fire", "Accident", "Fatalities",
    "Data Leakages", "Community Rehabilitation", "Labour Rights", "Strikes",
    "Misleading Advertisements", "Consumer Protection", "Consumer Awareness",
    "consumer boycotts", "Child Labor", "Workplace Harassment", "Discrimination"
]

environmental_keywords = [
    "Oil Spills", "Toxic Emissions", "Hazardous Waste Disposal",
    "Rehabilitation And Resettlement", "Water Stress", "Water Contamination",
    "Deforestation", "Resource Depletion", "Environmental Fines"
]

all_keywords = governance_keywords + social_keywords + environmental_keywords

BASE_URL = "https://news.google.com"
CUTOFF_DATE = datetime.now(timezone.utc) - timedelta(days=1)

def fetch_news(company, keyword):
    try:
        query = f"{company} {keyword} when:1d".replace(" ", "%20")
        url = f"{BASE_URL}/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
        resp = requests.get(url, timeout=40, verify=False)
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, "html.parser")

        results = []
        for article in soup.select("article"):
            data = article.find_all('a')
            for x_data in data:
                href_link = x_data.get('href')
                full_url = urljoin(BASE_URL, href_link) if href_link else None
                raw_headline = x_data.text.strip()
                if not raw_headline:
                    continue
                headline = clean_headline(raw_headline)
                source_tag = article.select_one("div.vr1PYe")
                source = source_tag.get_text(strip=True) if source_tag else None

                time_tag = article.find("time")
                published = time_tag["datetime"] if time_tag and time_tag.has_attr("datetime") else None
                if published:
                    pub_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
                    if pub_date < CUTOFF_DATE:
                        continue

                results.append({"Company": company, "Keyword": keyword,"Headline": headline,"Source": source,"Link": full_url,"Published": published})
        return results
    except Exception as e:
        print(f"Error fetching news for {company} + {keyword}: {e}")
        return []

def clean_headline(text: str) -> str:
    try:
        cleaned_str = re.sub(r"[\r\n\t]|&nbsp;?", "", text, flags=re.IGNORECASE).strip()
        cleaned_str = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', cleaned_str)
        cleaned_str = unidecode.unidecode(cleaned_str)
        try:
            fixed = cleaned_str.encode('latin1').decode('utf-8')
        except Exception:
            fixed = cleaned_str
        return ftfy.fix_text(fixed)

    except Exception:
        return text.strip()

def process_company(company):
    company_news = []
    for kw in all_keywords:
        print(f"   → {company}: Searching keyword: {kw}")
        news_items = fetch_news(company, kw)
        company_news.extend(news_items)
    print(f"   ✔ Finished {company}, collected {len(company_news)} items.")
    return company_news


if __name__ == "__main__":
    df = pd.read_excel(r"C:\Users\kaustubh.keny\Projects\OUTPUTS\nse_data\List of Companies.xlsx")
    company_names = df['Company name'].dropna().tolist()

    output_dir = r"C:\Users\kaustubh.keny\Projects\OUTPUTS\nse_data"
    os.makedirs(output_dir, exist_ok=True)

    all_news = []

    print(f"\n Starting parallel processing for {len(company_names)} companies...\n")
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_company, company): company for company in company_names}
        for idx, future in enumerate(as_completed(futures), start=1):
            company = futures[future]
            try:
                result = future.result()
                if result:
  
                    for item in result:
                        if "link" in item and "title" in item:
                            item["hyperlink"] = f'=HYPERLINK("{item["link"]}", "{item["title"]}")'
                    
                    # Write immediately to disk
                    clean_name = company.replace(" ", "_").replace("/", "_")
                    excel_path = os.path.join(output_dir, f"{clean_name}.xlsx")
                    pd.DataFrame(result).to_excel(excel_path, index=False, engine="openpyxl")
                
                    print(f"[{idx}/{len(company_names)}] ✅ {company} done → {excel_path} ({len(result)} records)")
                else:
                    print(f"[{idx}/{len(company_names)}] ⚠️ No news found for {company}")
            except Exception as e:
                print(f"[{idx}/{len(company_names)}] ❌ Error processing {company}: {e}")




 