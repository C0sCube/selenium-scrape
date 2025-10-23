import requests, re
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})


# ---- Shared Pandas Helpers ----
def normalize_columns_to_dates(df):
    """Convert datetime-like columns to mm-dd-YYYY strings."""
    df.columns = [
        c.strftime("%m-%d-%Y") if isinstance(c, pd.Timestamp) else c
        for c in df.columns
    ]
    return df


def ensure_full_date_range(df, start, end):
    """Ensure dataframe has full date coverage, fill missing dates with NaN."""
    full_range = pd.date_range(start=start, end=end, freq="D")
    df = df.reindex(columns=full_range)
    return normalize_columns_to_dates(df)

# ---- Fetchers ----
def fetch_rba(cfg, start=None, end=None):
    r = session.get(cfg["url"], timeout=15)
    r.raise_for_status()
    df = pd.read_excel(BytesIO(r.content))

    df.columns = df.iloc[1, :]
    df = df.iloc[-5:, :].T.reset_index()
    df.columns = df.iloc[0, :]
    df = df.drop(df.index[0])

    return normalize_columns_to_dates(df)

def fetch_bcra(cfg, start=None, end=None):
    r = session.get(cfg["url"], timeout=15, verify=False)
    r.raise_for_status()
    df = pd.read_html(r.text)[0]

    today = datetime.today()
    date_range = pd.date_range(end=today, periods=5)   # last 5 days

    df.columns = ["Header", "Date", "Value"]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)

    reshaped = df.pivot_table(index="Header", columns="Date", values="Value", aggfunc="first")
    reshaped = reshaped.reindex(columns=date_range)
    reshaped = normalize_columns_to_dates(reshaped)
    return reshaped.reset_index()

def fetch_banxico(cfg, start, end):
    url = f"{cfg['url']}/{','.join(cfg['series_ids'])}/datos/{start}/{end}"
    r = session.get(url, headers=cfg["headers"], timeout=15)
    r.raise_for_status()
    data = r.json()

    records = {}
    for s in data['bmx']['series']:
        sid = s['idSerie']
        rec = {d['fecha']: d['dato'] for d in s['datos']}
        records[sid] = rec

    df = pd.DataFrame.from_dict(records, orient="index")
    df.columns = pd.to_datetime(df.columns, dayfirst=True)
    df.index = df.index.map(lambda x: cfg["series_map"].get(x, x))

    return normalize_columns_to_dates(df.sort_index(axis=1))

def fetch_boc(cfg, start, end):
    url = f"{cfg['url']}/{','.join(cfg['series_ids'])}/json?start_date={start}&end_date={end}"
    r = session.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()

    records = {}
    for row in data["observations"]:
        date = row["d"]
        for sid in cfg["series_ids"]:
            records.setdefault(sid, {})[date] = row.get(sid, {}).get("v")

    df = pd.DataFrame.from_dict(records, orient="index")
    df.columns = pd.to_datetime(df.columns)
    df = ensure_full_date_range(df, start, end)
    df.index = df.index.map(lambda x: cfg["series_map"].get(x, x))

    return df.sort_index(axis=1)

def fetch_boj(cfg, start, end):
    dates = pd.date_range(start=datetime.strptime(start, "%y%m%d"), end=datetime.strptime(end, "%y%m%d"))
    records = {}

    for date in dates:
        url = f"{cfg['url']}/md{date.strftime('%y%m%d')}.htm"
        try:
            r = session.get(url, timeout=15)
            if r.status_code != 200:continue
        except Exception:continue

        soup = BeautifulSoup(r.text, "html.parser")
        for li in soup.find_all("li"):
            text = li.get_text(strip=True)
            text = re.sub(r"[\[\%]+","",text,re.IGNORECASE)
            parts = text.split("]")
            if len(parts) < 2:continue
            key,val = parts
            # print(f"{key}:{val}")
            series_name = f"Call Rate {key}"
            records.setdefault(series_name, {})[date] = val

    out = pd.DataFrame.from_dict(records, orient="index")
    out = out.reindex(columns=dates)
    return normalize_columns_to_dates(out)

def fetch_nyfed(cfg, start, end):
    url = (
        f"{cfg['url']}?startDt={start}&endDt={end}"
        f"&sort={cfg['headers']['sort']}"
        f"&productCode={cfg['headers']['productCode']}"
        f"&eventCodes={cfg['headers']['eventCodes']}"
        f"&format={cfg['headers']['format']}"
    )
    r = session.get(url, timeout=15)
    r.raise_for_status()

    if cfg["headers"]["format"] == "csv":
        df = pd.read_csv(BytesIO(r.content))
    else:
        df = pd.read_excel(BytesIO(r.content))

    df.columns = [str(c).strip() for c in df.columns]
    return df

def fetch_cboe(cfg, start=None, end=None, days=5):
    r = session.get(cfg["url"], timeout=15, verify=False)
    r.raise_for_status()

    df = pd.read_csv(BytesIO(r.content))
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df = df.iloc[-days:, :]
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    full_range = pd.date_range(start=start_dt, end=end_dt, freq="D")
    df = df.set_index("DATE").reindex(full_range)
    wide = pd.DataFrame([df["OVX"].tolist()], index=["OVX Index"], columns=full_range)
    return normalize_columns_to_dates(wide)

CONFIG = {
    "Banxico": {
        "fetcher": fetch_banxico,
        "url": "https://www.banxico.org.mx/SieAPIRest/service/v1/series",
        "headers": {
            "Bmx-Token": "1846d489e18513d4306b70e257a21edb56e82325de0139af91dd2176975029bf"
        },
        "series_ids": ["SF61745","SF331451","SF43783","SF43878","SF111916"],
        "series_map": {
            "SF61745": "Target rate 1/",
            "SF331451": "Overnight TIIE funding rate 2/",
            "SF43783": "28 day TIIE 3/",
            "SF43878": "91 day TIIE 3/",
            "SF111916": "182 day TIIE 3/"
        }
    },
    "BoC": {
        "fetcher": fetch_boc,
        "url": "https://www.bankofcanada.ca/valet/observations",
        "series_ids": ["V39079","CL.CDN.MOST.1DL","V39078","V80691310","V122530"],
        "series_map": {
            "V39079": "Target for the overnight rate",
            "CL.CDN.MOST.1DL": "Overnight money market financing rate1",
            "V39078": "Bank rate - Daily",
            "V80691310": "Bank rate - Weekly",
            "V122530": "Bank rate - Monthly"
        }
    },
    "RBA": {
        "fetcher": fetch_rba,
        "url": "https://www.rba.gov.au/statistics/tables/xls/f01d.xlsx"
    },
    "BCRA": {
        "fetcher": fetch_bcra,
        "url": "https://www.bcra.gob.ar/PublicacionesEstadisticas/Principales_variables_i.asp"
    },
    "BOJ": {
        "fetcher": fetch_boj,
        "url": "https://www3.boj.or.jp/market/en/stat"
    },
    "NYCFED": {
        "fetcher": fetch_nyfed,
        "url": "https://markets.newyorkfed.org/read",
        "headers": {
            "sort": "postDt:-1,'data.closeTm':-1",
            "productCode": 70,     # repo operations
            "eventCodes": 730,     # event code for operations
            "format": "csv"        # NY Fed supports csv or xls
        }
    },
    "CBOE": {
        "fetcher": fetch_cboe,
        "url": "https://cdn.cboe.com/api/global/us_indices/daily_prices/OVX_History.csv"
    }
}


def run_pipeline(days=5, outfile="central_banks_config_2.xlsx"):
    end = datetime.today()
    start = end - timedelta(days=days)

    with pd.ExcelWriter(outfile) as writer:
        for name, cfg in CONFIG.items():
            fetcher = cfg["fetcher"]

            try:
                if name in ["Banxico", "BoC"]:
                    df = fetcher(cfg, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
                if name in ["BOJ"]:
                    df = fetcher(cfg, start.strftime("%y%m%d"), end.strftime("%y%m%d"))
                elif name in ["CBOE"]:
                    df = fetcher(cfg, start, end)
                elif name in ["NYCFED"]:
                    df = fetcher(cfg, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
                else:
                    df = fetcher(cfg, start, end)

                if df is not None and not df.empty:
                    df.to_excel(writer, sheet_name=name, index=not name in ["RBA", "BCRA"])
                else:
                    print(f"{name}: No data returned")

            except Exception as e:
                print(f"Error fetching {name}: {e}")

    print(f"Data saved to {outfile}")


if __name__ == "__main__":
    run_pipeline(days=5)

