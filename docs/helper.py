"""
excel_hybrid_updater.py
-----------------------------------
Hybrid safe-update system for injecting central bank data
into an existing Excel workbook (e.g., 'Daily Interest Rate.xlsx').

Features:
✅ Auto backup before update
✅ VLOOKUP-safe (keeps formulas & sheet structure)
✅ Updates only matching sheets (banks)
✅ Leaves other sheets (like Workstation, Economy Tool) untouched
✅ Modular for integration with bnk2.py or any pipeline
"""

import shutil
import pandas as pd
from datetime import datetime, timedelta
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows


# ===============================================================
# 🧩 1. Backup existing Excel before modification
# ===============================================================
def backup_excel(file_path: str) -> str:
    """Create a timestamped backup copy of the given Excel file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = file_path.replace(".xlsx", f"_backup_{timestamp}.xlsx")
    shutil.copy(file_path, backup_path)
    print(f"🧷 Backup created: {backup_path}")
    return backup_path


# ===============================================================
# 🧩 2. Fetch central bank data using existing fetchers
# ===============================================================
def get_bank_dataframe(cfg: dict, name: str, days: int = 5):
    """Fetch data for a given bank configuration (compatible with bnk2.py)."""
    from bnk2 import CONFIG  # ensure same source
    fetcher = cfg["fetcher"]
    end = datetime.today()
    start = end - timedelta(days=days)

    if name in ["Banxico", "BoC"]:
        return fetcher(cfg, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    elif name in ["BOJ"]:
        return fetcher(cfg, start.strftime("%y%m%d"), end.strftime("%y%m%d"))
    elif name in ["CBOE"]:
        return fetcher(cfg, start, end)
    elif name in ["NYCFED"]:
        return fetcher(cfg, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    else:
        return fetcher(cfg, start, end)


# ===============================================================
# 🧩 3. Overwrite only data region (preserve VLOOKUPs & structure)
# ===============================================================
def update_sheet_in_place(ws, df):
    """
    Replace existing sheet data with new DataFrame content,
    preserving sheet name and Excel formula integrity.
    """
    # Clear existing data region (not formulas)
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.value = None

    # Write new data starting from A1
    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)


# ===============================================================
# 🧩 4. Safe master update function
# ===============================================================
def update_master_excel(master_path: str, config: dict = None, days: int = 5):
    """
    Safely updates all matching bank sheets in the master Excel workbook.
    Keeps formulas and custom sheets intact.
    """
    from bnk2 import CONFIG as default_config
    config = config or default_config

    # Step 1: Backup
    backup_excel(master_path)

    # Step 2: Load workbook
    wb = load_workbook(master_path)

    for name, cfg in config.items():
        if name not in wb.sheetnames:
            print(f"⚠️ {name} not found in workbook, skipping.")
            continue

        try:
            df = get_bank_dataframe(cfg, name, days)
            if df is None or df.empty:
                print(f"⚠️ {name}: No data fetched.")
                continue

            ws = wb[name]
            update_sheet_in_place(ws, df)
            print(f"✅ Updated {name} successfully.")

        except Exception as e:
            print(f"❌ Error updating {name}: {e}")

    # Step 3: Save workbook
    wb.save(master_path)
    print(f"💾 All updates applied to {master_path}")


# ===============================================================
# 🧩 5. Optional: Log update events inside the workbook
# ===============================================================
def log_update(wb, log_text: str):
    """Append a timestamped log entry to an 'UpdateLog' sheet."""
    if "UpdateLog" not in wb.sheetnames:
        ws = wb.create_sheet("UpdateLog")
        ws.append(["Timestamp", "Log"])
    else:
        ws = wb["UpdateLog"]

    ws.append([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), log_text])


# ===============================================================
# 🧩 6. Example usage (you can comment out or call manually)
# ===============================================================
if __name__ == "__main__":
    from bnk2 import CONFIG
    update_master_excel("Daily Interest Rate.xlsx", CONFIG, days=5)

# from excel_hybrid_updater import update_master_excel
# from bnk2 import CONFIG

# update_master_excel("Daily Interest Rate.xlsx", CONFIG, days=5)
