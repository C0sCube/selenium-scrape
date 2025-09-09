from app.utils import Helper
from datetime import datetime

#configs
CONFIG = Helper.load_json(r"configs\param_table.json5", typ="json5")
PATHS = Helper.load_json(r"paths.json")


TODAY = datetime.now().strftime("%Y-%m-%d")

#directories
LOG_DIR = Helper.create_dir(PATHS["output"],"logs")
CCH_DIR = Helper.create_dir(PATHS["output"],"cache",TODAY) 
PRS_DIR = Helper.create_dir(PATHS["output"],"process",TODAY) 

POST_SCRAPE_OPS = CONFIG["POST_SCRAPE_OPS"]

# BANK_CODES = [f"PSB_{i}" for i in range(1,13)]+["PVB_{i}" for i in range(1,23)]
BANK_CODES = ["PVB_11"]