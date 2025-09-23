from app.utils import Helper
from datetime import datetime

#configs
CONFIG = Helper.load_json(r"configs\params_anand.json5", typ="json5")
GENERIC_ACTION_CONFIG = Helper.load_json(r"configs\generic_actions.json5", typ="json5")
PATHS = Helper.load_json(r"paths.json5", typ="json5")

TODAY = datetime.now().strftime("%Y-%m-%d")
PROGRAM_NAME = "DepositRate Scrape"

#directories
LOG_DIR = Helper.create_dir(PATHS["output"],"logs")
CCH_DIR = Helper.create_dir(PATHS["output"],"cache",TODAY) 
PRS_DIR = Helper.create_dir(PATHS["output"],"process",TODAY)
CACHE_REP_DIR = Helper.create_dir(PATHS["output"],"report")

# POST_SCRAPE_OPS = CONFIG["POST_SCRAPE_OPS"]


#file size constants
MAX_REQUEST_BYTE_SIZE = 2_000_000 #2mb file

#Manual
MAX_DOWNLOAD_TIMEOUT = 45
MAX_DOWNLOAD_WAIT = 5


PVT_BANK_CODES = [f"PVB_{i}" for i in range(1,23)]
PUB_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]
ALL_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]+["PVB_{i}" for i in range(1,23)]


