import json,json5, os
from datetime import datetime


root_dir = os.path.dirname(os.path.dirname(__file__))


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
    
def load_json5(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json5.load(f)
    
def create_dir(root_path: str, *args) -> str:
    full_path = os.path.join(root_path, *args)
    os.makedirs(full_path, exist_ok=True)
    return full_path

PATHS = load_json5(r"paths.json5")
conf_path = os.path.join(root_dir,PATHS["configs"])
gen_conf_path =os.path.join(root_dir,PATHS["generic_config"])

CONFIG = load_json5(conf_path)
GENERIC_ACTION_CONFIG = load_json5(gen_conf_path)
SCRIPTS = GENERIC_ACTION_CONFIG["scripts"]

TODAY = datetime.now().strftime("%Y-%m-%d")
PROGRAM_NAME = "DepositRate Scrape"

#directories
LOG_DIR = create_dir(PATHS["output"],"logs")
CCH_DIR = create_dir(PATHS["output"],"cache",TODAY) 
PRS_DIR = create_dir(PATHS["output"],"process",TODAY)
CACHE_REP_DIR = create_dir(PATHS["output"],"report")

# POST_SCRAPE_OPS = CONFIG["POST_SCRAPE_OPS"]


#file size constants
MAX_REQUEST_BYTE_SIZE = 2_000_000 #2mb file

#Manual
MAX_DOWNLOAD_TIMEOUT = 45
MAX_DOWNLOAD_WAIT = 5


PVT_BANK_CODES = [f"PVB_{i}" for i in range(1,23)]
PUB_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]
ALL_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]+["PVB_{i}" for i in range(1,23)]


