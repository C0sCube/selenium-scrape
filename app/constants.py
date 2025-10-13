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



root_dir = os.path.dirname(os.path.dirname(__file__))
PATHS = load_json5(r"paths.json5")
conf_path = os.path.join(root_dir,PATHS["configs"])
gen_conf_path =os.path.join(root_dir,PATHS["generic_config"])

CONFIG = load_json5(conf_path)
GENERIC_ACTION_CONFIG = load_json5(gen_conf_path)
POST_SCRAPE_OPS = CONFIG.get("POST_SCRAPE_OPS")
SCRIPTS = GENERIC_ACTION_CONFIG["scripts"]

TODAY = datetime.now().strftime("%Y-%m-%d")

#directories
OUTPUT_PATH = PATHS["output"]
LOG_DIR = create_dir(OUTPUT_PATH,"logs",TODAY)
CCH_DIR = create_dir(OUTPUT_PATH,"cache",TODAY) 
PRS_DIR = create_dir(OUTPUT_PATH,"process",TODAY)
DATA_DIR = create_dir(OUTPUT_PATH,"data",TODAY)
CACHE_REP_DIR = create_dir(OUTPUT_PATH,"report",TODAY)

#file size constants
MAX_REQUEST_BYTE_SIZE = 2_000_000 #2mb file

#Manual
MAX_DOWNLOAD_TIMEOUT = 45
MAX_DOWNLOAD_WAIT = 5


DRIVER_LOAD_RETRIES = 5
LOAD_IN_BETWEEN_DELAY = 10 #seconds

PVT_BANK_CODES = [f"PVB_{i}" for i in range(1,23)]
PUB_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]
ALL_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]+["PVB_{i}" for i in range(1,23)]


