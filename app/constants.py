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
    if os.path.exists(full_path):
        return full_path
    os.makedirs(full_path, exist_ok=True)
    return full_path


PATHS = load_json(r"paths.json")
CONFIG = load_json5(os.path.join(root_dir,PATHS["configs"]))
GENERIC_ACTIONS = load_json5(os.path.join(root_dir,PATHS["generic_config"]))
POST_SCRAPE_OPS = CONFIG.get("POST_SCRAPE_OPS")
SCRIPTS = GENERIC_ACTIONS.get("scripts",{})


#directories
OUTPUT_PATH = PATHS["output"]
LOG_DIR = create_dir(OUTPUT_PATH,"log")
SESSION_ROOT = create_dir(OUTPUT_PATH, "session")
DATA_DIR = create_dir(OUTPUT_PATH,"data")
HTMLTOPDF_PATH = PATHS["htmltopdf_path"]

#schedule times
SCHEDULE_TIMES = PATHS.get("schedule_time",["0900","0230"])
RUN_DAYS =PATHS.get("schedule_days",["mon", "tue", "wed", "thu", "fri"]) 

#file size constants
MAX_REQUEST_BYTE_SIZE = 3_000_000 #2mb file

#Manual
MAX_DOWNLOAD_TIMEOUT = 45
MAX_DOWNLOAD_WAIT = 5


DRIVER_LOAD_RETRIES = 5
LOAD_IN_BETWEEN_DELAY = 10 #seconds

COOL_DOWN = 30

PVT_BANK_CODES = [f"PVB_{i}" for i in range(1,23)]
PUB_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]
FRN_BANK_CODES = [f"FRB_{i}" for i in range(1,37)]
ALL_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]+["PVB_22"]+[f"PVB_{i}" for i in range(1,22)]


