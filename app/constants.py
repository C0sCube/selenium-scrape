import json,json5, os
# from datetime import datetime
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

def get_paths():
    return load_json(r"paths.json")

def load_config():
    output = get_paths()
    return load_json5(os.path.join(root_dir,output["configs"]))

def load_gen_config():
    output = get_paths()
    return load_json5(os.path.join(root_dir,output["generic_config"]))

def output_path():
    return get_paths()["output"]

def out_nse_path():
    return get_paths()["output_nsepath"]

def load_days():
    paths = load_json(r"paths.json")
    return paths.get("schedule_days",["mon", "tue", "wed", "thu", "fri"])

def load_times():
    paths = load_json(r"paths.json")
    return paths.get("schedule_time",["0900","0230"])

def load_mail_data():
    paths = load_json(r"paths.json")
    return paths.get("mail_data",{
        "sender": "newsrssfetch.fornse@cogencis.com",
        "dev_recipients": [
            "Kaustubh.Keny@cogencis.com"
        ],
        "recipients": [
            "Kaustubh.Keny@cogencis.com"
        ],
        "cc": [],
        "bcc": [],
        "server": "172.17.0.126",
        "port": 25
    })

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
FRN_BANK_CODES = [f"FRB_{i}" for i in range(1,11)]
SFB_BANK_CODES = [f"SFB_{i}" for i in range(1,12)]
ALL_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]+["PVB_22"]+[f"PVB_{i}" for i in range(1,22)]


