import json,json5, os
# from datetime import datetime
root_dir = os.path.dirname(os.path.dirname(__file__))

path_root = os.path.join(root_dir,r"paths.json")

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
    return load_json(path_root)

def output_path():
    return get_paths()["output"]

def get_root_dir():
    return get_paths()["root"]


def load_config():
    root_dir = get_paths()["config"]
    return load_json5(root_dir)
    
def load_gen_config():
    root_dir = get_root_dir()
    config_path = os.path.join(root_dir,"configs","generic_config.json5")
    return load_json5(config_path)


def session_dir():
    out_dir = output_path()
    return create_dir(out_dir,"session")

def schedule_config():
    paths = get_paths()
    return paths["config_schedule"]



def load_mail_config():
    paths = get_paths()
    return paths.get("config_mail")


# PVT_BANK_CODES = [f"PVB_{i}" for i in range(1,23)]
# PUB_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]
FRN_BANK_CODES = [f"FRB_{i}" for i in range(1,11)]
SFB_BANK_CODES = [f"SFB_{i}" for i in range(1,12)]
ALL_BANK_CODES = [f"PSB_{i}" for i in range(1,13)]+["PVB_22"]+[f"PVB_{i}" for i in range(1,22)]


