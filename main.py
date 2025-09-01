import os, warnings,time, ssl
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow-specific-selenium
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_stdlib_context

from datetime import datetime
from app.utils import Helper
from app.program_logger import get_forever_logger
from app.BankScraper import BankScraper

config = Helper.load_json(r"configs\param_table.json5", typ="json5")
paths = Helper.load_json(r"paths.json")

# Constants
LOG_DIR, CACHE_DIR, PROCESS_DIR = Helper.create_dirs(paths["output"], ["logs", "cache", "processed"])
TODAY = datetime.now().strftime("%d%m%Y")
HRM = Helper.get_timestamp(sep="")
FILE_NAME = f"cache_{TODAY}_{HRM}.json5"

# Logger
logger = get_forever_logger(name="scraper", log_dir=LOG_DIR)

BANK_CODES = [f"PSB_{i}" for i in range(1,13)]+["PVB_1","PVB_2","PVB_3","PVB_4","PVB_5","PVB_6","PVB_8","PVB_10","PVB_11","PVB_13","PVB_14","PVB_15","PVB_16","PVB_18","PVB_20"]
BANK_CODES = ["PVB_12"]
final_dict = BankScraper.get_final_struct(TODAY,FILE_NAME)

try:
    for code in BANK_CODES:
        bank_params = config[code]
        scraper = BankScraper(bank_params, logger,paths)
        result = scraper.run()
        final_dict["records"].append(result)
        final_dict["registry"].update({code:bank_params["bank_name"]})
        time.sleep(5)
except KeyboardInterrupt:
    logger.warning("Process Interrupted by User!")
except Exception as e:
    pass
Helper.save_json(final_dict, os.path.join(CACHE_DIR, FILE_NAME))