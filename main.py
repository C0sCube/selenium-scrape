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



BANK_CODES = [f"PSB_{i}" for i in range(1,13)]
BANK_CODES = ["PVB_3"]
final_dict = BankScraper.get_final_struct(TODAY,FILE_NAME)
for code in BANK_CODES:
    bank_params = config[code]
    scraper = BankScraper(bank_params, logger,paths)
    result = scraper.run()
    final_dict["records"].append(result)
    time.sleep(2)

Helper.save_json(final_dict, os.path.join(CACHE_DIR, FILE_NAME))