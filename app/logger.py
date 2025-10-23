import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

# --- Optional ColorLog Support ---
try:
    import colorlog
    COLORLOG_AVAILABLE = True
except ImportError:
    COLORLOG_AVAILABLE = False

# --- Custom Log Levels ---
TRACE_LEVEL_NUM = 15
SAVE_LEVEL_NUM = 22
NOTICE_LEVEL_NUM = 25

logging.addLevelName(TRACE_LEVEL_NUM, "TRAC")
logging.addLevelName(SAVE_LEVEL_NUM, "SAVE")
logging.addLevelName(NOTICE_LEVEL_NUM, "NOTI")

def trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE_LEVEL_NUM):
        self._log(TRACE_LEVEL_NUM, message, args, **kwargs)

def save(self, message, *args, **kwargs):
    if self.isEnabledFor(SAVE_LEVEL_NUM):
        self._log(SAVE_LEVEL_NUM, message, args, **kwargs)

def notice(self, message, *args, **kwargs):
    if self.isEnabledFor(NOTICE_LEVEL_NUM):
        self._log(NOTICE_LEVEL_NUM, message, args, **kwargs)

logging.Logger.trace = trace
logging.Logger.save = save
logging.Logger.notice = notice

# --- Shared Formatters and Colors ---
DEFAULT_FORMAT = "%(asctime)s [%(levelname)s]: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_COLORS = {
    'TRAC': 'white',
    'SAVE': 'blue',
    'NOTI': 'bold_cyan',
    'DEBUG': 'cyan',
    'INFO': 'green',
    'WARN': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'bold_red',
}

def _get_formatter(use_color=False):
    if use_color and COLORLOG_AVAILABLE:
        return colorlog.ColoredFormatter(
            "%(log_color)s" + DEFAULT_FORMAT,
            datefmt=DATE_FORMAT,
            log_colors=LOG_COLORS
        )
    return logging.Formatter(DEFAULT_FORMAT, datefmt=DATE_FORMAT)

def _add_console_handler(logger, level, use_color=True):
    handler = colorlog.StreamHandler(sys.stdout) if use_color and COLORLOG_AVAILABLE else logging.StreamHandler(sys.stdout)
    handler.setFormatter(_get_formatter(use_color))
    handler.setLevel(level)
    logger.addHandler(handler)

# # --- Generic Logger Setup ---
# def setup_logger(
#     name="app_logger",
#     log_dir="logs",
#     log_level=logging.DEBUG,
#     to_console=True,
#     to_file=True,
#     use_color=True
# ):
#     os.makedirs(log_dir, exist_ok=True)
#     logger = logging.getLogger(name)
#     if logger.hasHandlers():
#         return logger
#     logger.setLevel(log_level)
#     logger.propagate = False

#     if to_file:
#         file_path = os.path.join(log_dir, f"{name}.log")
#         file_handler = logging.FileHandler(file_path, encoding='utf-8')
#         file_handler.setFormatter(_get_formatter(use_color=False))
#         file_handler.setLevel(TRACE_LEVEL_NUM)
#         logger.addHandler(file_handler)

#     if to_console:
#         _add_console_handler(logger, log_level, use_color)

#     return logger

from logging.handlers import TimedRotatingFileHandler

def setup_logger(
    name="app_logger",
    log_dir="logs",
    log_level=logging.DEBUG,
    to_console=True,
    to_file=True,
    use_color=True
):
    """
    Sets up a logger that:
    - Writes to a daily folder (created using log_dir/<YYYY-MM-DD>/)
    - Rotates automatically every midnight within that folder
    - Keeps the last 7 rotated files
    """

    # --- Ensure log folder exists ---
    today_folder = datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join(log_dir, today_folder)
    os.makedirs(log_dir, exist_ok=True)

    # --- Initialize logger ---
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    logger.setLevel(log_level)
    logger.propagate = False

    # --- File handler (rotates daily at midnight) ---
    if to_file:
        file_path = os.path.join(log_dir, f"{name}.log")

        file_handler = TimedRotatingFileHandler(
            file_path,
            when="midnight",   # rotate every midnight
            interval=1,        # 1 day
            backupCount=7,     # keep last 7 logs
            encoding='utf-8'
        )
        # Each rotated log will look like: scraper.log.2025-10-23
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setFormatter(_get_formatter(use_color=False))
        file_handler.setLevel(TRACE_LEVEL_NUM)
        logger.addHandler(file_handler)

    # --- Console handler ---
    if to_console:
        _add_console_handler(logger, log_level, use_color)

    return logger




# --- Global Logger Registry ---
_active_logger = None

def set_global_logger(logger):
    global _active_logger
    _active_logger = logger

def get_global_logger():
    return _active_logger or logging.getLogger("default_logger")
