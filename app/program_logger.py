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

# --- Generic Logger Setup ---
def setup_logger(
    name="app_logger",
    log_dir="logs",
    log_level=logging.DEBUG,
    to_console=True,
    to_file=True,
    use_color=True
):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    logger.setLevel(log_level)
    logger.propagate = False

    if to_file:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        file_path = os.path.join(log_dir, f"{name}_{timestamp}.log")
        file_handler = logging.FileHandler(file_path, encoding='utf-8')
        file_handler.setFormatter(_get_formatter(use_color=False))
        file_handler.setLevel(TRACE_LEVEL_NUM)
        logger.addHandler(file_handler)

    if to_console:
        _add_console_handler(logger, log_level, use_color)

    return logger

# --- Forever Logger ---
def get_forever_logger(
    name="watcher",
    log_dir="logs/daily",
    max_bytes=2 * 1024 * 1024,
    backup_count=5,
    log_level=logging.INFO,
    to_console=True,
    use_color=True
):
    today = datetime.now().strftime('%Y-%m-%d')
    folder_path = os.path.join(log_dir, today)
    os.makedirs(folder_path, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    logger.setLevel(log_level)
    logger.propagate = False

    log_file = os.path.join(folder_path, f"{name}.log")
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8'
    )
    file_handler.setFormatter(_get_formatter(use_color=False))
    logger.addHandler(file_handler)

    if to_console:
        _add_console_handler(logger, log_level, use_color)

    return logger

# --- Session Logger ---
def setup_session_logger(
    folder_name: str,
    base_log_dir: str = "logs/sessions",
    log_level: int = logging.DEBUG,
    to_console: bool = True,
    use_color: bool = True
):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs(base_log_dir, exist_ok=True)

    log_filename = f"fs_{folder_name}_{timestamp}.log"
    log_path = os.path.join(base_log_dir, log_filename)
    logger = logging.getLogger(f"session_{folder_name}_{timestamp}")

    if logger.hasHandlers():
        return logger

    logger.setLevel(log_level)
    formatter = logging.Formatter(DEFAULT_FORMAT, datefmt=DATE_FORMAT)

    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if to_console:
        if use_color and COLORLOG_AVAILABLE:
            color_formatter = colorlog.ColoredFormatter(
                "%(log_color)s" + DEFAULT_FORMAT,
                datefmt=DATE_FORMAT,
                log_colors= LOG_COLORS
            )
            console_handler = colorlog.StreamHandler(stream=sys.stdout)
            console_handler.setFormatter(color_formatter)
        else:
            console_handler = logging.StreamHandler(stream=sys.stdout)
            console_handler.setFormatter(formatter)

        console_handler.setLevel(log_level)
        logger.addHandler(console_handler)

    return logger


# --- Global Logger Registry ---
_active_logger = None

def set_active_logger(logger):
    global _active_logger
    _active_logger = logger

def get_active_logger():
    return _active_logger or logging.getLogger("default_logger")
