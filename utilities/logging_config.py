import logging
import os
from utilities.customized_logger import DailyRotatingFileHandler
from prefect.logging import get_run_logger

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[94m',  # Blue
        'INFO': '\033[92m',   # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',   # Red
        'CRITICAL': '\033[95m',  # Purple
        'ENDC': '\033[0m'     # Reset color
    }

    def format(self, record):
        log_message = super().format(record)
        return f"{self.COLORS.get(record.levelname, self.COLORS['ENDC'])}{log_message}{self.COLORS['ENDC']}"

class ConsoleFilter(logging.Filter):
    def filter(self, record):
        return not hasattr(record, 'no_console')

def setup_custom_logger(name, log_level=logging.INFO, log_dir='logs'):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.propagate = False  # Prevent propagation to avoid double logging

    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # File handler with DailyRotatingFileHandler
    file_handler = DailyRotatingFileHandler(
        base_filename=os.path.join(log_dir, name),
        when="midnight",
        interval=1,
        backupCount=7
    )
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler with colored output
    console_handler = logging.StreamHandler()
    console_formatter = ColoredFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger

# def get_logger(debug_mode=False):
#     """
#     Returns the appropriate logger based on the context.
#     If called within a Prefect task or flow, returns the Prefect run logger.
#     Otherwise, returns the custom logger.
#     """
#     try:
#         return get_run_logger()
#     except RuntimeError:
#         return setup_custom_logger("Prefect_Flow", logging.DEBUG if debug_mode else logging.INFO)

def get_logger(debug_mode=False):
    log_level = logging.DEBUG if debug_mode else logging.INFO
    return setup_custom_logger("Prefect_Flow", log_level)

def log_to_file_only(logger, level, message):
    logger.log(level, message, extra={'no_console': True})

def log_summary(logger, title, data):
    summary = f"\n{'#' * 10} {title.upper()} {'#' * 10}\n"
    for key, value in data.items():
        summary += f"{key}: {value}\n"
    summary += f"{'#' * (20 + len(title))}\n"
    logger.info(summary)

def log_duplicate_analysis(logger, df_name, df):
    total_rows = len(df)
    duplicate_rows = df.duplicated().sum()
    duplicate_percentage = (duplicate_rows / total_rows) * 100 if total_rows > 0 else 0

    log_summary(logger, f"DUPLICATE ANALYSIS: {df_name}", {
        "Total rows": total_rows,
        "Duplicate rows": duplicate_rows,
        "Percentage of duplicates": f"{duplicate_percentage:.2f}%"
    })

def log_greeks_update_summary(logger, data):
    log_summary(logger, "GREEKS UPDATES SUMMARY", data)